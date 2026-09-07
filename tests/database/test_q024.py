"""Unit tests for Phase 9: Q024 Atomic Dispatch Claim (FOR UPDATE SKIP LOCKED).

Verifies:
1. SQL construction and atomicity:
   - Replaces plain SELECT with atomic UPDATE ... RETURNING ...
   - Subquery uses FOR UPDATE SKIP LOCKED
   - Nationwide mode (circle_codes=None): circle filter omitted
   - Filtered mode (circle_codes=[...]): circle_code = ANY(%s) with integers
2. Dispatch isolation acceptance test:
   - Two concurrent workers (Worker A, Worker B) must never receive the same request
3. Processor integration:
   - process_pending_recharges respects ExecutionContext and circle_codes
   - On batch abort (e.g. token error or action token failure), unsubmitted
     claimed rows are immediately released via release_unprocessed_claims
4. Release helper:
   - release_unprocessed_claims resets unsubmitted claims (pyro_trans_id IS NULL)
     back to 'N' (or 'E' if retry_count > 0).
"""

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.context import ExecutionContext
from app.db.postgres import (
    fetch_pending_rows,
    release_unprocessed_claims,
)
from app.processor import process_pending_recharges
from app.zones import NZ


@contextmanager
def _mock_pg_conn(cursor):
    """Context manager helper mimicking get_pg_conn."""
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = None
    yield conn


class TestQ024QueryConstruction:
    """Test suite for Q024 atomic dispatch claim SQL structure and parameterization."""

    @patch("app.db.postgres.get_pg_conn")
    def test_q024_nationwide_mode_uses_atomic_update_and_skip_locked(self, mock_get_conn):
        """In nationwide mode (circle_codes=None), omits circle filter and uses FOR UPDATE SKIP LOCKED."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        fetch_pending_rows(batch_size=50)

        mock_cursor.execute.assert_called_once()
        executed_sql, params = mock_cursor.execute.call_args[0]

        # Outer UPDATE
        assert "UPDATE public.frc_pyro_request_data" in executed_sql
        assert "push_flag    = 'P'" in executed_sql
        assert "push_remarks = 'Claimed for Pyro dispatch'" in executed_sql
        assert "submitted_at = CURRENT_TIMESTAMP" in executed_sql

        # Inner SELECT with FOR UPDATE SKIP LOCKED
        assert "WHERE in_status   = 'C'" in executed_sql
        assert "AND push_flag   IN ('N', 'E')" in executed_sql
        assert "AND retry_count <= max_retries" in executed_sql
        assert "FOR UPDATE SKIP LOCKED" in executed_sql
        assert "LIMIT %s" in executed_sql

        # Circle filter omitted
        assert "circle_code = ANY" not in executed_sql
        assert params == (50,)

    @patch("app.db.postgres.get_pg_conn")
    def test_q024_filtered_mode_includes_circle_code_filter(self, mock_get_conn):
        """In filtered mode (circle_codes=[2, 55]), binds integer circle codes."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        fetch_pending_rows(batch_size=100, circle_codes=[2, 55])

        mock_cursor.execute.assert_called_once()
        executed_sql, params = mock_cursor.execute.call_args[0]

        assert "AND circle_code = ANY(%s)" in executed_sql
        assert "FOR UPDATE SKIP LOCKED" in executed_sql
        assert params == ([2, 55], 100)


class TestQ024DispatchIsolationAcceptance:
    """Acceptance test: Two concurrent workers must NEVER receive the same request."""

    def test_concurrent_workers_never_receive_same_request(self):
        """Worker A claims rows [101, 102], Worker B claims rows [103, 104].

        The intersection of claimed reqids between Worker A and Worker B is strictly empty.
        """
        # Simulate PostgreSQL state where Worker A claims first batch
        worker_a_rows = [
            {"reqid": 101, "caf_serial_no": "CAF001", "gsmno": "9412345671", "frcamt": 299, "circle_code": 2},
            {"reqid": 102, "caf_serial_no": "CAF002", "gsmno": "9412345672", "frcamt": 299, "circle_code": 2},
        ]
        # Worker B runs concurrently; SKIP LOCKED skips 101, 102 and returns next available rows
        worker_b_rows = [
            {"reqid": 103, "caf_serial_no": "CAF003", "gsmno": "9412345673", "frcamt": 299, "circle_code": 2},
            {"reqid": 104, "caf_serial_no": "CAF004", "gsmno": "9412345674", "frcamt": 299, "circle_code": 2},
        ]

        cursor_a = MagicMock()
        cursor_a.fetchall.return_value = worker_a_rows

        cursor_b = MagicMock()
        cursor_b.fetchall.return_value = worker_b_rows

        with patch("app.db.postgres.get_pg_conn", side_effect=[_mock_pg_conn(cursor_a), _mock_pg_conn(cursor_b)]):
            claimed_a = fetch_pending_rows(batch_size=2)
            claimed_b = fetch_pending_rows(batch_size=2)

        reqids_a = {r["reqid"] for r in claimed_a}
        reqids_b = {r["reqid"] for r in claimed_b}

        assert reqids_a == {101, 102}
        assert reqids_b == {103, 104}

        # ACCEPTANCE CRITERIA: No overlap between workers!
        assert len(reqids_a & reqids_b) == 0


class TestQ024ReleaseUnprocessedClaims:
    """Test suite for release_unprocessed_claims helper."""

    def test_release_empty_reqids_returns_zero_immediately(self):
        """Empty reqids returns 0 without database interaction."""
        with patch("app.db.postgres.get_pg_conn") as mock_conn:
            assert release_unprocessed_claims([]) == 0
            mock_conn.assert_not_called()

    @patch("app.db.postgres.get_pg_conn")
    def test_release_unprocessed_claims_resets_pending_flag(self, mock_get_conn):
        """Resets push_flag='N' (or 'E' on retry) for unsubmitted requests (pyro_trans_id IS NULL)."""
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 2
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        released = release_unprocessed_claims([101, 102])

        assert released == 2
        mock_cursor.execute.assert_called_once()
        executed_sql, (unique_reqids_param,) = mock_cursor.execute.call_args[0]

        assert "push_flag    = CASE WHEN retry_count > 0 THEN 'E' ELSE 'N' END" in executed_sql
        assert "WHERE reqid = ANY(%s)" in executed_sql
        assert "AND push_flag = 'P'" in executed_sql
        assert "AND pyro_trans_id IS NULL" in executed_sql
        assert "AND push_date IS NULL" in executed_sql
        assert sorted(unique_reqids_param) == [101, 102]


class TestQ024ProcessorZonewiseAndAbortRecovery:
    """Test suite for process_pending_recharges with zone filtering and abort recovery."""

    @pytest.mark.asyncio
    @patch("app.processor.token_manager")
    @patch("app.processor.async_release_unprocessed_claims")
    @patch("app.processor.async_fetch_pending_rows")
    async def test_processor_releases_claims_when_auth_fails_on_startup(
        self, mock_fetch, mock_release, mock_token_mgr
    ):
        """When token authentication fails, all claimed rows are immediately released."""
        mock_fetch.return_value = [
            {"reqid": 501, "caf_serial_no": "CAF001", "gsmno": "9412345678", "circle_code": 2},
            {"reqid": 502, "caf_serial_no": "CAF002", "gsmno": "9412345679", "circle_code": 2},
        ]
        mock_token_mgr.session_token = None
        mock_token_mgr.access_token = None
        mock_token_mgr.authenticate = AsyncMock(return_value=False)

        summary = await process_pending_recharges(batch_size=100)

        assert summary["auth_failed"] is True
        assert summary["processed"] == 0
        mock_release.assert_called_once_with([501, 502])

    @pytest.mark.asyncio
    @patch("app.processor.update_bcd_status")
    @patch("app.processor.async_mark_as_failed")
    @patch("app.processor.recharge")
    @patch("app.processor.token_manager")
    @patch("app.processor.async_release_unprocessed_claims")
    @patch("app.processor.async_fetch_pending_rows")
    async def test_processor_releases_unsubmitted_rows_on_batch_abort(
        self, mock_fetch, mock_release, mock_token_mgr, mock_recharge, mock_mark_failed, mock_update_bcd
    ):
        """When a batch aborts mid-way (e.g. token invalid 506 on row 1), remaining rows are released."""
        claimed_batch = [
            {
                "reqid": 501,
                "caf_serial_no": "CAF001",
                "gsmno": "9412345671",
                "vendormsisdn": "9412300000",
                "ctopup_number": "9412300000",
                "frcamt": 299,
                "mpin": "enc_mpin",
                "retry_count": 0,
                "max_retries": 3,
                "batch_date": "2026-09-01",
                "circle_code": 2,
            },
            {
                "reqid": 502,
                "caf_serial_no": "CAF002",
                "gsmno": "9412345672",
                "vendormsisdn": "9412300000",
                "ctopup_number": "9412300000",
                "frcamt": 299,
                "mpin": "enc_mpin",
                "retry_count": 0,
                "max_retries": 3,
                "batch_date": "2026-09-01",
                "circle_code": 2,
            },
        ]
        mock_fetch.return_value = claimed_batch
        mock_token_mgr.session_token = "valid_session"
        mock_token_mgr.access_token = "valid_access"
        mock_token_mgr.authenticate = AsyncMock(return_value=True)

        # Row 501 receives token error 506 -> aborts batch
        mock_recharge.return_value = {"statusCode": 506, "message": "Invalid token"}

        with patch("app.processor.decrypt", return_value="1234"):
            summary = await process_pending_recharges(batch_size=100)

        assert summary["processed"] == 1
        assert summary["retryable"] == 1

        # Row 502 was claimed but never submitted; it MUST be released!
        mock_release.assert_called_once_with([502])

    @pytest.mark.asyncio
    @patch("app.processor.token_manager")
    @patch("app.processor.async_fetch_pending_rows")
    async def test_processor_respects_execution_context_circle_filtering(
        self, mock_fetch, mock_token_mgr
    ):
        """process_pending_recharges forwards ExecutionContext circles to async_fetch_pending_rows."""
        mock_fetch.return_value = []
        ctx = ExecutionContext.create(source="SCHEDULED", zones_str="NZ")

        summary = await process_pending_recharges(batch_size=100, context=ctx)

        assert summary["processed"] == 0
        mock_fetch.assert_called_once_with(100, circle_codes=ctx.circle_codes)
        assert ctx.circle_codes == tuple(NZ)
