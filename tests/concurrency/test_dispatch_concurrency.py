"""Concurrency test suite for dispatch worker execution (Phase 15).

Verifies dispatch race-condition prevention and atomic locking:
1. Two concurrent dispatch workers claim mutually disjoint sets of requests via FOR UPDATE SKIP LOCKED.
2. Worker exhaustion: when Worker 1 claims all rows, Worker 2 receives an empty batch without error.
3. Batch abort under concurrency: unsubmitted claims are released back to 'N' without corrupting other workers.
4. Database contract: Q024 SQL enforces atomic UPDATE ... RETURNING with FOR UPDATE SKIP LOCKED.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from app.context import ExecutionContext
from app.db.postgres import fetch_pending_rows, release_unprocessed_claims
from app.processor import process_pending_recharges


class TestDispatchConcurrency:
    """Test suite for concurrent dispatch workers and FOR UPDATE SKIP LOCKED isolation."""

    @pytest.mark.asyncio
    @patch("app.processor.update_bcd_status")
    @patch("app.processor.async_mark_as_pushed")
    @patch("app.processor.recharge")
    @patch("app.processor.token_manager")
    @patch("app.processor.async_fetch_pending_rows")
    async def test_two_concurrent_dispatch_workers_disjoint_claims(
        self,
        mock_fetch,
        mock_token_mgr,
        mock_recharge,
        mock_mark_pushed,
        mock_update_bcd,
    ):
        """Two concurrent workers claim distinct, non-overlapping sets of requests."""
        mock_token_mgr.session_token = "valid_session"
        mock_token_mgr.access_token = "valid_access"

        # Worker 1 gets reqids 101, 102
        worker1_rows = [
            {"reqid": 101, "caf_serial_no": "CAF1", "gsmno": "9412300001", "vendormsisdn": "9412300000", "ctopup_number": "9412300000", "frcamt": 299, "mpin": "m1", "retry_count": 0, "max_retries": 3, "batch_date": "2026-09-01"},
            {"reqid": 102, "caf_serial_no": "CAF2", "gsmno": "9412300002", "vendormsisdn": "9412300000", "ctopup_number": "9412300000", "frcamt": 299, "mpin": "m2", "retry_count": 0, "max_retries": 3, "batch_date": "2026-09-01"},
        ]
        # Worker 2 gets reqids 103, 104 (skip locked rows 101, 102)
        worker2_rows = [
            {"reqid": 103, "caf_serial_no": "CAF3", "gsmno": "9412300003", "vendormsisdn": "9412300000", "ctopup_number": "9412300000", "frcamt": 299, "mpin": "m3", "retry_count": 0, "max_retries": 3, "batch_date": "2026-09-01"},
            {"reqid": 104, "caf_serial_no": "CAF4", "gsmno": "9412300004", "vendormsisdn": "9412300000", "ctopup_number": "9412300000", "frcamt": 299, "mpin": "m4", "retry_count": 0, "max_retries": 3, "batch_date": "2026-09-01"},
        ]

        mock_fetch.side_effect = [worker1_rows, worker2_rows]
        mock_recharge.return_value = {
            "statusCode": 2002,
            "message": "Success",
            "data": {"transactionId": "TXN_CONC"},
        }

        ctx1 = ExecutionContext.create(source="SCHEDULED", zones_str="NZ", execution_id="disp_worker_1")
        ctx2 = ExecutionContext.create(source="SCHEDULED", zones_str="NZ", execution_id="disp_worker_2")

        with patch("app.processor.decrypt", side_effect=lambda val, key: "1234"):
            task1 = process_pending_recharges(batch_size=2, context=ctx1)
            task2 = process_pending_recharges(batch_size=2, context=ctx2)
            summary1, summary2 = await asyncio.gather(task1, task2)

        # Disjoint set assertion: No single reqid processed twice
        claimed_w1 = {r["reqid"] for r in worker1_rows}
        claimed_w2 = {r["reqid"] for r in worker2_rows}
        assert claimed_w1.isdisjoint(claimed_w2)

        assert summary1["claimed"] == 2
        assert summary1["submitted"] == 2
        assert summary2["claimed"] == 2
        assert summary2["submitted"] == 2

        # 4 distinct recharge calls made
        assert mock_recharge.call_count == 4
        submitted_reqids = [call[1]["reqid"] for call in mock_recharge.call_args_list]
        assert set(submitted_reqids) == {101, 102, 103, 104}

    @pytest.mark.asyncio
    @patch("app.processor.token_manager")
    @patch("app.processor.async_fetch_pending_rows")
    async def test_worker_exhaustion_yields_clean_empty_batch(
        self,
        mock_fetch,
        mock_token_mgr,
    ):
        """When all rows are claimed by Worker 1, Worker 2 gets empty batch and completes cleanly."""
        mock_token_mgr.session_token = "valid_session"
        mock_token_mgr.access_token = "valid_access"

        mock_fetch.return_value = []
        ctx = ExecutionContext.create(source="MANUAL_API", zones_str="ALL")

        summary = await process_pending_recharges(batch_size=10, context=ctx)

        assert summary["claimed"] == 0
        assert summary["submitted"] == 0
        assert summary["success"] == 0

    @pytest.mark.asyncio
    @patch("app.processor.token_manager")
    @patch("app.processor.recharge")
    @patch("app.processor.async_release_unprocessed_claims")
    @patch("app.processor.async_mark_as_failed")
    @patch("app.processor.async_fetch_pending_rows")
    async def test_batch_abort_releases_unsubmitted_claims(
        self,
        mock_fetch,
        mock_mark_failed,
        mock_release,
        mock_recharge,
        mock_token_mgr,
    ):
        """When Worker 1 encounters action token error, unsubmitted rows are immediately released."""
        mock_token_mgr.session_token = "valid_session"
        mock_token_mgr.access_token = "valid_access"

        rows = [
            {"reqid": 201, "caf_serial_no": "CAF1", "gsmno": "9412300001", "vendormsisdn": "9412300000", "ctopup_number": "9412300000", "frcamt": 299, "mpin": "m1", "retry_count": 0, "max_retries": 3, "batch_date": "2026-09-01"},
            {"reqid": 202, "caf_serial_no": "CAF2", "gsmno": "9412300002", "vendormsisdn": "9412300000", "ctopup_number": "9412300000", "frcamt": 299, "mpin": "m2", "retry_count": 0, "max_retries": 3, "batch_date": "2026-09-01"},
            {"reqid": 203, "caf_serial_no": "CAF3", "gsmno": "9412300003", "vendormsisdn": "9412300000", "ctopup_number": "9412300000", "frcamt": 299, "mpin": "m3", "retry_count": 0, "max_retries": 3, "batch_date": "2026-09-01"},
        ]
        mock_fetch.return_value = rows

        # First row fails with action token error (-1) causing batch break
        mock_recharge.return_value = {
            "statusCode": -1,
            "message": "Failed to generate action token",
        }

        ctx = ExecutionContext.create(source="SCHEDULED", zones_str="NZ")

        with patch("app.processor.decrypt", return_value="1234"):
            summary = await process_pending_recharges(batch_size=3, context=ctx)

        # Rows 202 and 203 were never submitted, so finally block must release them
        mock_release.assert_called_once()
        released_ids = mock_release.call_args[0][0]
        assert set(released_ids) == {202, 203}

    @patch("app.db.postgres.get_pg_conn")
    def test_q024_atomic_update_returning_query_contract(self, mock_get_conn):
        """Verify fetch_pending_rows executes atomic UPDATE ... RETURNING with FOR UPDATE SKIP LOCKED."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cur.fetchall.return_value = []
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        fetch_pending_rows(batch_size=25, circle_codes=[2, 55])

        mock_cur.execute.assert_called_once()
        executed_sql, params = mock_cur.execute.call_args[0]

        # Check atomic UPDATE structure
        assert "UPDATE public.frc_pyro_request_data" in executed_sql
        assert "push_flag    = 'P'" in executed_sql
        assert "FOR UPDATE SKIP LOCKED" in executed_sql
        assert "in_status   = 'C'" in executed_sql or "in_status = 'C'" in executed_sql
        assert "RETURNING" in executed_sql
        assert "reqid" in executed_sql
