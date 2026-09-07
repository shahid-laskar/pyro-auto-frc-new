"""Unit tests for Phase 8: Cross-Database Failure Recovery.

Verifies:
1. Critical failure simulation:
   - Q023 (PostgreSQL staging) succeeds -> in_status='S', push_flag='N'
   - Q020 (Oracle claim writeback) fails -> OracleClaimMismatchError / connection failure
   - Staged requests remain in_status='S' (NOT DISPATCHABLE)
   - Pyro recharge processor runs -> Pyro dispatch = 0 for unclaimed requests
2. Reconciliation recovery path (reconcile_staged_requests):
   - Scenario A (Confirmed): Oracle already records FRC_FLOW_STATUS='RQ' and FRC_REQID=reqid
     -> marks dispatchable (in_status='C') without re-claiming or duplicate recharge
     -> subsequent Pyro processor dispatches exactly once
   - Scenario B (Unclaimed): Oracle records FRC_FLOW_STATUS='NP' and FRC_REQID IS NULL
     -> retries Q020 claim writeback via batch_writeback_bcd_rq
     -> marks dispatchable upon claim success -> Pyro dispatches exactly once
   - Scenario C (Conflict): Oracle records FRC_FLOW_STATUS='RQ' for a DIFFERENT reqid
     -> marks request failed (mark_requests_staging_failed) -> in_status='F', push_flag='F'
     -> Pyro dispatch = 0, permanently preventing a duplicate / second recharge
   - Scenario D (Missing): Oracle candidate not found in BCD table
     -> marks request failed -> Pyro dispatch = 0
"""

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.batch.populator import run_batch_population
from app.batch.reconciler import reconcile_staged_requests
from app.db.oracle import (
    BCD_STATUS_NP,
    BCD_STATUS_RQ,
    OracleClaimMismatchError,
    fetch_bcd_claim_statuses,
)
from app.db.postgres import (
    fetch_pending_rows,
    fetch_staged_unconfirmed_requests,
    mark_requests_dispatchable,
    mark_requests_staging_failed,
)
from app.processor import process_pending_recharges


@contextmanager
def _mock_pg_conn(cursor):
    """Context manager helper mimicking get_pg_conn."""
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = None
    yield conn


@contextmanager
def _mock_oracle_conn(cursor):
    """Context manager helper mimicking get_oracle_conn."""
    conn = MagicMock()
    conn.cursor.return_value = cursor
    yield conn


class TestDatabaseHelperQueries:
    """Test suite for direct DB helper functions supporting Phase 8."""

    @patch("app.db.postgres.get_pg_conn")
    def test_fetch_staged_unconfirmed_requests_queries_in_status_s(self, mock_get_conn):
        """fetch_staged_unconfirmed_requests must query WHERE in_status = 'S'."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {
                "reqid": 501,
                "caf_serial_no": "CAF001",
                "gsmno": "9412345678",
                "circle_code": 2,
                "batch_date": "2026-09-01",
                "created_at": "2026-09-01 10:00:00",
            }
        ]
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        result = fetch_staged_unconfirmed_requests(limit=100)

        assert len(result) == 1
        assert result[0]["reqid"] == 501
        assert result[0]["caf_serial_no"] == "CAF001"

        mock_cursor.execute.assert_called_once()
        executed_sql, (limit_param,) = mock_cursor.execute.call_args[0]
        assert "WHERE in_status = 'S'" in executed_sql
        assert "ORDER BY created_at ASC" in executed_sql
        assert limit_param == 100

    def test_fetch_bcd_claim_statuses_empty_input(self):
        """Empty candidates returns empty dict without DB call."""
        with patch("app.db.oracle.get_oracle_conn") as mock_conn:
            result = fetch_bcd_claim_statuses([])
            assert result == {}
            mock_conn.assert_not_called()

    @patch("app.db.oracle.get_oracle_conn")
    def test_fetch_bcd_claim_statuses_queries_oracle_and_maps_keys(self, mock_get_conn):
        """fetch_bcd_claim_statuses returns dictionary keyed by (gsm, caf, circle)."""
        mock_cursor = MagicMock()
        # Row format: (GSMNUMBER, CAF_SERIAL_NO, CIRCLE_CODE, FRC_FLOW_STATUS, FRC_REQID)
        mock_cursor.fetchone.return_value = ("9412345678", "CAF001", 2, "RQ", 501)
        mock_get_conn.side_effect = lambda: _mock_oracle_conn(mock_cursor)

        candidates = [{"gsmnumber": "9412345678", "caf_serial_no": "CAF001", "circle_code": 2}]
        statuses = fetch_bcd_claim_statuses(candidates)

        assert ("9412345678", "CAF001", 2) in statuses
        rec = statuses[("9412345678", "CAF001", 2)]
        assert rec["FRC_FLOW_STATUS"] == "RQ"
        assert rec["FRC_REQID"] == 501

        mock_cursor.execute.assert_called_once()
        executed_sql, binds = mock_cursor.execute.call_args[0]
        assert "WHERE GSMNUMBER     = :gsmnumber" in executed_sql
        assert "AND CAF_SERIAL_NO = :caf_serial_no" in executed_sql
        assert "AND CIRCLE_CODE   = :circle_code" in executed_sql
        assert binds["gsmnumber"] == "9412345678"
        assert binds["caf_serial_no"] == "CAF001"
        assert binds["circle_code"] == 2


class TestRequiredCrossDatabaseSimulation:
    """Test suite executing the required simulation in Section 12 of the Implementation Plan:

    1. Simulate Q023 (Postgres staging) = success, Q020 (Oracle claim) = failure.
    2. Verify Pyro dispatch = 0 for the unclaimed request.
    3. Simulate recovery.
    """

    @pytest.mark.asyncio
    @patch("app.processor.recharge")
    @patch("app.processor.token_manager")
    @patch("app.db.postgres.get_pg_conn")
    @patch("app.batch.populator.mark_requests_dispatchable")
    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    async def test_step1_and_step2_q023_success_q020_failure_pyro_dispatch_is_zero(
        self,
        mock_fetch_oracle,
        mock_fetch_pg,
        mock_bulk_insert,
        mock_writeback,
        mock_dispatchable,
        mock_pg_conn,
        mock_token_mgr,
        mock_recharge,
    ):
        """Simulate Q023 = success, Q020 = failure; verify Pyro dispatch = 0."""
        # 1. Oracle discovery Q019 finds candidate
        mock_fetch_oracle.return_value = [
            {
                "GSMNUMBER": "9412345678",
                "CAF_SERIAL_NO": "CAF001",
                "DE_CSCCODE": "CSC01",
                "CIRCLE_CODE": 2,
                "HLR_FINAL_ACT_DATE": "2026-09-01 10:00:00",
            }
        ]
        # 2. Postgres enrichment Q022 matches
        mock_fetch_pg.return_value = [
            {
                "gsmnumber": "9412345678",
                "caf_serial_no": "CAF001",
                "de_csccode": "CSC01",
                "circle_code": "2",
                "live_photo_time": "2026-09-01 10:00:00",
                "frc_plan_name": "Plan1",
                "frc_plan_code": "P01",
                "frc_category_code": "C01",
                "frcamt": 299,
                "ctopup_number": "9412300000",
                "mpin_raw": "1234",
                "vendorid": "VEND01",
                "vendormsisdn": "9412300000",
                "kyc_mode": "EKYC",
            }
        ]
        # 3. Postgres staging Q023 succeeds -> inserted with in_status='S'
        mock_bulk_insert.return_value = [
            {
                "reqid": 501,
                "caf_serial_no": "CAF001",
                "gsmno": "9412345678",
                "gsmnumber": "9412345678",
                "circle_code": 2,
            }
        ]
        # 4. Oracle claim Q020 FAILS!
        mock_writeback.side_effect = OracleClaimMismatchError(
            "Oracle Q020 claim verification mismatch: expected 1 claims, got 0 successes"
        )

        populator_summary = run_batch_population()

        assert populator_summary["inserted"] == 1
        assert populator_summary["bcd_rq_updated"] == 0
        assert populator_summary["dispatchable"] == 0
        assert populator_summary["errors"] == 1

        # CRITICAL: mark_requests_dispatchable was NOT called
        mock_dispatchable.assert_not_called()

        # 5. Verify Pyro dispatch = 0 for the unclaimed request!
        # Postgres fetch_pending_rows only queries WHERE in_status = 'C'
        # Since reqid 501 is in_status = 'S', the query returns EMPTY list!
        mock_pg_cursor = MagicMock()
        mock_pg_cursor.fetchall.return_value = []  # No rows with in_status='C'
        mock_pg_conn.side_effect = lambda: _mock_pg_conn(mock_pg_cursor)

        mock_token_mgr.session_token = "valid_session"
        mock_token_mgr.access_token = "valid_access"

        recharge_summary = await process_pending_recharges(batch_size=100)

        # Pyro dispatch MUST be 0!
        assert recharge_summary["processed"] == 0
        assert recharge_summary["registered"] == 0
        mock_recharge.assert_not_called()

    @patch("app.batch.reconciler.mark_requests_dispatchable")
    @patch("app.batch.reconciler.batch_writeback_bcd_rq")
    @patch("app.batch.reconciler.fetch_bcd_claim_statuses")
    @patch("app.batch.reconciler.fetch_staged_unconfirmed_requests")
    def test_step3_recovery_scenario_a_oracle_already_claimed(
        self,
        mock_fetch_staged,
        mock_fetch_oracle_status,
        mock_claim_writeback,
        mock_mark_dispatchable,
    ):
        """Simulate recovery: Oracle claim succeeded earlier before network disconnect.

        Reconciler confirms the request without issuing another claim writeback,
        preventing duplicate claim and second recharge.
        """
        # Staged request in Postgres (in_status='S')
        mock_fetch_staged.return_value = [
            {
                "reqid": 501,
                "caf_serial_no": "CAF001",
                "gsmno": "9412345678",
                "circle_code": 2,
            }
        ]
        # Oracle inspection reveals record is already claimed for reqid 501!
        mock_fetch_oracle_status.return_value = {
            ("9412345678", "CAF001", 2): {
                "GSMNUMBER": "9412345678",
                "CAF_SERIAL_NO": "CAF001",
                "CIRCLE_CODE": 2,
                "FRC_FLOW_STATUS": BCD_STATUS_RQ,
                "FRC_REQID": 501,
            }
        }
        mock_mark_dispatchable.return_value = 1

        summary = reconcile_staged_requests(batch_size=100)

        assert summary["staged_found"] == 1
        assert summary["already_claimed_confirmed"] == 1
        assert summary["reclaimed_confirmed"] == 0
        assert summary["conflict_marked_failed"] == 0

        # Dispatched without re-claiming in Oracle!
        mock_claim_writeback.assert_not_called()
        mock_mark_dispatchable.assert_called_once_with([501])

    @patch("app.batch.reconciler.mark_requests_dispatchable")
    @patch("app.batch.reconciler.batch_writeback_bcd_rq")
    @patch("app.batch.reconciler.fetch_bcd_claim_statuses")
    @patch("app.batch.reconciler.fetch_staged_unconfirmed_requests")
    def test_step3_recovery_scenario_b_oracle_unclaimed_retry_succeeds(
        self,
        mock_fetch_staged,
        mock_fetch_oracle_status,
        mock_claim_writeback,
        mock_mark_dispatchable,
    ):
        """Simulate recovery: Oracle record is unclaimed ('NP'), retry claim succeeds."""
        mock_fetch_staged.return_value = [
            {
                "reqid": 501,
                "caf_serial_no": "CAF001",
                "gsmno": "9412345678",
                "circle_code": 2,
            }
        ]
        # Oracle inspection reveals record was never claimed (still NP and NULL reqid)
        mock_fetch_oracle_status.return_value = {
            ("9412345678", "CAF001", 2): {
                "GSMNUMBER": "9412345678",
                "CAF_SERIAL_NO": "CAF001",
                "CIRCLE_CODE": 2,
                "FRC_FLOW_STATUS": BCD_STATUS_NP,
                "FRC_REQID": None,
            }
        }
        mock_claim_writeback.return_value = 1
        mock_mark_dispatchable.return_value = 1

        summary = reconcile_staged_requests(batch_size=100)

        assert summary["staged_found"] == 1
        assert summary["reclaimed_confirmed"] == 1
        assert summary["already_claimed_confirmed"] == 0
        assert summary["conflict_marked_failed"] == 0

        mock_claim_writeback.assert_called_once_with([mock_fetch_staged.return_value[0]])
        mock_mark_dispatchable.assert_called_once_with([501])

    @patch("app.batch.reconciler.mark_requests_staging_failed")
    @patch("app.batch.reconciler.mark_requests_dispatchable")
    @patch("app.batch.reconciler.batch_writeback_bcd_rq")
    @patch("app.batch.reconciler.fetch_bcd_claim_statuses")
    @patch("app.batch.reconciler.fetch_staged_unconfirmed_requests")
    def test_step3_recovery_scenario_c_oracle_conflict_different_reqid_marks_failed(
        self,
        mock_fetch_staged,
        mock_fetch_oracle_status,
        mock_claim_writeback,
        mock_mark_dispatchable,
        mock_mark_failed,
    ):
        """Simulate recovery: Oracle record claimed by DIFFERENT reqid (999).

        Marks request failed to permanently hold and prevent a duplicate recharge!
        """
        mock_fetch_staged.return_value = [
            {
                "reqid": 501,
                "caf_serial_no": "CAF001",
                "gsmno": "9412345678",
                "circle_code": 2,
            }
        ]
        # Oracle inspection reveals record belongs to another reqid (999)!
        mock_fetch_oracle_status.return_value = {
            ("9412345678", "CAF001", 2): {
                "GSMNUMBER": "9412345678",
                "CAF_SERIAL_NO": "CAF001",
                "CIRCLE_CODE": 2,
                "FRC_FLOW_STATUS": BCD_STATUS_RQ,
                "FRC_REQID": 999,
            }
        }
        mock_mark_failed.return_value = 1

        summary = reconcile_staged_requests(batch_size=100)

        assert summary["staged_found"] == 1
        assert summary["conflict_marked_failed"] == 1
        assert summary["already_claimed_confirmed"] == 0
        assert summary["reclaimed_confirmed"] == 0

        # NEVER mark dispatchable and NEVER retry claim!
        mock_mark_dispatchable.assert_not_called()
        mock_claim_writeback.assert_not_called()

        # Staged request marked failed
        mock_mark_failed.assert_called_once()
        call_reqids, call_kwargs = mock_mark_failed.call_args[0], mock_mark_failed.call_args[1]
        assert call_reqids[0] == [501]
        assert "Reconciliation conflict" in call_kwargs["reason"]

    @patch("app.batch.reconciler.mark_requests_staging_failed")
    @patch("app.batch.reconciler.mark_requests_dispatchable")
    @patch("app.batch.reconciler.fetch_bcd_claim_statuses")
    @patch("app.batch.reconciler.fetch_staged_unconfirmed_requests")
    def test_step3_recovery_scenario_d_oracle_candidate_missing_marks_failed(
        self,
        mock_fetch_staged,
        mock_fetch_oracle_status,
        mock_mark_dispatchable,
        mock_mark_failed,
    ):
        """Simulate recovery: Oracle record deleted/missing in BCD table."""
        mock_fetch_staged.return_value = [
            {
                "reqid": 501,
                "caf_serial_no": "CAF001",
                "gsmno": "9412345678",
                "circle_code": 2,
            }
        ]
        mock_fetch_oracle_status.return_value = {}  # Record not found
        mock_mark_failed.return_value = 1

        summary = reconcile_staged_requests(batch_size=100)

        assert summary["staged_found"] == 1
        assert summary["conflict_marked_failed"] == 1
        mock_mark_dispatchable.assert_not_called()
        mock_mark_failed.assert_called_once()
        assert mock_mark_failed.call_args[0][0] == [501]
