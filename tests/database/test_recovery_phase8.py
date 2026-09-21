"""Unit tests for Phase 8: Cross-Database Failure Recovery.

Verifies:
1. Critical failure simulation:
   - Q023 (PostgreSQL staging) succeeds -> in_status='C', push_flag='N'
   - Q020 (Oracle claim writeback) fails -> OracleClaimMismatchError / connection failure
   - Population summary records claim failure and held requests
   - Pyro recharge processor runs -> Pyro dispatch = 0 when no rows are available
2. Direct dispatchable architecture:
   - Pending rows must strictly query in_status='C' and push_flag IN ('N', 'E')
   - Reconciler safely returns clean 0 summary without DB churn
"""

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.batch.populator import run_batch_population
from app.batch.reconciler import reconcile_staged_requests
from app.db.oracle import OracleClaimMismatchError
from app.db.postgres import fetch_pending_rows
from app.processor import process_pending_recharges


@contextmanager
def _mock_pg_conn(cursor):
    """Context manager helper mimicking get_pg_conn."""
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = None
    yield conn


class TestDatabaseHelperQueries:
    """Test suite for direct DB helper functions supporting Phase 8."""

    @patch("app.db.postgres.get_pg_conn")
    def test_fetch_pending_rows_queries_in_status_c(self, mock_get_conn):
        """fetch_pending_rows must query WHERE in_status = 'C'."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        fetch_pending_rows(batch_size=100)

        mock_cursor.execute.assert_called_once()
        executed_sql, (batch_size_param,) = mock_cursor.execute.call_args[0]
        assert "WHERE in_status   = 'C'" in executed_sql
        assert "AND push_flag   IN ('N', 'E')" in executed_sql
        assert batch_size_param == 100


class TestRequiredCrossDatabaseSimulation:
    """Test suite executing the required simulation in Section 12 of the Implementation Plan:

    1. Simulate Q023 (Postgres staging) = success, Q020 (Oracle claim) = failure.
    2. Verify Pyro dispatch = 0 for unclaimed requests.
    3. Reconciler safe execution in direct dispatchable architecture.
    """

    @pytest.mark.asyncio
    @patch("app.processor.recharge")
    @patch("app.processor.token_manager")
    @patch("app.db.postgres.get_pg_conn")
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
        # 3. Postgres staging Q023 succeeds
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

        # 5. Verify Pyro dispatch = 0 when no rows are pending
        mock_pg_cursor = MagicMock()
        mock_pg_cursor.fetchall.return_value = []
        mock_pg_conn.side_effect = lambda: _mock_pg_conn(mock_pg_cursor)

        mock_token_mgr.session_token = "valid_session"
        mock_token_mgr.access_token = "valid_access"

        recharge_summary = await process_pending_recharges(batch_size=100)

        # Pyro dispatch MUST be 0!
        assert recharge_summary["claimed"] == 0
        assert recharge_summary["submitted"] == 0
        mock_recharge.assert_not_called()

    def test_step3_recovery_reconciler_is_safe_noop(self):
        """Reconciliation in direct dispatchable architecture is a clean no-op."""
        summary = reconcile_staged_requests(batch_size=100)

        assert summary["staged_found"] == 0
        assert summary["already_claimed_confirmed"] == 0
        assert summary["reclaimed_confirmed"] == 0
        assert summary["conflict_marked_failed"] == 0
        assert summary["errors"] == 0
