"""Fault tolerance test suite for Q020 Oracle claim writeback failure (Phase 15).

Verifies staging integrity, failure containment, and dispatch guard:
1. Complete Oracle claim writeback failure:
   - Staged rows in PostgreSQL remain strictly in_status='S' (NOT DISPATCHABLE).
   - mark_requests_dispatchable is never invoked.
   - Summary accurately records claim_failed > 0 and held > 0.
   - Dispatch processor finds 0 dispatchable rows and triggers 0 Pyro recharges.
2. Partial Oracle claim writeback mismatch:
   - When fewer rows are updated than expected, the batch dispatchable transition is halted.
   - All staged rows remain held in in_status='S' for reconciliation.
3. OracleClaimMismatchError handling:
   - Hard mismatch exceptions are safely caught and contained without corrupting state.
"""

from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from app.batch.populator import run_batch_population
from app.context import ExecutionContext
from app.db.oracle import OracleClaimMismatchError
from app.processor import process_pending_recharges


class TestQ020FailureFaultTolerance:
    """Test suite verifying system resilience against Oracle Q020 claim writeback failures."""

    @pytest.mark.asyncio
    @patch("app.processor.recharge")
    @patch("app.processor.token_manager")
    @patch("app.processor.async_fetch_pending_rows")
    @patch("app.batch.populator.mark_requests_dispatchable")
    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    async def test_q020_complete_failure_holds_staged_requests_and_prevents_dispatch(
        self,
        mock_oracle_fetch,
        mock_pg_kyc,
        mock_bulk_insert,
        mock_writeback,
        mock_mark_dispatchable,
        mock_fetch_pending,
        mock_token_mgr,
        mock_recharge,
    ):
        """When Q020 fails, staged rows remain in_status='S' and dispatch is completely blocked."""
        ctx = ExecutionContext.create(source="SCHEDULED", zones_str="NZ")

        mock_oracle_fetch.return_value = [
            {
                "GSMNUMBER": "9412300001",
                "CAF_SERIAL_NO": "CAF_001",
                "CIRCLE_CODE": 2,
                "DE_CSCCODE": "CSC1",
                "HLR_FINAL_ACT_DATE": "2026-09-01",
            }
        ]
        mock_pg_kyc.return_value = [
            {
                "gsmnumber": "9412300001",
                "caf_serial_no": "CAF_001",
                "circle_code": 2,
                "vendorid": "V1",
                "vendormsisdn": "9412300000",
                "frcamt": 299,
                "mpin_raw": "1234",
                "kyc_mode": "EKYC",
            }
        ]
        # PostgreSQL staging succeeds
        mock_bulk_insert.return_value = [{"reqid": 801, "caf_serial_no": "CAF_001"}]

        # Oracle claim writeback fails with network timeout
        mock_writeback.side_effect = RuntimeError("Oracle ORA-12170: TNS:Connect timeout occurred")

        # 1. Run population
        summary = run_batch_population(context=ctx)

        # Staging succeeded, but claim failed
        assert summary["staged"] == 1
        assert summary["claim_expected"] == 1
        assert summary["claim_success"] == 0
        assert summary["claim_failed"] == 1
        assert summary["held"] == 1
        assert summary["dispatchable"] == 0
        assert summary["errors"] == 1

        # CRITICAL GUARD: mark_requests_dispatchable must NEVER be called
        mock_mark_dispatchable.assert_not_called()

        # 2. Run dispatch processor
        mock_token_mgr.session_token = "valid_session"
        mock_token_mgr.access_token = "valid_access"
        # Since in_status='S', the DB query for in_status='C' returns empty
        mock_fetch_pending.return_value = []

        disp_summary = await process_pending_recharges(batch_size=10, context=ctx)

        # 0 requests claimed or dispatched
        assert disp_summary["claimed"] == 0
        assert disp_summary["submitted"] == 0
        mock_recharge.assert_not_called()

    @patch("app.batch.populator.mark_requests_dispatchable")
    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_q020_partial_claim_mismatch_halts_dispatch_transition(
        self,
        mock_oracle_fetch,
        mock_pg_kyc,
        mock_bulk_insert,
        mock_writeback,
        mock_mark_dispatchable,
    ):
        """When Oracle updates fewer rows than staged, none of the rows transition to dispatchable."""
        ctx = ExecutionContext.create(source="SCHEDULED", zones_str="NZ")

        mock_oracle_fetch.return_value = [
            {"GSMNUMBER": "9412300001", "CAF_SERIAL_NO": "CAF1", "CIRCLE_CODE": 2, "DE_CSCCODE": "C1", "HLR_FINAL_ACT_DATE": "2026-09-01"},
            {"GSMNUMBER": "9412300002", "CAF_SERIAL_NO": "CAF2", "CIRCLE_CODE": 2, "DE_CSCCODE": "C1", "HLR_FINAL_ACT_DATE": "2026-09-01"},
            {"GSMNUMBER": "9412300003", "CAF_SERIAL_NO": "CAF3", "CIRCLE_CODE": 2, "DE_CSCCODE": "C1", "HLR_FINAL_ACT_DATE": "2026-09-01"},
        ]
        mock_pg_kyc.return_value = [
            {"gsmnumber": "9412300001", "caf_serial_no": "CAF1", "circle_code": 2, "vendorid": "V1", "vendormsisdn": "9412300000", "frcamt": 299, "mpin_raw": "11", "kyc_mode": "EKYC"},
            {"gsmnumber": "9412300002", "caf_serial_no": "CAF2", "circle_code": 2, "vendorid": "V1", "vendormsisdn": "9412300000", "frcamt": 299, "mpin_raw": "22", "kyc_mode": "EKYC"},
            {"gsmnumber": "9412300003", "caf_serial_no": "CAF3", "circle_code": 2, "vendorid": "V1", "vendormsisdn": "9412300000", "frcamt": 299, "mpin_raw": "33", "kyc_mode": "EKYC"},
        ]
        mock_bulk_insert.return_value = [
            {"reqid": 811, "caf_serial_no": "CAF1"},
            {"reqid": 812, "caf_serial_no": "CAF2"},
            {"reqid": 813, "caf_serial_no": "CAF3"},
        ]
        # Only 2 rows updated in Oracle instead of 3
        mock_writeback.return_value = 2

        summary = run_batch_population(context=ctx)

        assert summary["staged"] == 3
        assert summary["claim_expected"] == 3
        assert summary["claim_success"] == 2
        assert summary["claim_failed"] == 1
        assert summary["held"] == 3
        assert summary["dispatchable"] == 0

        mock_mark_dispatchable.assert_not_called()

    @patch("app.batch.populator.mark_requests_dispatchable")
    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_q020_claim_mismatch_exception_handled(
        self,
        mock_oracle_fetch,
        mock_pg_kyc,
        mock_bulk_insert,
        mock_writeback,
        mock_mark_dispatchable,
    ):
        """OracleClaimMismatchError is trapped and logged without crashing the process."""
        ctx = ExecutionContext.create(source="MANUAL_API", zones_str="ALL")

        mock_oracle_fetch.return_value = [
            {"GSMNUMBER": "9412300001", "CAF_SERIAL_NO": "CAF1", "CIRCLE_CODE": 2, "DE_CSCCODE": "C1", "HLR_FINAL_ACT_DATE": "2026-09-01"},
        ]
        mock_pg_kyc.return_value = [
            {"gsmnumber": "9412300001", "caf_serial_no": "CAF1", "circle_code": 2, "vendorid": "V1", "vendormsisdn": "9412300000", "frcamt": 299, "mpin_raw": "11", "kyc_mode": "EKYC"},
        ]
        mock_bulk_insert.return_value = [{"reqid": 821, "caf_serial_no": "CAF1"}]
        mock_writeback.side_effect = OracleClaimMismatchError("Oracle rowcount mismatch: expected 1, updated 0")

        summary = run_batch_population(context=ctx)

        assert summary["staged"] == 1
        assert summary["claim_failed"] == 1
        assert summary["held"] == 1
        assert summary["dispatchable"] == 0
        assert summary["errors"] == 1
        mock_mark_dispatchable.assert_not_called()
