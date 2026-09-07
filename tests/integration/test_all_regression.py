"""Integration regression test suite for nationwide ALL mode (Phase 15 & Phase 17).

Verifies backwards compatibility and nationwide behavior:
1. When zones="ALL" (or omitted), context operates in ALL mode.
2. Oracle discovery (Q019) is invoked with circle_codes=None (circle filter omitted).
3. PostgreSQL KYC enrichment (Q022) is invoked with circle_codes=None (circle filter omitted).
4. Candidates from all 4 zones (NZ, WZ, EZ, SZ) are staged simultaneously.
5. Oracle claim writeback (Q020) claims candidates across all zones.
6. Dispatch processor (Q024) is invoked with circle_codes=None (nationwide dispatch).
7. Legacy invocation without ExecutionContext preserves full ALL behavior.
"""

from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from app.batch.populator import run_batch_population
from app.context import ExecutionContext
from app.db.oracle import BCD_STATUS_W
from app.processor import process_pending_recharges
from app.zones import VALID_ZONES, get_zone_circles


class TestAllRegressionIntegration:
    """Regression test suite for nationwide ALL mode execution."""

    @pytest.mark.asyncio
    @patch("app.processor.update_bcd_status")
    @patch("app.processor.async_mark_as_pushed")
    @patch("app.processor.recharge")
    @patch("app.processor.token_manager")
    @patch("app.processor.async_fetch_pending_rows")
    @patch("app.batch.populator.mark_requests_dispatchable")
    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    async def test_all_mode_end_to_end_nationwide_execution(
        self,
        mock_oracle_fetch,
        mock_pg_kyc,
        mock_bulk_insert,
        mock_writeback,
        mock_mark_dispatchable,
        mock_fetch_pending,
        mock_token_mgr,
        mock_recharge,
        mock_mark_pushed,
        mock_update_bcd,
    ):
        """ALL mode processes candidates from NZ, WZ, EZ, and SZ without circle restrictions."""
        ctx = ExecutionContext.create(
            source="SCHEDULED",
            zones_str="ALL",
            execution_id="exec_all_reg_001",
        )
        assert ctx.mode == "ALL"
        assert ctx.circle_codes is None
        assert ctx.zone_codes == ("ALL",)

        # 4 records spanning NZ(2), WZ(1), EZ(70), SZ(40)
        mock_oracle_fetch.return_value = [
            {"GSMNUMBER": "9412300001", "CAF_SERIAL_NO": "CAF_NZ", "CIRCLE_CODE": 2, "DE_CSCCODE": "CSC1", "HLR_FINAL_ACT_DATE": "2026-09-01"},
            {"GSMNUMBER": "9422300002", "CAF_SERIAL_NO": "CAF_WZ", "CIRCLE_CODE": 1, "DE_CSCCODE": "CSC2", "HLR_FINAL_ACT_DATE": "2026-09-01"},
            {"GSMNUMBER": "9432300003", "CAF_SERIAL_NO": "CAF_EZ", "CIRCLE_CODE": 70, "DE_CSCCODE": "CSC3", "HLR_FINAL_ACT_DATE": "2026-09-01"},
            {"GSMNUMBER": "9442300004", "CAF_SERIAL_NO": "CAF_SZ", "CIRCLE_CODE": 40, "DE_CSCCODE": "CSC4", "HLR_FINAL_ACT_DATE": "2026-09-01"},
        ]

        mock_pg_kyc.return_value = [
            {"gsmnumber": "9412300001", "caf_serial_no": "CAF_NZ", "circle_code": 2, "vendorid": "V1", "vendormsisdn": "9412399991", "frcamt": 299, "mpin_raw": "1111", "kyc_mode": "EKYC"},
            {"gsmnumber": "9422300002", "caf_serial_no": "CAF_WZ", "circle_code": 1, "vendorid": "V2", "vendormsisdn": "9422399992", "frcamt": 399, "mpin_raw": "2222", "kyc_mode": "DKYC"},
            {"gsmnumber": "9432300003", "caf_serial_no": "CAF_EZ", "circle_code": 70, "vendorid": "V3", "vendormsisdn": "9432399993", "frcamt": 499, "mpin_raw": "3333", "kyc_mode": "EKYC"},
            {"gsmnumber": "9442300004", "caf_serial_no": "CAF_SZ", "circle_code": 40, "vendorid": "V4", "vendormsisdn": "9442399994", "frcamt": 599, "mpin_raw": "4444", "kyc_mode": "DKYC"},
        ]

        mock_bulk_insert.return_value = [
            {"reqid": 701, "caf_serial_no": "CAF_NZ"},
            {"reqid": 702, "caf_serial_no": "CAF_WZ"},
            {"reqid": 703, "caf_serial_no": "CAF_EZ"},
            {"reqid": 704, "caf_serial_no": "CAF_SZ"},
        ]
        mock_writeback.return_value = 4
        mock_mark_dispatchable.return_value = 4

        # 1. Run Population in ALL mode
        pop_summary = run_batch_population(context=ctx)

        # Assert circle_codes is None for Q019 & Q022 (no circle filter)
        _, oracle_kwargs = mock_oracle_fetch.call_args
        assert oracle_kwargs["circle_codes"] is None

        _, pg_kwargs = mock_pg_kyc.call_args
        assert pg_kwargs["circle_codes"] is None

        assert pop_summary["mode"] == "ALL"
        assert pop_summary["zones"] == ["ALL"]
        assert pop_summary["oracle_selected"] == 4
        assert pop_summary["postgres_matched"] == 4
        assert pop_summary["staged"] == 4
        assert pop_summary["claim_success"] == 4
        assert pop_summary["dispatchable"] == 4
        assert pop_summary["errors"] == 0

        # 2. Run Dispatch in ALL mode
        mock_token_mgr.session_token = "sess_all_token"
        mock_token_mgr.access_token = "acc_all_token"

        mock_fetch_pending.return_value = [
            {"reqid": 701, "caf_serial_no": "CAF_NZ", "gsmno": "9412300001", "circle_code": 2, "vendormsisdn": "9412399991", "ctopup_number": "9412399991", "frcamt": 299, "mpin": "enc1", "retry_count": 0, "max_retries": 3, "batch_date": "2026-09-01"},
            {"reqid": 702, "caf_serial_no": "CAF_WZ", "gsmno": "9422300002", "circle_code": 1, "vendormsisdn": "9422399992", "ctopup_number": "9422399992", "frcamt": 399, "mpin": "enc2", "retry_count": 0, "max_retries": 3, "batch_date": "2026-09-01"},
            {"reqid": 703, "caf_serial_no": "CAF_EZ", "gsmno": "9432300003", "circle_code": 70, "vendormsisdn": "9432399993", "ctopup_number": "9432399993", "frcamt": 499, "mpin": "enc3", "retry_count": 0, "max_retries": 3, "batch_date": "2026-09-01"},
            {"reqid": 704, "caf_serial_no": "CAF_SZ", "gsmno": "9442300004", "circle_code": 40, "vendormsisdn": "9442399994", "ctopup_number": "9442399994", "frcamt": 599, "mpin": "enc4", "retry_count": 0, "max_retries": 3, "batch_date": "2026-09-01"},
        ]

        mock_recharge.return_value = {
            "statusCode": 2002,
            "message": "Success",
            "data": {"transactionId": "TXN_ALL"},
        }

        with patch("app.processor.decrypt", side_effect=lambda val, key: "1234"):
            disp_summary = await process_pending_recharges(batch_size=50, context=ctx)

        # Q024 called with circle_codes=None
        mock_fetch_pending.assert_called_once_with(50, circle_codes=None)
        assert mock_recharge.call_count == 4
        assert mock_mark_pushed.call_count == 4
        assert mock_update_bcd.call_count == 4

        assert disp_summary["mode"] == "ALL"
        assert disp_summary["zones"] == ["ALL"]
        assert disp_summary["claimed"] == 4
        assert disp_summary["submitted"] == 4
        assert disp_summary["success"] == 4

    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.mark_requests_dispatchable")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_legacy_invocation_without_context_defaults_to_nationwide(
        self,
        mock_oracle_fetch,
        mock_pg_kyc,
        mock_mark_dispatchable,
        mock_writeback,
        mock_bulk_insert,
    ):
        """Calling run_batch_population() without context passes circle_codes=None."""
        mock_oracle_fetch.return_value = []

        summary = run_batch_population()

        assert summary["mode"] is None
        assert summary["zones"] is None
        _, oracle_kwargs = mock_oracle_fetch.call_args
        assert oracle_kwargs["circle_codes"] is None
