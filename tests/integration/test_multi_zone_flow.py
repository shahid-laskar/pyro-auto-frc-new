"""Integration test suite for multi-zone (NZ,WZ) end-to-end flow (Phase 15).

Verifies the combined multi-zone execution:
1. Context resolution for NZ,WZ (14 circles total: 9 NZ + 5 WZ).
2. Oracle discovery (Q019) queries combined NZ + WZ circles.
3. PostgreSQL KYC enrichment (Q022) queries combined NZ + WZ circles.
4. Cross-zone candidate staging into PostgreSQL (Q023).
5. Atomic Oracle claim writeback (Q020) for cross-zone batch.
6. Dispatch processor (Q024) claims pending rows across both NZ and WZ.
7. Strict zone boundary isolation: EZ and SZ circles are completely excluded.
"""

from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from app.batch.populator import run_batch_population
from app.context import ExecutionContext
from app.db.oracle import BCD_STATUS_W
from app.processor import process_pending_recharges
from app.zones import EZ, SZ, get_zone_circles


class TestMultiZoneFlowIntegration:
    """Integration tests verifying multi-zone (NZ,WZ) staging and dispatch flows."""

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
    async def test_multi_zone_end_to_end_population_and_dispatch(
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
        """Combined multi-zone execution across NZ and WZ with cross-zone candidates."""
        ctx = ExecutionContext.create(
            source="SCHEDULED",
            zones_str="NZ,WZ",
            execution_id="exec_multi_int_001",
        )
        assert ctx.zone_codes == ("NZ", "WZ")
        expected_circles = tuple(sorted(get_zone_circles("NZ") + get_zone_circles("WZ")))
        assert ctx.circle_codes == expected_circles
        assert len(ctx.circle_codes) == 14

        # Mock Oracle discovery returning 1 NZ row (circle 2) and 1 WZ row (circle 1)
        mock_oracle_fetch.return_value = [
            {
                "GSMNUMBER": "9412300001",
                "CAF_SERIAL_NO": "CAF_NZ_001",
                "CIRCLE_CODE": 2,  # NZ
                "DE_CSCCODE": "CSC_UPW",
                "HLR_FINAL_ACT_DATE": "2026-09-01 10:00:00",
            },
            {
                "GSMNUMBER": "9422300002",
                "CAF_SERIAL_NO": "CAF_WZ_002",
                "CIRCLE_CODE": 1,  # WZ (Maharashtra)
                "DE_CSCCODE": "CSC_MAH",
                "HLR_FINAL_ACT_DATE": "2026-09-01 10:15:00",
            },
        ]

        # Mock Postgres KYC matching both
        mock_pg_kyc.return_value = [
            {
                "gsmnumber": "9412300001",
                "caf_serial_no": "CAF_NZ_001",
                "circle_code": 2,
                "vendorid": "VEND_NZ",
                "vendormsisdn": "9412399991",
                "frcamt": 299,
                "frc_plan_name": "FRC 299",
                "frc_plan_code": "P299",
                "frc_category_code": "CAT1",
                "ctopup_number": "9412399991",
                "mpin_raw": "1111",
                "kyc_mode": "EKYC",
                "de_csccode": "CSC_UPW",
                "live_photo_time": "2026-09-01 10:05:00",
            },
            {
                "gsmnumber": "9422300002",
                "caf_serial_no": "CAF_WZ_002",
                "circle_code": 1,
                "vendorid": "VEND_WZ",
                "vendormsisdn": "9422399992",
                "frcamt": 499,
                "frc_plan_name": "FRC 499",
                "frc_plan_code": "P499",
                "frc_category_code": "CAT2",
                "ctopup_number": "9422399992",
                "mpin_raw": "2222",
                "kyc_mode": "DKYC",
                "de_csccode": "CSC_MAH",
                "live_photo_time": "2026-09-01 10:20:00",
            },
        ]

        # Mock bulk insert staging
        mock_bulk_insert.return_value = [
            {"reqid": 601, "caf_serial_no": "CAF_NZ_001"},
            {"reqid": 602, "caf_serial_no": "CAF_WZ_002"},
        ]
        mock_writeback.return_value = 2
        mock_mark_dispatchable.return_value = 2

        # 1. Run Population
        pop_summary = run_batch_population(context=ctx)

        # Verify Q019 & Q022 received all 14 circles
        _, oracle_kwargs = mock_oracle_fetch.call_args
        assert set(oracle_kwargs["circle_codes"]) == set(expected_circles)

        _, pg_kwargs = mock_pg_kyc.call_args
        assert set(pg_kwargs["circle_codes"]) == set(expected_circles)

        # Verify both rows staged and claimed
        assert pop_summary["execution_id"] == "exec_multi_int_001"
        assert pop_summary["zones"] == ["NZ", "WZ"]
        assert pop_summary["staged"] == 2
        assert pop_summary["claim_success"] == 2
        assert pop_summary["dispatchable"] == 2
        assert pop_summary["errors"] == 0

        # 2. Run Dispatch
        mock_token_mgr.session_token = "sess_multi_token"
        mock_token_mgr.access_token = "acc_multi_token"

        mock_fetch_pending.return_value = [
            {
                "reqid": 601,
                "caf_serial_no": "CAF_NZ_001",
                "gsmno": "9412300001",
                "circle_code": 2,
                "vendormsisdn": "9412399991",
                "ctopup_number": "9412399991",
                "frcamt": 299,
                "mpin": "enc_mpin_1",
                "retry_count": 0,
                "max_retries": 3,
                "batch_date": "2026-09-01",
            },
            {
                "reqid": 602,
                "caf_serial_no": "CAF_WZ_002",
                "gsmno": "9422300002",
                "circle_code": 1,
                "vendormsisdn": "9422399992",
                "ctopup_number": "9422399992",
                "frcamt": 499,
                "mpin": "enc_mpin_2",
                "retry_count": 0,
                "max_retries": 3,
                "batch_date": "2026-09-01",
            },
        ]

        # First recharge returns 2002, second returns 2002
        mock_recharge.side_effect = [
            {"statusCode": 2002, "message": "Success", "data": {"transactionId": "TXN_MULTI_001"}},
            {"statusCode": 2002, "message": "Success", "data": {"transactionId": "TXN_MULTI_002"}},
        ]

        with patch("app.processor.decrypt", side_effect=lambda val, key: "1234"):
            disp_summary = await process_pending_recharges(batch_size=20, context=ctx)

        # Verify Q024 called with multi-zone circle codes
        mock_fetch_pending.assert_called_once_with(20, circle_codes=ctx.circle_codes)
        assert mock_recharge.call_count == 2
        assert mock_mark_pushed.call_count == 2
        assert mock_update_bcd.call_count == 2

        assert disp_summary["execution_id"] == "exec_multi_int_001"
        assert disp_summary["zones"] == ["NZ", "WZ"]
        assert disp_summary["claimed"] == 2
        assert disp_summary["submitted"] == 2
        assert disp_summary["success"] == 2

    def test_multi_zone_isolation_excludes_ez_and_sz(self):
        """Multi-zone NZ,WZ context permits NZ and WZ circles, but strictly denies EZ and SZ."""
        ctx = ExecutionContext.create(source="MANUAL_API", zones_str="NZ,WZ")

        # All circles in NZ and WZ are allowed
        for c in get_zone_circles("NZ") + get_zone_circles("WZ"):
            assert ctx.is_circle_allowed(c) is True

        # All circles in EZ and SZ are denied
        for c in get_zone_circles("EZ") + get_zone_circles("SZ"):
            assert ctx.is_circle_allowed(c) is False
