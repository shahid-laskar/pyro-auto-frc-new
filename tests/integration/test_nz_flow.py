"""Integration test suite for single-zone (NZ) end-to-end flow (Phase 15).

Verifies the entire lifecycle under single-zone NZ execution:
1. Context resolution for NZ (9 circles: 2, 55, 56, 59, 60, 61, 62, 64, 65).
2. Oracle discovery (Q019) filtered strictly by NZ circles.
3. PostgreSQL KYC enrichment (Q022) filtered strictly by NZ circles.
4. Staging into PostgreSQL (Q023) in_status='S'.
5. Oracle claim writeback (Q020) transitions BCD to RQ.
6. Staged row marked dispatchable (in_status='C').
7. Dispatch processor (Q024) claims dispatchable NZ row via FOR UPDATE SKIP LOCKED.
8. Pyro recharge submission, callback wait status (W), and pushed flag (P).
9. Strict circle isolation: non-NZ circles are never queried or dispatched.
"""

from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from app.batch.populator import run_batch_population
from app.context import ExecutionContext
from app.db.oracle import BCD_STATUS_W
from app.processor import process_pending_recharges
from app.zones import NZ, WZ, EZ, SZ, get_zone_circles


class TestNZFlowIntegration:
    """Integration tests verifying end-to-end single zone (NZ) processing."""

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
    async def test_nz_end_to_end_population_and_dispatch(
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
        """Complete end-to-end flow: NZ discovery -> staging -> claim -> dispatch -> Pyro submit."""
        # 1. Initialize context for NZ
        ctx = ExecutionContext.create(
            source="SCHEDULED",
            zones_str="NZ",
            execution_id="exec_nz_int_001",
        )
        assert ctx.zone_codes == ("NZ",)
        assert ctx.circle_codes == tuple(sorted(get_zone_circles("NZ")))
        assert len(ctx.circle_codes) == 9

        # 2. Mock Oracle BCD discovery (Q019)
        mock_oracle_fetch.return_value = [
            {
                "GSMNUMBER": "9412300001",
                "CAF_SERIAL_NO": "CAF_NZ_001",
                "CIRCLE_CODE": 2,  # UP West (NZ)
                "DE_CSCCODE": "CSC_UPW",
                "HLR_FINAL_ACT_DATE": "2026-09-01 10:00:00",
            },
            {
                "GSMNUMBER": "9412300002",
                "CAF_SERIAL_NO": "CAF_NZ_002",
                "CIRCLE_CODE": 55,  # Haryana (NZ)
                "DE_CSCCODE": "CSC_HAR",
                "HLR_FINAL_ACT_DATE": "2026-09-01 10:30:00",
            },
        ]

        # 3. Mock PostgreSQL KYC enrichment (Q022)
        mock_pg_kyc.return_value = [
            {
                "gsmnumber": "9412300001",
                "caf_serial_no": "CAF_NZ_001",
                "circle_code": 2,
                "vendorid": "VEND01",
                "vendormsisdn": "9412399991",
                "frcamt": 299,
                "frc_plan_name": "FRC 299",
                "frc_plan_code": "PLAN299",
                "frc_category_code": "CAT1",
                "ctopup_number": "9412399991",
                "mpin_raw": "1234",
                "kyc_mode": "EKYC",
                "de_csccode": "CSC_UPW",
                "live_photo_time": "2026-09-01 10:05:00",
            },
            {
                "gsmnumber": "9412300002",
                "caf_serial_no": "CAF_NZ_002",
                "circle_code": 55,
                "vendorid": "VEND02",
                "vendormsisdn": "9412399992",
                "frcamt": 399,
                "frc_plan_name": "FRC 399",
                "frc_plan_code": "PLAN399",
                "frc_category_code": "CAT1",
                "ctopup_number": "9412399992",
                "mpin_raw": "5678",
                "kyc_mode": "DKYC",
                "de_csccode": "CSC_HAR",
                "live_photo_time": "2026-09-01 10:35:00",
            },
        ]

        # 4. Mock Bulk Insert (Q023 staging)
        mock_bulk_insert.return_value = [
            {"reqid": 501, "caf_serial_no": "CAF_NZ_001"},
            {"reqid": 502, "caf_serial_no": "CAF_NZ_002"},
        ]
        # 5. Mock Oracle Claim Writeback (Q020)
        mock_writeback.return_value = 2
        mock_mark_dispatchable.return_value = 2

        # --- EXECUTE POPULATION ---
        pop_summary = run_batch_population(context=ctx)

        # Verify Q019 and Q022 invoked with exact NZ circle codes
        mock_oracle_fetch.assert_called_once()
        _, oracle_kwargs = mock_oracle_fetch.call_args
        assert set(oracle_kwargs["circle_codes"]) == set(ctx.circle_codes)

        mock_pg_kyc.assert_called_once()
        _, pg_kwargs = mock_pg_kyc.call_args
        assert set(pg_kwargs["circle_codes"]) == set(ctx.circle_codes)

        # Verify Staging and Claim writeback
        mock_bulk_insert.assert_called_once()
        inserted_rows = mock_bulk_insert.call_args[0][0]
        assert len(inserted_rows) == 2
        assert {r["circle_code"] for r in inserted_rows} == {2, 55}

        mock_writeback.assert_called_once_with([
            {"reqid": 501, "caf_serial_no": "CAF_NZ_001"},
            {"reqid": 502, "caf_serial_no": "CAF_NZ_002"},
        ])
        mock_mark_dispatchable.assert_called_once_with([501, 502])

        # Assert Population Summary metrics
        assert pop_summary["execution_id"] == "exec_nz_int_001"
        assert pop_summary["zones"] == ["NZ"]
        assert pop_summary["mode"] == "FILTERED"
        assert pop_summary["oracle_selected"] == 2
        assert pop_summary["postgres_matched"] == 2
        assert pop_summary["staged"] == 2
        assert pop_summary["claim_success"] == 2
        assert pop_summary["claim_failed"] == 0
        assert pop_summary["held"] == 0
        assert pop_summary["dispatchable"] == 2
        assert pop_summary["errors"] == 0

        # --- EXECUTE DISPATCH ---
        mock_token_mgr.session_token = "sess_nz_123"
        mock_token_mgr.access_token = "acc_nz_456"

        # Mock pending rows returned by Q024 FOR UPDATE SKIP LOCKED
        mock_fetch_pending.return_value = [
            {
                "reqid": 501,
                "caf_serial_no": "CAF_NZ_001",
                "gsmno": "9412300001",
                "circle_code": 2,
                "vendormsisdn": "9412399991",
                "ctopup_number": "9412399991",
                "frcamt": 299,
                "mpin": inserted_rows[0]["mpin"],  # valid encrypted mpin
                "retry_count": 0,
                "max_retries": 3,
                "batch_date": "2026-09-01",
            }
        ]

        mock_recharge.return_value = {
            "statusCode": 2002,
            "message": "Success",
            "data": {"transactionId": "PYRO_TXN_NZ_001"},
        }

        disp_summary = await process_pending_recharges(batch_size=10, context=ctx)

        # Verify Q024 invoked with NZ circle filter
        mock_fetch_pending.assert_called_once_with(10, circle_codes=ctx.circle_codes)

        # Verify Pyro recharge submission
        mock_recharge.assert_called_once()
        recharge_kwargs = mock_recharge.call_args[1]
        assert recharge_kwargs["reqid"] == 501
        assert recharge_kwargs["gsmno"] == "9412300001"
        assert recharge_kwargs["amount"] == 299

        # Verify Postgres marked pushed and Oracle marked W
        mock_mark_pushed.assert_called_once()
        assert mock_mark_pushed.call_args[0][0] == 501
        assert mock_mark_pushed.call_args[0][1] == "PYRO_TXN_NZ_001"

        mock_update_bcd.assert_called_once_with(
            "CAF_NZ_001", 501, BCD_STATUS_W,
            "FRC submitted to Pyro. pyroTxnId=PYRO_TXN_NZ_001"
        )

        assert disp_summary["execution_id"] == "exec_nz_int_001"
        assert disp_summary["zones"] == ["NZ"]
        assert disp_summary["claimed"] == 1
        assert disp_summary["submitted"] == 1
        assert disp_summary["success"] == 1
        assert disp_summary["registered"] == 1

    def test_nz_circle_isolation_predicates(self):
        """Verify strict isolation: NZ circles are allowed; WZ, EZ, SZ circles are denied."""
        ctx = ExecutionContext.create(source="MANUAL_API", zones_str="NZ")

        # All 9 NZ circles must be allowed
        nz_circles = get_zone_circles("NZ")
        for circle in nz_circles:
            assert ctx.is_circle_allowed(circle) is True

        # Non-NZ circles must be rejected
        other_circles = get_zone_circles("WZ") + get_zone_circles("EZ") + get_zone_circles("SZ")
        for circle in other_circles:
            assert ctx.is_circle_allowed(circle) is False

    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_nz_population_discards_cross_zone_mismatch(
        self,
        mock_oracle_fetch,
        mock_pg_kyc,
        mock_bulk_insert,
    ):
        """Cross-zone circle mismatch between Oracle and Postgres is discarded before staging."""
        ctx = ExecutionContext.create(source="MANUAL_API", zones_str="NZ")

        mock_oracle_fetch.return_value = [
            {
                "GSMNUMBER": "9412300001",
                "CAF_SERIAL_NO": "CAF_001",
                "CIRCLE_CODE": 2,  # NZ circle in Oracle
                "DE_CSCCODE": "CSC1",
                "HLR_FINAL_ACT_DATE": "2026-09-01",
            }
        ]
        # Postgres returns mismatched circle_code (e.g. circle 1 from WZ)
        mock_pg_kyc.return_value = [
            {
                "gsmnumber": "9412300001",
                "caf_serial_no": "CAF_001",
                "circle_code": 1,  # WZ circle (mismatch!)
                "vendorid": "V1",
                "vendormsisdn": "9412300000",
                "frcamt": 299,
                "mpin_raw": "1234",
                "kyc_mode": "EKYC",
            }
        ]

        summary = run_batch_population(context=ctx)

        # Mismatched row was skipped
        assert summary["skipped_identity_mismatch"] == 1
        assert summary["staged"] == 0
        mock_bulk_insert.assert_not_called()
