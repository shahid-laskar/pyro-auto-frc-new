"""Unit tests for Phase 13: Observability and Execution Correlation.

Verifies:
1. Population summary exposes:
   - execution_id, source, zones, mode
   - oracle_selected, postgres_matched, staged, claim_expected, claim_success, claim_failed, held
2. Population logs structured context [exec_id=...][source=...][zones=...][mode=...].
3. Dispatch summary exposes:
   - execution_id, source, zones, mode
   - claimed, submitted, success, transient_failure, permanent_failure, retry
4. Dispatch logs structured context [exec_id=...][source=...][zones=...][mode=...].
5. Security hygiene: No MPIN, password, token, or secret key is logged.
"""

from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from app.batch.populator import run_batch_population
from app.context import ExecutionContext
from app.processor import process_pending_recharges


class TestPopulationObservability:
    @patch("app.batch.populator.logger.info")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.mark_requests_dispatchable")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_population_summary_and_logging_with_context(
        self,
        mock_oracle,
        mock_pg,
        mock_dispatchable,
        mock_writeback,
        mock_insert,
        mock_log_info,
    ):
        """Population summary exposes all Phase 13 required metrics and correlation metadata."""
        ctx = ExecutionContext.create(
            source="SCHEDULED",
            zones_str="NZ",
            execution_id="exec_obs_pop_001",
        )

        mock_oracle.return_value = [
            {
                "GSMNUMBER": "9412300001",
                "CAF_SERIAL_NO": "CAF01",
                "CIRCLE_CODE": 2,
                "DE_CSCCODE": "CSC1",
                "HLR_FINAL_ACT_DATE": "2026-09-01",
            },
            {
                "GSMNUMBER": "9412300002",
                "CAF_SERIAL_NO": "CAF02",
                "CIRCLE_CODE": 2,
                "DE_CSCCODE": "CSC1",
                "HLR_FINAL_ACT_DATE": "2026-09-01",
            },
        ]
        mock_pg.return_value = [
            {
                "gsmnumber": "9412300001",
                "caf_serial_no": "CAF01",
                "circle_code": 2,
                "vendorid": "V1",
                "vendormsisdn": "9412300000",
                "frcamt": 299,
                "mpin_raw": "1234",
                "kyc_mode": "EKYC",
            },
            {
                "gsmnumber": "9412300002",
                "caf_serial_no": "CAF02",
                "circle_code": 2,
                "vendorid": "V1",
                "vendormsisdn": "9412300000",
                "frcamt": 299,
                "mpin_raw": "5678",
                "kyc_mode": "DKYC",
            },
        ]
        mock_insert.return_value = [
            {"reqid": 101, "caf_serial_no": "CAF01"},
            {"reqid": 102, "caf_serial_no": "CAF02"},
        ]
        mock_writeback.return_value = 2
        mock_dispatchable.return_value = 2

        summary = run_batch_population(context=ctx)

        # 1. Execution correlation
        assert summary["execution_id"] == "exec_obs_pop_001"
        assert summary["source"] == "SCHEDULED"
        assert summary["zones"] == ["NZ"]
        assert summary["mode"] == "FILTERED"

        # 2. Phase 13 population summary metrics
        assert summary["oracle_selected"] == 2
        assert summary["postgres_matched"] == 2
        assert summary["staged"] == 2
        assert summary["claim_expected"] == 2
        assert summary["claim_success"] == 2
        assert summary["claim_failed"] == 0
        assert summary["held"] == 0

        # 3. Structured logging output
        formatted_logs = [
            c[0][0] % c[0][1:] if len(c[0]) > 1 else c[0][0]
            for c in mock_log_info.call_args_list
        ]
        start_log = next(m for m in formatted_logs if "Batch population started" in m)
        assert "[exec_id=exec_obs_pop_001][source=SCHEDULED][zones=['NZ']][mode=FILTERED]" in start_log

        complete_log = next(m for m in formatted_logs if "Batch complete" in m)
        assert "[exec_id=exec_obs_pop_001][source=SCHEDULED][zones=['NZ']][mode=FILTERED]" in complete_log
        assert "oracle_selected=2" in complete_log
        assert "staged=2" in complete_log
        assert "claim_success=2" in complete_log

    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.mark_requests_dispatchable")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_population_summary_on_claim_failure_reports_held(
        self, mock_oracle, mock_pg, mock_dispatchable, mock_writeback, mock_insert
    ):
        """When Oracle writeback fails, staged rows are reported as claim_failed and held."""
        ctx = ExecutionContext.create(source="MANUAL_API", zones_str="ALL")

        mock_oracle.return_value = [
            {
                "GSMNUMBER": "9412300001",
                "CAF_SERIAL_NO": "CAF01",
                "CIRCLE_CODE": 1,
                "DE_CSCCODE": "CSC1",
                "HLR_FINAL_ACT_DATE": "2026-09-01",
            }
        ]
        mock_pg.return_value = [
            {
                "gsmnumber": "9412300001",
                "caf_serial_no": "CAF01",
                "circle_code": 1,
                "vendorid": "V1",
                "vendormsisdn": "9412300000",
                "frcamt": 299,
                "mpin_raw": "1234",
                "kyc_mode": "EKYC",
            }
        ]
        mock_insert.return_value = [{"reqid": 201, "caf_serial_no": "CAF01"}]
        # Oracle claim fails
        mock_writeback.side_effect = RuntimeError("Oracle timeout")

        summary = run_batch_population(context=ctx)

        assert summary["staged"] == 1
        assert summary["claim_expected"] == 1
        assert summary["claim_success"] == 0
        assert summary["claim_failed"] == 1
        assert summary["held"] == 1
        assert summary["dispatchable"] == 0
        mock_dispatchable.assert_not_called()


class TestDispatchObservability:
    @pytest.mark.asyncio
    @patch("app.processor.logger.info")
    @patch("app.processor.token_manager")
    @patch("app.processor.recharge")
    @patch("app.processor.async_mark_as_pushed")
    @patch("app.processor.async_mark_as_failed")
    @patch("app.processor.update_bcd_status")
    @patch("app.processor.async_fetch_pending_rows")
    async def test_dispatch_summary_and_logging_with_context(
        self,
        mock_fetch,
        mock_update_bcd,
        mock_mark_failed,
        mock_mark_pushed,
        mock_recharge,
        mock_token,
        mock_log_info,
    ):
        """Dispatch summary exposes all Phase 13 required metrics and correlation metadata."""
        ctx = ExecutionContext.create(
            source="SCHEDULED",
            zones_str="WZ",
            execution_id="exec_obs_disp_001",
        )

        mock_token.session_token = "valid_session"
        mock_token.access_token = "valid_access"

        mock_fetch.return_value = [
            {
                "reqid": 301,
                "caf_serial_no": "CAF01",
                "gsmno": "9412300001",
                "vendormsisdn": "9412300000",
                "ctopup_number": "9412300000",
                "frcamt": 299,
                "mpin": "enc_mpin_1",
                "retry_count": 0,
                "max_retries": 3,
                "batch_date": "2026-09-07",
            },
            {
                "reqid": 302,
                "caf_serial_no": "CAF02",
                "gsmno": "9412300002",
                "vendormsisdn": "9412300000",
                "ctopup_number": "9412300000",
                "frcamt": 299,
                "mpin": "enc_mpin_2",
                "retry_count": 0,
                "max_retries": 3,
                "batch_date": "2026-09-07",
            },
            {
                "reqid": 303,
                "caf_serial_no": "CAF03",
                "gsmno": "9412300003",
                "vendormsisdn": "9412300000",
                "ctopup_number": "9412300000",
                "frcamt": 299,
                "mpin": "enc_mpin_3",
                "retry_count": 0,
                "max_retries": 3,
                "batch_date": "2026-09-07",
            },
        ]

        # 301 -> success (2002)
        # 302 -> transient failure (5000)
        # 303 -> permanent failure (5006)
        mock_recharge.side_effect = [
            {"statusCode": 2002, "data": {"transactionId": "TXN_301"}},
            {"statusCode": 5000, "message": "IN timeout"},
            {"statusCode": 5006, "message": "Invalid denomination"},
        ]

        with patch("app.processor.decrypt", return_value="1234"):
            summary = await process_pending_recharges(context=ctx)

        # 1. Execution correlation
        assert summary["execution_id"] == "exec_obs_disp_001"
        assert summary["source"] == "SCHEDULED"
        assert summary["zones"] == ["WZ"]
        assert summary["mode"] == "FILTERED"

        # 2. Phase 13 dispatch summary metrics
        assert summary["claimed"] == 3
        assert summary["submitted"] == 3
        assert summary["success"] == 1
        assert summary["transient_failure"] == 1
        assert summary["permanent_failure"] == 1
        assert summary["retry"] == 1

        # 3. Structured logging output
        formatted_logs = [
            c[0][0] % c[0][1:] if len(c[0]) > 1 else c[0][0]
            for c in mock_log_info.call_args_list
        ]
        start_log = next(m for m in formatted_logs if "Processor started" in m)
        assert "[exec_id=exec_obs_disp_001][source=SCHEDULED][zones=['WZ']][mode=FILTERED]" in start_log

        complete_log = next(m for m in formatted_logs if "Processor complete" in m)
        assert "[exec_id=exec_obs_disp_001][source=SCHEDULED][zones=['WZ']][mode=FILTERED]" in complete_log
        assert "claimed=3" in complete_log
        assert "submitted=3" in complete_log
        assert "success=1" in complete_log
        assert "transient_failure=1" in complete_log
        assert "permanent_failure=1" in complete_log

    @pytest.mark.asyncio
    @patch("app.processor.async_fetch_pending_rows")
    async def test_dispatch_empty_batch_reports_zero_metrics(self, mock_fetch):
        """When no rows are pending, summary reports zeroes for all dispatch metrics."""
        ctx = ExecutionContext.create(source="SCHEDULED", zones_str="ALL")
        mock_fetch.return_value = []

        summary = await process_pending_recharges(context=ctx)

        assert summary["execution_id"] == ctx.execution_id
        assert summary["claimed"] == 0
        assert summary["submitted"] == 0
        assert summary["success"] == 0
        assert summary["transient_failure"] == 0
        assert summary["permanent_failure"] == 0
        assert summary["retry"] == 0


class TestSecurityLoggingHygiene:
    @pytest.mark.asyncio
    @patch("app.processor.logger.info")
    @patch("app.processor.token_manager")
    @patch("app.processor.recharge")
    @patch("app.processor.async_mark_as_pushed")
    @patch("app.processor.update_bcd_status")
    @patch("app.processor.async_fetch_pending_rows")
    async def test_no_sensitive_data_in_processor_logs(
        self,
        mock_fetch,
        mock_update_bcd,
        mock_mark_pushed,
        mock_recharge,
        mock_token,
        mock_log_info,
    ):
        """Ensures that cleartext MPIN or tokens are never emitted to loggers."""
        mock_token.session_token = "secret_session_token_xyz"
        mock_token.access_token = "secret_access_token_abc"
        mock_fetch.return_value = [
            {
                "reqid": 401,
                "caf_serial_no": "CAF01",
                "gsmno": "9412300001",
                "vendormsisdn": "9412300000",
                "ctopup_number": "9412300000",
                "frcamt": 299,
                "mpin": "enc_mpin_9999",
                "retry_count": 0,
                "max_retries": 3,
                "batch_date": "2026-09-07",
            }
        ]
        mock_recharge.return_value = {"statusCode": 2002, "data": {"transactionId": "TXN_401"}}

        sensitive_mpin = "SUPER_SECRET_MPIN_9999"
        with patch("app.processor.decrypt", return_value=sensitive_mpin):
            await process_pending_recharges()

        for call in mock_log_info.call_args_list:
            formatted_msg = call[0][0] % call[0][1:] if len(call[0]) > 1 else call[0][0]
            assert sensitive_mpin not in formatted_msg
            assert "secret_session_token_xyz" not in formatted_msg
            assert "secret_access_token_abc" not in formatted_msg
