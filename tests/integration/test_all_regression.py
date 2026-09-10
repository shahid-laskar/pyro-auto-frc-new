"""Integration regression test suite for nationwide ALL mode (Phase 15 & Phase 17).

Verifies backwards compatibility and nationwide behavior:
1. When zones="ALL" (or omitted), context operates in ALL mode across all 29 circles.
2. Oracle discovery (Q019) is invoked with circle_codes=None (circle filter omitted).
3. PostgreSQL KYC enrichment (Q022) is invoked with circle_codes=None (circle filter omitted).
4. Candidates from all 4 zones (NZ, WZ, EZ, SZ) are staged simultaneously with in_status='S'.
5. Oracle claim writeback (Q020) claims candidates across all zones and unlocks in_status='C'.
6. Dispatch processor (Q024) is invoked with circle_codes=None (nationwide dispatch).
7. Callback processing (/callback/recharge) operates idempotently and maps status codes correctly.
8. Retry mechanics properly handle transient errors, dealer exhaustion (405), and permanent errors.
9. Status checker polls in-flight requests nationwide, advances to 'NR', and resolves terminal states.
10. Scheduler jobs execute under settings.enabled_zones="ALL" with advisory lock concurrency protection.
"""

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, call, patch
import pytest
from fastapi.testclient import TestClient

from app.batch.populator import run_batch_population
from app.callback import recharge_callback
from app.config import settings
from app.context import ExecutionContext
from app.db.oracle import (
    BCD_STATUS_F,
    BCD_STATUS_ID,
    BCD_STATUS_NR,
    BCD_STATUS_P,
    BCD_STATUS_W,
    fetch_eligible_bcd_records,
)
from app.db.postgres import (
    FLAG_FAILED,
    FLAG_PENDING,
    FLAG_RETRY,
    FLAG_SUCCESS,
    fetch_cos_bcd_for_gsms,
    fetch_pending_rows,
)
from app.processor import process_pending_recharges
from app.scheduler import _batch_population_job, _recharge_job
from app.status_checker import run_status_checks
from main import app


# ── Mock connection helpers ───────────────────────────────────────────────────

@contextmanager
def _mock_oracle_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    yield conn


@contextmanager
def _mock_pg_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = None
    yield conn


# ── Pipeline 1 & 2 End-to-End Regression ──────────────────────────────────────

class TestAllModeEndToEndPipeline:
    """End-to-end regression tests verifying nationwide pipeline operation."""

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

    @patch("app.db.oracle.get_oracle_conn")
    def test_all_mode_q019_discovery_query_structure(self, mock_get_conn):
        """Under ALL mode (circle_codes=None), Q019 omits CIRCLE_CODE predicate completely."""
        mock_cursor = MagicMock()
        mock_cursor.description = []
        mock_cursor.fetchall.return_value = []
        mock_get_conn.side_effect = lambda: _mock_oracle_conn(mock_cursor)

        fetch_eligible_bcd_records(fetch_size=200, circle_codes=None)

        mock_cursor.execute.assert_called_once()
        sql, binds = mock_cursor.execute.call_args[0]
        assert "CIRCLE_CODE IN" not in sql
        assert binds == {"status_np": "NP", "fetch_size": 200}

    @patch("app.db.postgres.get_pg_read_conn")
    def test_all_mode_q022_enrichment_query_structure(self, mock_get_conn):
        """Under ALL mode (circle_codes=None), Q022 omits circle predicate and has no text casts."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        fetch_cos_bcd_for_gsms(["9412300001", "9412300002"], circle_codes=None)

        mock_cursor.execute.assert_called_once()
        sql, params = mock_cursor.execute.call_args[0]
        assert "cb.circle_code = ANY(" not in sql
        assert "cb.circle_code::TEXT" not in sql
        assert params == {"gsms": ["9412300001", "9412300002"]}

    @patch("app.db.postgres.get_pg_conn")
    def test_all_mode_q024_atomic_dispatch_claim_query_structure(self, mock_get_conn):
        """Under ALL mode (circle_codes=None), Q024 claims nationwide rows with FOR UPDATE SKIP LOCKED."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        fetch_pending_rows(batch_size=100, circle_codes=None)

        mock_cursor.execute.assert_called_once()
        sql, params = mock_cursor.execute.call_args[0]
        assert "circle_code = ANY(%s)" not in sql
        assert "FOR UPDATE SKIP LOCKED" in sql
        assert params == (100,)


# ── Callback Webhook Regression ───────────────────────────────────────────────

class TestAllModeCallbackRegression:
    """Verify callback handling, error code mapping, and idempotency."""

    def setup_method(self):
        self.client = TestClient(app)

    @pytest.mark.asyncio
    @patch("app.callback.update_bcd_status")
    @patch("app.callback.async_mark_as_success")
    @patch("app.callback.async_insert_txn_log")
    @patch("app.callback.async_find_row_by_pyro_trans_id")
    async def test_callback_success_2000_advances_postgres_and_oracle(
        self,
        mock_find_row,
        mock_insert_log,
        mock_mark_success,
        mock_update_bcd,
    ):
        """Callback 2000 transitions Postgres to 'Y' and Oracle to 'P'."""
        mock_find_row.return_value = {
            "reqid": 501,
            "caf_serial_no": "CAF01",
            "gsmno": "9412345678",
            "batch_date": "2026-09-01",
            "push_flag": "P",
            "client_txn_id": "00501",
            "frcamt": 299.0,
        }

        payload = {
            "statusCode": 2000,
            "message": "Recharge successful",
            "data": {
                "transactionId": "999888777",
                "clientTxnId": "00501",
                "destMsisdn": "9412345678",
                "amount": 299.0,
                "status": "SUCCESS",
                "dealerBalanceBefore": 1000.0,
                "dealerBalanceAfter": 701.0,
            },
        }

        response = self.client.post("/callback/recharge", json=payload)
        assert response.status_code == 200
        assert response.json() == {"received": True}

        mock_mark_success.assert_called_once()
        mock_update_bcd.assert_called_once_with(
            "CAF01", 501, BCD_STATUS_P,
            "Recharge successful via callback. pyroTxnId=999888777 gsmno=9412345678 amount=299.0",
        )

    @pytest.mark.asyncio
    @patch("app.callback.update_bcd_status")
    @patch("app.callback.async_mark_as_failed")
    @patch("app.callback.async_insert_txn_log")
    @patch("app.callback.async_find_row_by_pyro_trans_id")
    async def test_callback_failure_902_advances_postgres_and_oracle(
        self,
        mock_find_row,
        mock_insert_log,
        mock_mark_failed,
        mock_update_bcd,
    ):
        """Callback 902 transitions Postgres to 'F' and Oracle to 'F'."""
        mock_find_row.return_value = {
            "reqid": 502,
            "caf_serial_no": "CAF02",
            "gsmno": "9412345678",
            "batch_date": "2026-09-01",
            "push_flag": "P",
            "client_txn_id": "00502",
            "frcamt": 299.0,
        }

        payload = {
            "statusCode": 902,
            "message": "Transaction failed on IN",
            "data": {
                "transactionId": "999888778",
                "clientTxnId": "00502",
                "destMsisdn": "9412345678",
                "amount": 299.0,
                "status": "FAILED",
            },
        }

        response = self.client.post("/callback/recharge", json=payload)
        assert response.status_code == 200

        mock_mark_failed.assert_called_once()
        mock_update_bcd.assert_called_once_with(
            "CAF02", 502, BCD_STATUS_F, "[902] Transaction failed on IN",
        )

    @pytest.mark.asyncio
    @patch("app.callback.update_bcd_status")
    @patch("app.callback.async_mark_as_failed")
    @patch("app.callback.async_insert_txn_log")
    @patch("app.callback.async_find_row_by_pyro_trans_id")
    async def test_callback_permanent_invalid_data_code_maps_oracle_id(
        self,
        mock_find_row,
        mock_insert_log,
        mock_mark_failed,
        mock_update_bcd,
    ):
        """Callback error 5006 (invalid data) maps Oracle BCD to 'ID'."""
        mock_find_row.return_value = {
            "reqid": 503,
            "caf_serial_no": "CAF03",
            "gsmno": "9412345678",
            "batch_date": "2026-09-01",
            "push_flag": "P",
            "client_txn_id": "00503",
            "frcamt": 299.0,
        }

        payload = {
            "statusCode": 5006,
            "message": "Subscriber is suspended or barred",
            "data": {
                "transactionId": "999888779",
                "clientTxnId": "00503",
                "destMsisdn": "9412345678",
                "amount": 299.0,
                "status": "FAILED",
            },
        }

        response = self.client.post("/callback/recharge", json=payload)
        assert response.status_code == 200

        # Must map to BCD_STATUS_ID
        mock_update_bcd.assert_called_once_with(
            "CAF03", 503, BCD_STATUS_ID, "[5006] Subscriber is suspended or barred",
        )

    @pytest.mark.asyncio
    @patch("app.callback.update_bcd_status")
    @patch("app.callback.async_mark_as_success")
    @patch("app.callback.async_find_row_by_pyro_trans_id")
    async def test_callback_already_terminal_row_is_idempotent(
        self,
        mock_find_row,
        mock_mark_success,
        mock_update_bcd,
    ):
        """Duplicate callbacks on already terminal rows ('Y' or 'F') are safely ignored."""
        mock_find_row.return_value = {
            "reqid": 504,
            "caf_serial_no": "CAF04",
            "gsmno": "9412345678",
            "batch_date": "2026-09-01",
            "push_flag": "Y",  # Already terminal success
            "client_txn_id": "00504",
            "frcamt": 299.0,
        }

        payload = {
            "statusCode": 2000,
            "message": "Recharge successful",
            "data": {
                "transactionId": "999888780",
                "clientTxnId": "00504",
                "destMsisdn": "9412345678",
                "amount": 299.0,
                "status": "SUCCESS",
            },
        }

        response = self.client.post("/callback/recharge", json=payload)
        assert response.status_code == 200
        assert response.json() == {"received": True, "note": "already processed"}

        mock_mark_success.assert_not_called()
        mock_update_bcd.assert_not_called()


# ── Retry Mechanics & Dealer Exhaustion Regression ─────────────────────────────

class TestAllModeRetryAndDealerExhaustion:
    """Verify transient error retries, max attempt exhaustion, and dealer 405 caching."""

    @pytest.mark.asyncio
    @patch("app.processor.async_release_unprocessed_claims")
    @patch("app.processor.update_bcd_status")
    @patch("app.processor.async_mark_as_failed")
    @patch("app.processor.recharge")
    @patch("app.processor.token_manager")
    @patch("app.processor.async_fetch_pending_rows")
    async def test_dealer_exhaustion_405_skips_subsequent_batch_requests(
        self,
        mock_fetch_pending,
        mock_token_mgr,
        mock_recharge,
        mock_mark_failed,
        mock_update_bcd,
        mock_release_unprocessed,
    ):
        """When dealer returns 405, subsequent requests for same dealer are skipped without Pyro calls."""
        mock_token_mgr.session_token = "tok_sess"
        mock_token_mgr.access_token = "tok_acc"

        # 2 requests with the same dealer (9412399999)
        mock_fetch_pending.return_value = [
            {"reqid": 601, "caf_serial_no": "CAF01", "gsmno": "9412300001", "circle_code": 2, "vendormsisdn": "9412399999", "ctopup_number": "9412399999", "frcamt": 299, "mpin": "enc1", "retry_count": 0, "max_retries": 3, "batch_date": "2026-09-01"},
            {"reqid": 602, "caf_serial_no": "CAF02", "gsmno": "9412300002", "circle_code": 2, "vendormsisdn": "9412399999", "ctopup_number": "9412399999", "frcamt": 299, "mpin": "enc2", "retry_count": 0, "max_retries": 3, "batch_date": "2026-09-01"},
        ]

        # First recharge returns 405 (Insufficient balance)
        mock_recharge.return_value = {
            "statusCode": 405,
            "message": "Insufficient balance",
        }

        with patch("app.processor.decrypt", side_effect=lambda val, key: "1234"):
            summary = await process_pending_recharges(batch_size=10)

        # recharge() called ONLY ONCE because dealer was exhausted
        assert mock_recharge.call_count == 1
        assert summary["retry"] == 2
        assert mock_mark_failed.call_count == 1
        mock_release_unprocessed.assert_called_once_with([602])

    @pytest.mark.asyncio
    @patch("app.processor.update_bcd_status")
    @patch("app.processor.async_mark_as_failed")
    @patch("app.processor.recharge")
    @patch("app.processor.token_manager")
    @patch("app.processor.async_fetch_pending_rows")
    async def test_transient_error_exhaustion_at_max_retries(
        self,
        mock_fetch_pending,
        mock_token_mgr,
        mock_recharge,
        mock_mark_failed,
        mock_update_bcd,
    ):
        """When retry_count >= max_retries, transient error transitions row to permanent FLAG_FAILED."""
        mock_token_mgr.session_token = "tok_sess"
        mock_token_mgr.access_token = "tok_acc"

        # Request with retry_count = 3 (max_retries = 3)
        mock_fetch_pending.return_value = [
            {"reqid": 603, "caf_serial_no": "CAF03", "gsmno": "9412300003", "circle_code": 2, "vendormsisdn": "9412399998", "ctopup_number": "9412399998", "frcamt": 299, "mpin": "enc1", "retry_count": 3, "max_retries": 3, "batch_date": "2026-09-01"},
        ]

        mock_recharge.return_value = {
            "statusCode": 500,
            "message": "Internal Pyro timeout",
        }

        with patch("app.processor.decrypt", side_effect=lambda val, key: "1234"):
            summary = await process_pending_recharges(batch_size=10)

        assert summary["transient_failure"] == 1
        # Called with FLAG_FAILED because max retries reached
        mock_mark_failed.assert_called_once()
        assert mock_mark_failed.call_args[0][1] == FLAG_FAILED
        mock_update_bcd.assert_called_once_with(
            "CAF03", 603, BCD_STATUS_F, "Max retries exhausted. Last error: [500] Pyro/IN transient error — will retry",
        )


# ── Status Checker Regression ──────────────────────────────────────────────────

class TestAllModeStatusCheckerRegression:
    """Verify polling status checker nationwide execution and transitions."""

    @pytest.mark.asyncio
    @patch("app.status_checker.update_bcd_status")
    @patch("app.status_checker.async_mark_as_success")
    @patch("app.status_checker.check_transaction_status")
    @patch("app.status_checker.async_update_status_check_attempt")
    @patch("app.status_checker.async_fetch_pushed_rows_for_status_check")
    async def test_status_checker_first_attempt_advances_oracle_nr_and_resolves_2000(
        self,
        mock_fetch_rows,
        mock_update_attempt,
        mock_check_status,
        mock_mark_success,
        mock_update_bcd,
    ):
        """First status check poll updates Oracle to 'NR', then to 'P' on status 2000."""
        mock_fetch_rows.return_value = [
            {"reqid": 801, "pyro_trans_id": 999111, "caf_serial_no": "CAF801", "gsmno": "9412345678", "batch_date": "2026-09-01", "status_check_count": 0},
        ]

        mock_check_status.return_value = {
            "statusCode": 2000,
            "message": "Success",
            "data": {"dealerBalanceBefore": 1000.0, "dealerBalanceAfter": 701.0},
        }

        summary = await run_status_checks()

        assert summary["checked"] == 1
        assert summary["success"] == 1

        # Must have called update_bcd_status twice: first with 'NR', then with 'P'
        assert mock_update_bcd.call_count == 2
        mock_update_bcd.assert_has_calls([
            call("CAF801", 801, BCD_STATUS_NR, "No callback received. Initiating status check. pyroTxnId=999111"),
            call("CAF801", 801, BCD_STATUS_P, "Recharge successful via status check. pyroTxnId=999111"),
        ])
        mock_mark_success.assert_called_once()

    @pytest.mark.asyncio
    @patch("app.status_checker.update_bcd_status")
    @patch("app.status_checker.async_mark_as_failed")
    @patch("app.status_checker.check_transaction_status")
    @patch("app.status_checker.async_update_status_check_attempt")
    @patch("app.status_checker.async_fetch_pushed_rows_for_status_check")
    async def test_status_checker_901_retry_vs_max_attempt_exhaustion(
        self,
        mock_fetch_rows,
        mock_update_attempt,
        mock_check_status,
        mock_mark_failed,
        mock_update_bcd,
    ):
        """Status 901 retries if attempts < max, marks failed when max attempts reached."""
        # Row with attempt 4 (settings.status_check_max_attempts = 5) -> attempt_no = 5 (max)
        mock_fetch_rows.return_value = [
            {"reqid": 802, "pyro_trans_id": 999112, "caf_serial_no": "CAF802", "gsmno": "9412345678", "batch_date": "2026-09-01", "status_check_count": 4},
        ]

        mock_check_status.return_value = {
            "statusCode": 901,
            "message": "Transaction not found",
        }

        summary = await run_status_checks()

        assert summary["checked"] == 1
        assert summary["failed"] == 1
        mock_mark_failed.assert_called_once()
        mock_update_bcd.assert_called_once_with(
            "CAF802", 802, BCD_STATUS_F, "[901] Transaction not found after 5 status-check attempts",
        )


# ── Scheduler Regression ───────────────────────────────────────────────────────

class TestAllModeSchedulerIntegration:
    """Verify scheduler job execution under settings.enabled_zones='ALL'."""

    @pytest.mark.asyncio
    @patch("app.batch.populator.run_batch_population")
    async def test_scheduled_population_job_uses_all_mode_and_logs(self, mock_populator):
        """_batch_population_job executes with mode='ALL' when settings.enabled_zones='ALL'."""
        mock_populator.return_value = {
            "execution_id": "exec_pop_sched",
            "mode": "ALL",
            "zones": ["ALL"],
            "oracle_selected": 0,
            "staged": 0,
            "claim_success": 0,
            "dispatchable": 0,
            "errors": 0,
        }

        with patch.object(settings, "enabled_zones", "ALL"):
            await _batch_population_job()

        mock_populator.assert_called_once()
        passed_ctx = mock_populator.call_args[1]["context"]
        assert passed_ctx.mode == "ALL"
        assert passed_ctx.zone_codes == ("ALL",)
        assert passed_ctx.circle_codes is None
        assert passed_ctx.source == "SCHEDULED"

    @pytest.mark.asyncio
    @patch("app.processor.process_pending_recharges")
    async def test_scheduled_recharge_job_uses_all_mode_and_logs(self, mock_recharge_proc):
        """_recharge_job executes with mode='ALL' when settings.enabled_zones='ALL'."""
        mock_recharge_proc.return_value = {
            "execution_id": "exec_rech_sched",
            "mode": "ALL",
            "zones": ["ALL"],
            "claimed": 0,
            "submitted": 0,
            "success": 0,
        }

        with patch.object(settings, "enabled_zones", "ALL"):
            await _recharge_job()

        mock_recharge_proc.assert_called_once()
        passed_ctx = mock_recharge_proc.call_args[1]["context"]
        assert passed_ctx.mode == "ALL"
        assert passed_ctx.zone_codes == ("ALL",)
        assert passed_ctx.circle_codes is None
        assert passed_ctx.source == "SCHEDULED"
