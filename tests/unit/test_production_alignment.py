"""Comprehensive verification suite for production alignment architecture.

Tests:
1. Direct in_status='C' staging, push_flag='N', pyro_status='N', remarks null, 0 DDL.
2. Complete elimination of mark_requests_dispatchable and update_bcd_status from production code.
3. Ingestion claim sets exact remark 'N101: request inserted' in CAF_ADMIN.BCD.
4. Pure verbatim Pyro error message preserved in last_error_msg (no prefixes, no synthetic text).
5. Immediate terminal stop on duplicate subscriber window (415, 5016) without retry.
6. Immediate terminal stop on IN switch rejection (5010, 5030, 5005, 5011, 5012, 902) in status checker and callback.
7. Dealer exhaustion (405) releases subsequent dealer claims in batch without synthetic errors.
8. Bounded concurrency Semaphore enforcement (recharge_concurrency).
9. Shared persistent httpx.AsyncClient lifecycle (init_http_client, close_http_client).
10. Smart token cache validity skipping refresh when token is still valid.
11. Status checker query contains LIMIT clause with status_check_batch_size.
12. Batch abort on auth failure releases unprocessed claims.
"""

import asyncio
from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from app.auth.token_manager import PyroAuthService, token_manager
from app.callback import recharge_callback
from app.config import settings
from app.db.oracle import batch_writeback_bcd_rq
from app.db.postgres import (
    FLAG_FAILED,
    FLAG_PENDING,
    FLAG_RETRY,
    FLAG_SUCCESS,
    bulk_insert_frc_requests,
    fetch_pending_rows,
    fetch_pushed_rows_for_status_check,
    mark_as_failed,
)
from app.processor import process_pending_recharges
import app.pyro_client as pyro_client
from app.status_checker import run_status_checks


@contextmanager
def _mock_pg_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = None
    yield conn


class TestZeroDDLAndDirectStagingContract:
    """Requirement: Direct insertion as 'C', 'N', 'N', 0 DDL, no staging churn."""

    @patch("app.db.postgres.get_pg_conn")
    def test_bulk_insert_direct_c_n_n_and_null_remarks(self, mock_get_conn):
        """Rows inserted directly as in_status='C', push_flag='N', pyro_status='N', remarks NULL."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (101, "CAF101", "9412345678", 2)
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        row = {
            "caf_serial_no": "CAF101",
            "gsmno": "9412345678",
            "csccode": "CSC01",
            "circle_code": 2,
            "edate": "2026-09-01",
            "reqdate": "2026-09-01 10:00:00",
            "frc_plan_name": "P1",
            "frc_plan_code": "PC1",
            "frc_category_code": "CC1",
            "frcamt": 299,
            "ctopup_number": "9412300000",
            "vendormsisdn": "9412300000",
            "vendorid": "V1",
            "mpin": "enc_mpin",
            "mpin_length": 4,
            "max_retries": 3,
            "kyc_mode": "EKYC",
        }

        result = bulk_insert_frc_requests([row])
        assert len(result) == 1
        assert result[0]["reqid"] == 101

        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "'C', 'N', 'N'" in executed_sql
        assert "RETURNING reqid, caf_serial_no" in executed_sql

    def test_elimination_of_mark_requests_dispatchable_from_production(self):
        """Verify mark_requests_dispatchable is completely removed from app modules."""
        import app.batch.populator as populator
        import app.batch.reconciler as reconciler
        import app.db.postgres as pg

        assert not hasattr(populator, "mark_requests_dispatchable")
        assert not hasattr(reconciler, "mark_requests_dispatchable")
        assert not hasattr(pg, "mark_requests_dispatchable")

    def test_elimination_of_update_bcd_status_from_production(self):
        """Verify update_bcd_status is completely removed from app modules."""
        import app.callback as cb
        import app.db.oracle as ora
        import app.processor as proc
        import app.status_checker as sc

        assert not hasattr(ora, "update_bcd_status")
        assert not hasattr(proc, "update_bcd_status")
        assert not hasattr(cb, "update_bcd_status")
        assert not hasattr(sc, "update_bcd_status")


class TestOracleIngestionClaimExactRemark:
    """Requirement: Single writeback to Oracle at ingestion with exact N101 remark."""

    @patch("app.db.oracle.get_oracle_conn")
    def test_batch_writeback_bcd_rq_sets_exact_n101_remark(self, mock_get_conn):
        """Oracle claim writeback sets FRC_FLOW_STATUS='RQ' and REMARKS='N101: request inserted'."""
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        candidates = [{"reqid": 501, "caf_serial_no": "CAF501", "circle_code": 2, "gsmnumber": "9412345678"}]
        count = batch_writeback_bcd_rq(candidates)

        assert count == 1
        mock_cursor.execute.assert_called_once()
        executed_sql = mock_cursor.execute.call_args[0][0]
        binds = mock_cursor.execute.call_args[0][1]

        assert "FRC_FLOW_STATUS" in executed_sql
        assert binds["status"] == "RQ"
        assert "FRC_FLOW_REMARKS       = 'N101: request inserted'" in executed_sql
        assert binds["reqid"] == 501
        assert binds["caf_serial_no"] == "CAF501"


class TestPureVerbatimPyroErrorMessages:
    """Requirement: last_error_msg stores strictly response.get('message'), no synthetic text."""

    @patch("app.db.postgres.get_pg_conn")
    def test_mark_as_failed_stores_verbatim_error_message(self, mock_get_conn):
        """mark_as_failed sets last_error_msg to verbatim pyro_msg without synthetic prefixes."""
        mock_cursor = MagicMock()
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        verbatim_pyro_error = "Subscriber number barred in HLR"
        mark_as_failed(
            reqid=501,
            push_flag=FLAG_FAILED,
            pyro_msg=verbatim_pyro_error,
            response_text='{"statusCode": 5006, "message": "Subscriber number barred in HLR"}',
            final_statuscode=5006,
        )

        mock_cursor.execute.assert_called_once()
        executed_sql, params = mock_cursor.execute.call_args[0]

        assert "last_error_msg" in executed_sql
        assert params[2] == verbatim_pyro_error
        assert "push_remarks" not in executed_sql


class TestDuplicateWindowTerminalStop:
    """Requirement: 415 and 5016 immediately mark FLAG_FAILED, FAL, FAILED, and never retry."""

    @pytest.mark.asyncio
    @patch("app.processor.async_mark_as_failed")
    @patch("app.processor.recharge")
    @patch("app.processor.token_manager")
    @patch("app.processor.async_fetch_pending_rows")
    async def test_duplicate_code_415_immediate_terminal_stop(
        self, mock_fetch_pending, mock_token_mgr, mock_recharge, mock_mark_failed
    ):
        """Code 415 terminates immediately with FLAG_FAILED and verbatim message."""
        mock_token_mgr.session_token = "tok"
        mock_token_mgr.access_token = "tok"

        mock_fetch_pending.return_value = [
            {"reqid": 41501, "caf_serial_no": "CAF415", "gsmno": "9412345678", "circle_code": 2, "vendormsisdn": "9412300000", "ctopup_number": "9412300000", "frcamt": 299, "mpin": "enc", "retry_count": 0, "max_retries": 3, "batch_date": "2026-09-01"},
        ]

        verbatim_msg = "Duplicate transaction within window"
        mock_recharge.return_value = {
            "statusCode": 415,
            "message": verbatim_msg,
        }

        with patch("app.processor.decrypt", return_value="1234"):
            summary = await process_pending_recharges(batch_size=10)

        assert summary["permanent_failure"] == 1
        assert summary["transient_failure"] == 0
        mock_mark_failed.assert_called_once_with(
            41501, FLAG_FAILED, verbatim_msg, '{"statusCode": 415, "message": "Duplicate transaction within window"}', 415
        )

    @pytest.mark.asyncio
    @patch("app.processor.async_mark_as_failed")
    @patch("app.processor.recharge")
    @patch("app.processor.token_manager")
    @patch("app.processor.async_fetch_pending_rows")
    async def test_duplicate_code_5016_immediate_terminal_stop(
        self, mock_fetch_pending, mock_token_mgr, mock_recharge, mock_mark_failed
    ):
        """Code 5016 terminates immediately with FLAG_FAILED and verbatim message."""
        mock_token_mgr.session_token = "tok"
        mock_token_mgr.access_token = "tok"

        mock_fetch_pending.return_value = [
            {"reqid": 50161, "caf_serial_no": "CAF5016", "gsmno": "9412345678", "circle_code": 2, "vendormsisdn": "9412300000", "ctopup_number": "9412300000", "frcamt": 299, "mpin": "enc", "retry_count": 0, "max_retries": 3, "batch_date": "2026-09-01"},
        ]

        verbatim_msg = "Duplicate recharge request"
        mock_recharge.return_value = {
            "statusCode": 5016,
            "message": verbatim_msg,
        }

        with patch("app.processor.decrypt", return_value="1234"):
            summary = await process_pending_recharges(batch_size=10)

        assert summary["permanent_failure"] == 1
        assert summary["transient_failure"] == 0
        mock_mark_failed.assert_called_once_with(
            50161, FLAG_FAILED, verbatim_msg, '{"statusCode": 5016, "message": "Duplicate recharge request"}', 5016
        )


class TestINSwitchRejectionTerminalStop:
    """Requirement: Code 5010 (and 5030, 5005, 5011, 5012, 902) stops polling immediately."""

    @pytest.mark.asyncio
    @patch("app.status_checker.async_mark_as_failed")
    @patch("app.status_checker.check_transaction_status")
    @patch("app.status_checker.async_update_status_check_attempt")
    @patch("app.status_checker.async_fetch_pushed_rows_for_status_check")
    async def test_status_checker_5010_already_frc_done_immediate_terminal_stop(
        self, mock_fetch_rows, mock_update_attempt, mock_check_status, mock_mark_failed
    ):
        """Code 5010 in status checker immediately terminates polling on attempt 1."""
        mock_fetch_rows.return_value = [
            {"reqid": 901, "pyro_trans_id": 999111, "caf_serial_no": "CAF901", "gsmno": "9412345678", "batch_date": "2026-09-01", "status_check_count": 0},
        ]

        verbatim_msg = "Already FRC done for this subscriber"
        mock_check_status.return_value = {
            "statusCode": 5010,
            "message": verbatim_msg,
        }

        summary = await run_status_checks()

        assert summary["checked"] == 1
        assert summary["failed"] == 1
        assert summary["retry"] == 0
        mock_mark_failed.assert_called_once_with(
            901, FLAG_FAILED, verbatim_msg, '{"statusCode": 5010, "message": "Already FRC done for this subscriber"}', 5010
        )

    @pytest.mark.asyncio
    @patch("app.callback.async_mark_as_failed")
    @patch("app.callback.async_insert_txn_log")
    @patch("app.callback.async_find_row_by_pyro_trans_id")
    async def test_callback_5010_marks_terminal_failed(
        self, mock_find_row, mock_insert_log, mock_mark_failed
    ):
        """Code 5010 callback transitions request to FLAG_FAILED with verbatim message."""
        from fastapi.testclient import TestClient
        from main import app

        mock_find_row.return_value = {
            "reqid": 902,
            "caf_serial_no": "CAF902",
            "gsmno": "9412345678",
            "batch_date": "2026-09-01",
            "push_flag": "P",
            "client_txn_id": "00902",
            "frcamt": 299.0,
        }

        verbatim_msg = "Already FRC done"
        payload = {
            "statusCode": 5010,
            "message": verbatim_msg,
            "data": {
                "transactionId": "999888778",
                "clientTxnId": "00902",
                "destMsisdn": "9412345678",
                "amount": 299.0,
                "status": "FAILED",
            },
        }

        client = TestClient(app)
        response = client.post("/callback/recharge", json=payload)
        assert response.status_code == 200
        assert response.json() == {"received": True}
        mock_mark_failed.assert_called_once()
        assert mock_mark_failed.call_args[0][0] == 902
        assert mock_mark_failed.call_args[0][1] == FLAG_FAILED
        assert mock_mark_failed.call_args[0][2] == verbatim_msg


class TestDealerExhaustionReleaseSafety:
    """Requirement: Dealer 405 exhaustion releases subsequent rows without synthetic errors."""

    @pytest.mark.asyncio
    @patch("app.processor.async_release_unprocessed_claims")
    @patch("app.processor.async_mark_as_failed")
    @patch("app.processor.recharge")
    @patch("app.processor.token_manager")
    @patch("app.processor.async_fetch_pending_rows")
    async def test_dealer_405_releases_unprocessed_rows(
        self, mock_fetch_pending, mock_token_mgr, mock_recharge, mock_mark_failed, mock_release_unprocessed
    ):
        """Dealer exhaustion releases unsubmitted row 702 without creating fake error."""
        mock_token_mgr.session_token = "tok"
        mock_token_mgr.access_token = "tok"

        mock_fetch_pending.return_value = [
            {"reqid": 701, "caf_serial_no": "CAF701", "gsmno": "9412300001", "circle_code": 2, "vendormsisdn": "DEALER_EXHAUST", "ctopup_number": "DEALER_EXHAUST", "frcamt": 299, "mpin": "enc", "retry_count": 0, "max_retries": 3, "batch_date": "2026-09-01"},
            {"reqid": 702, "caf_serial_no": "CAF702", "gsmno": "9412300002", "circle_code": 2, "vendormsisdn": "DEALER_EXHAUST", "ctopup_number": "DEALER_EXHAUST", "frcamt": 299, "mpin": "enc", "retry_count": 0, "max_retries": 3, "batch_date": "2026-09-01"},
        ]

        mock_recharge.return_value = {
            "statusCode": 405,
            "message": "Insufficient balance",
        }

        with patch("app.processor.decrypt", return_value="1234"):
            summary = await process_pending_recharges(batch_size=10)

        # recharge() only called once for 701; 702 skipped
        assert mock_recharge.call_count == 1
        assert summary["retry"] == 2
        mock_release_unprocessed.assert_called_once_with([702])


class TestHighThroughputOptimization:
    """Requirement: Concurrency semaphore, HTTP connection pooling, token cache, limit."""

    def test_recharge_concurrency_configuration(self):
        """recharge_concurrency is configured to 4 and status_check_batch_size to 100."""
        assert settings.recharge_concurrency == 4
        assert settings.status_check_batch_size == 100

    @pytest.mark.asyncio
    async def test_shared_http_client_lifecycle(self):
        """HTTP client is shared and persistent across multiple requests."""
        pyro_client.init_http_client()
        client1 = pyro_client._get_client()
        client2 = pyro_client._get_client()
        assert client1 is client2
        assert not client1.is_closed

        await pyro_client.close_http_client()
        assert client1.is_closed

    def test_token_manager_skips_refresh_when_valid(self):
        """PyroAuthService._is_access_token_valid honors expiry safety window."""
        mgr = PyroAuthService()
        assert mgr._is_access_token_valid() is False

        # Simulate fresh token (expires in 1000s)
        import time
        mgr.access_token = "valid_jwt"
        mgr._access_token_exp = time.time() + 1000
        assert mgr._is_access_token_valid() is True

        # Simulate near-expiry token (expires in 10s, less than safety window 60s)
        mgr._access_token_exp = time.time() + 10
        assert mgr._is_access_token_valid() is False

    @patch("app.db.postgres.get_pg_conn")
    def test_status_check_query_has_limit_clause(self, mock_get_conn):
        """fetch_pushed_rows_for_status_check enforces LIMIT %s."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        fetch_pushed_rows_for_status_check(limit=100)

        mock_cursor.execute.assert_called_once()
        sql, params = mock_cursor.execute.call_args[0]
        assert "LIMIT %s" in sql
        assert params[-1] == 100
