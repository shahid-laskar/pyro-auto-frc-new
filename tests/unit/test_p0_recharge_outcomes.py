"""P0: recharge outcomes that may already have been executed must never end as a retry or FAILED.

- Synchronous 2000 + SUCCESS (spec §9) is SUCCESS, not a transient retry.
- 5018 (duplicate clientTxnId), 5016 on a retry, and -1 after the request was sent
  are parked for reconciliation (push_flag='F', final_status NULL, 'RECONCILE:' remark).
- -1 when the request was never sent (connect failure, no action token) is still retried.
- mark_for_reconciliation never overwrites a SUCCESS row.
- recharge() reports connection failures as not sent.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app.db.postgres import RECONCILE_REMARK_PREFIX, mark_for_reconciliation
from app.processor import process_pending_recharges


def _row(retry_count=0):
    return {
        "reqid": 501, "caf_serial_no": "CAF501", "gsmno": "9489902775",
        "vendormsisdn": "8903696189", "ctopup_number": "8903696189",
        "frcamt": 153, "mpin": "enc", "retry_count": retry_count, "max_retries": 3,
        "batch_date": "2026-11-21",
    }


async def _dispatch(response, retry_count=0):
    with patch("app.processor.async_fetch_pending_rows", new_callable=AsyncMock,
               return_value=[_row(retry_count)]), \
         patch("app.processor.token_manager") as tm, \
         patch("app.processor.decrypt", return_value="1234"), \
         patch("app.processor.recharge", new_callable=AsyncMock, return_value=response), \
         patch("app.processor.async_mark_as_pushed", new_callable=AsyncMock) as pushed, \
         patch("app.processor.async_mark_as_success", new_callable=AsyncMock) as success, \
         patch("app.processor.async_mark_as_failed", new_callable=AsyncMock) as failed, \
         patch("app.processor.async_mark_for_reconciliation", new_callable=AsyncMock) as reconcile, \
         patch("app.processor.async_release_unprocessed_claims", new_callable=AsyncMock):
        tm.session_token = "s"
        tm.access_token = "a"
        tm.authenticate = AsyncMock(return_value=True)
        summary = await process_pending_recharges(batch_size=1)
    return summary, {"pushed": pushed, "success": success, "failed": failed, "reconcile": reconcile}


@pytest.mark.asyncio
class TestRechargeOutcomeClassification:

    async def test_sync_2000_success_marks_success(self):
        resp = {"statusCode": 2000, "status": "SUCCESS", "message": "Recharge successful",
                "data": {"transactionId": "43385", "status": "SUCCESS",
                         "dealerBalanceBefore": 446.28, "dealerBalanceAfter": 293.28},
                "_request_sent": True}
        summary, m = await _dispatch(resp)
        m["success"].assert_awaited_once()
        args = m["success"].call_args.args
        assert args[0] == 501 and args[2] == 446.28 and args[3] == 293.28
        assert args[4] == 2000 and args[5] == 43385
        m["failed"].assert_not_called()
        m["reconcile"].assert_not_called()
        assert summary["success"] == 1

    async def test_sync_2000_without_success_status_is_parked(self):
        resp = {"statusCode": 2000, "status": "PENDING", "message": "?", "data": {}}
        summary, m = await _dispatch(resp)
        m["reconcile"].assert_awaited_once()
        m["success"].assert_not_called()
        m["failed"].assert_not_called()
        assert summary["reconciliation_required"] == 1

    @pytest.mark.parametrize("retry_count", [0, 2])
    async def test_5018_duplicate_client_txn_is_parked(self, retry_count):
        resp = {"statusCode": 5018, "status": "FAILED", "message": "Duplicate clientTxnId"}
        summary, m = await _dispatch(resp, retry_count)
        m["reconcile"].assert_awaited_once()
        m["failed"].assert_not_called()
        assert m["reconcile"].call_args.args[3] == 5018
        assert summary["reconciliation_required"] == 1

    async def test_5016_on_retry_is_parked(self):
        resp = {"statusCode": 5016, "status": "FAILED", "message": "same subscriber within 10 minutes"}
        summary, m = await _dispatch(resp, retry_count=1)
        m["reconcile"].assert_awaited_once()
        m["failed"].assert_not_called()

    async def test_5016_on_first_attempt_is_still_terminal_failure(self):
        resp = {"statusCode": 5016, "status": "FAILED", "message": "same subscriber within 10 minutes"}
        summary, m = await _dispatch(resp, retry_count=0)
        m["reconcile"].assert_not_called()
        assert m["failed"].call_args.args[1] == "F"

    @pytest.mark.parametrize("message", ["Request timed out", "Response parse failed: x", "RemoteProtocolError"])
    async def test_minus_one_after_send_is_parked(self, message):
        resp = {"statusCode": -1, "status": "ERROR", "message": message, "_request_sent": True}
        summary, m = await _dispatch(resp)
        m["reconcile"].assert_awaited_once()
        m["failed"].assert_not_called()

    async def test_minus_one_legacy_response_without_flag_is_parked(self):
        resp = {"statusCode": -1, "status": "ERROR", "message": "Request timed out"}
        _, m = await _dispatch(resp)
        m["reconcile"].assert_awaited_once()

    async def test_connection_failure_not_sent_is_retried(self):
        resp = {"statusCode": -1, "status": "ERROR", "message": "Connection failed: refused",
                "_request_sent": False}
        summary, m = await _dispatch(resp)
        m["reconcile"].assert_not_called()
        assert m["failed"].call_args.args[1] == "E"
        assert summary["retry"] == 1

    async def test_action_token_failure_is_retried(self):
        resp = {"statusCode": -1, "status": "ERROR", "message": "Failed to generate action token",
                "_request_sent": False}
        _, m = await _dispatch(resp)
        m["reconcile"].assert_not_called()
        assert m["failed"].call_args.args[1] == "E"

    async def test_2002_registered_unchanged(self):
        resp = {"statusCode": 2002, "status": "IN_PROCESS", "data": {"transactionId": 43383}}
        _, m = await _dispatch(resp)
        m["pushed"].assert_awaited_once()
        m["reconcile"].assert_not_called()


class TestMarkForReconciliationSql:

    def _run(self, **kwargs):
        cur = MagicMock()
        cur.rowcount = 1
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        with patch("app.db.postgres.get_pg_write_conn") as get_conn:
            get_conn.return_value.__enter__.return_value = conn
            mark_for_reconciliation(501, "outcome unknown after send: Request timed out", **kwargs)
        return cur.execute.call_args.args

    def test_parks_as_f_with_null_final_status_and_marker(self):
        sql, params = self._run(status_code=-1)
        flat = " ".join(sql.split())
        assert "push_flag = 'F'" in flat
        assert "final_status = NULL" in flat
        assert "AND push_flag <> 'Y'" in flat        # never overwrite a success
        assert params[1].startswith(RECONCILE_REMARK_PREFIX)
        assert len(params[1]) <= 200
        assert params[-1] == 501

    def test_keeps_existing_pyro_trans_id(self):
        sql, params = self._run(pyro_trans_id=43385)
        assert "pyro_trans_id           = COALESCE(pyro_trans_id, %s)" in sql
        assert 43385 in params


def _tm():
    tm = MagicMock()
    tm.get_action_token = AsyncMock(return_value="act")
    tm.session_token = "s"
    tm.access_token = "a"
    return tm


async def _recharge(client):
    from app.pyro_client import recharge
    with patch("app.pyro_client.token_manager", _tm()), \
         patch("app.pyro_client._get_client", return_value=client), \
         patch("app.pyro_client.async_insert_txn_log", new_callable=AsyncMock) as log:
        result = await recharge(
            reqid=501, caf_serial_no="CAF501", gsmno="9489902775", batch_date="2026-11-21",
            dealer_msisdn="8903696189", dest_msisdn="9489902775", amount=153, mpin="1234",
        )
    return result, log


@pytest.mark.asyncio
class TestRechargeTransportMetadata:

    @pytest.mark.parametrize("exc", [
        httpx.ConnectError("refused"), httpx.ConnectTimeout("slow"), httpx.PoolTimeout("pool"),
    ])
    async def test_connect_failures_are_not_sent(self, exc):
        client = AsyncMock()
        client.post.side_effect = exc
        result, log = await _recharge(client)
        assert result["_request_sent"] is False and result["statusCode"] == -1
        assert log.call_args.kwargs["error_class"] == "RequestNotSent"

    async def test_read_timeout_is_sent(self):
        client = AsyncMock()
        client.post.side_effect = httpx.ReadTimeout("slow")
        result, _ = await _recharge(client)
        assert result["_request_sent"] is True and result["message"] == "Request timed out"
