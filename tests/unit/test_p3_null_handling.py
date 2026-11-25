"""P3 falsy-vs-null and robustness fixes (FRC).

#14 Missing dealer balances are stored as NULL, not 0.00.
#15 Pyro error responses with "data": null are parsed, not turned into -1
    (which would now park the row as 'outcome unknown').
#16 transactionId 0 is a value, not "missing".
#17 400 / 5010 / 5017 are permanent: no pointless retries.
    Rows without a pyro_trans_id are not polled (they are parked by the stale sweep).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.callback import router as callback_router
from app.config import settings
from app.db import postgres as pg
from app.processor import process_pending_recharges
from app.status_checker import run_status_checks


# ── #15 ──────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_recharge_error_with_null_data_keeps_its_status_code():
    from app.auth.token_manager import ActionTokens
    from app.pyro_client import recharge
    resp = MagicMock()
    resp.text = '{"statusCode":5012,"status":"FAILED","message":"Sorry You have Entered Invalid Mpin","data":null}'
    resp.json.return_value = {"statusCode": 5012, "status": "FAILED",
                              "message": "Sorry You have Entered Invalid Mpin", "data": None}
    resp.status_code = 200
    client = AsyncMock()
    client.post.return_value = resp
    tm = MagicMock()
    tm.get_action_token = AsyncMock(return_value=ActionTokens("s", "a", "act"))
    with patch("app.pyro_client.token_manager", tm), \
         patch("app.pyro_client._get_client", return_value=client), \
         patch("app.pyro_client.async_insert_txn_log", new_callable=AsyncMock):
        result = await recharge(reqid=501, caf_serial_no="C", gsmno="9489902775",
                                batch_date="2026-11-21", dealer_msisdn="8903696189",
                                dest_msisdn="9489902775", amount=153, mpin="1234")
    assert result["statusCode"] == 5012


# ── #14 / #16 callback ───────────────────────────────────────────────────────

@pytest.fixture
def cb():
    app = FastAPI()
    app.include_router(callback_router)
    row = {"reqid": 501, "caf_serial_no": "C", "gsmno": "9489902775", "batch_date": "2026-11-21",
           "push_flag": "P", "final_status": None, "push_remarks": None,
           "pyro_trans_id": None, "client_txn_id": "00501", "frcamt": 153}
    with patch("app.callback.async_find_row_by_pyro_trans_id", new_callable=AsyncMock, return_value=row) as by_txn, \
         patch("app.callback.async_find_row_by_client_txn_id", new_callable=AsyncMock, return_value=None), \
         patch("app.callback.async_insert_txn_log", new_callable=AsyncMock), \
         patch("app.callback.async_mark_as_success", new_callable=AsyncMock, return_value=1) as success, \
         patch("app.callback.async_mark_as_failed", new_callable=AsyncMock), \
         patch.object(settings, "callback_allowed_ips", ""):
        yield TestClient(app), by_txn, success


def _cb_body(**data):
    base = {"clientTxnId": "00501", "transactionId": 43385, "destMsisdn": "9489902775",
            "amount": 153, "status": "SUCCESS"}
    base.update(data)
    return {"statusCode": 2000, "status": "SUCCESS", "message": "ok", "data": base}


def test_callback_missing_balances_are_null(cb):
    client, _, success = cb
    client.post("/callback/recharge", json=_cb_body())
    args = success.call_args.args
    assert args[2] is None and args[3] is None


def test_callback_zero_balance_is_kept(cb):
    client, _, success = cb
    client.post("/callback/recharge", json=_cb_body(dealerBalanceBefore=153.0, dealerBalanceAfter=0.0))
    args = success.call_args.args
    assert args[2] == 153.0 and args[3] == 0.0


def test_callback_transaction_id_zero_is_not_missing(cb):
    client, by_txn, _ = cb
    r = client.post("/callback/recharge", json=_cb_body(transactionId=0))
    assert r.json().get("note") != "missing transactionId"
    by_txn.assert_awaited_once_with(0)


# ── #14 status checker ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_status_check_missing_balances_are_null():
    data = {"clientTxnId": "00501", "transactionId": "43385", "destMsisdn": "9489902775",
            "amount": 153.0, "status": "SUCCESS"}
    row = {"reqid": 501, "pyro_trans_id": 43385, "caf_serial_no": "C", "gsmno": "9489902775",
           "batch_date": "2026-11-21", "status_check_count": 0, "client_txn_id": "00501", "frcamt": 153}
    with patch("app.status_checker.async_release_orphaned_claims", new_callable=AsyncMock, return_value=[]), \
         patch("app.status_checker.async_park_stale_pushed_rows", new_callable=AsyncMock, return_value=[]), \
         patch("app.status_checker.async_fetch_pushed_rows_for_status_check", new_callable=AsyncMock, return_value=[row]), \
         patch("app.status_checker.check_transaction_status", new_callable=AsyncMock,
               return_value={"statusCode": 2000, "status": "SUCCESS", "data": data}), \
         patch("app.status_checker.async_update_status_check_attempt", new_callable=AsyncMock), \
         patch("app.status_checker.async_mark_as_success", new_callable=AsyncMock, return_value=1) as success:
        await run_status_checks()
    args = success.call_args.args
    assert args[2] is None and args[3] is None


def test_unpollable_rows_are_not_fetched():
    cur = MagicMock()
    cur.fetchall.return_value = []
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    with patch("app.db.postgres.get_pg_write_conn") as get_conn:
        get_conn.return_value.__enter__.return_value = conn
        pg.fetch_pushed_rows_for_status_check(10)
    assert "pyro_trans_id IS NOT NULL" in cur.execute.call_args.args[0]


# ── #17 ──────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.parametrize("code,msg", [
    (400, "Bad Request: amount must be positive"),
    (5010, "Recharge failed. Only First Recharge (FRC) accounts or plans are allowed."),
    (5017, "Invalid clientTxnId"),
])
async def test_permanent_codes_are_not_retried(code, msg):
    row = {"reqid": 1, "caf_serial_no": "C", "gsmno": "9400000000", "vendormsisdn": "8903696189",
           "ctopup_number": "8903696189", "frcamt": 153, "mpin": "m", "retry_count": 0,
           "max_retries": 3, "batch_date": "2026-11-21"}
    with patch("app.processor.async_fetch_pending_rows", new_callable=AsyncMock, return_value=[row]), \
         patch("app.processor.token_manager") as tm, \
         patch("app.processor.decrypt", return_value="1234"), \
         patch("app.processor.recharge", new_callable=AsyncMock,
               return_value={"statusCode": code, "status": "FAILED", "message": msg}), \
         patch("app.processor.async_mark_as_failed", new_callable=AsyncMock) as failed, \
         patch("app.processor.async_release_unprocessed_claims", new_callable=AsyncMock):
        tm.session_token = "s"
        tm.access_token = "a"
        summary = await process_pending_recharges(batch_size=1)
    assert failed.call_args.args[1] == "F"
    assert summary["permanent_failure"] == 1
