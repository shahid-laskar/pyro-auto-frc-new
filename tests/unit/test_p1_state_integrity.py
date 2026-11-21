"""P1 state-integrity tests.

#4 Guarded state transitions (no callback/poller overwrite).
#5 Poll exhaustion parks instead of failing; transport errors don't burn attempts;
   stale pushes are swept; late SUCCESS resolves parked rows; clientTxnId fallback.
#6 Strict callback validation; source-IP allow-list; malformed bodies rejected.
#7 One bad row can't crash the batch or leave orphan tasks.
"""

import re
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.callback import router as callback_router
from app.config import settings
from app.db import postgres as pg
from app.processor import process_pending_recharges
from app.status_checker import run_status_checks


# ══════════════════════════════════════════════════════════════════════════════
# #6 / #5  Callback
# ══════════════════════════════════════════════════════════════════════════════

def _db_row(**overrides):
    row = {
        "reqid": 501, "caf_serial_no": "CAF501", "gsmno": "9489902775",
        "batch_date": "2026-11-21", "push_flag": "P", "final_status": None,
        "push_remarks": "Submitted to Pyro - awaiting callback",
        "pyro_trans_id": 43385, "client_txn_id": "00501", "frcamt": 153,
    }
    row.update(overrides)
    return row


def _body(status_code=2000, status="SUCCESS", **data_overrides):
    data = {
        "clientTxnId": "00501", "transactionId": 43385, "destMsisdn": "9489902775",
        "amount": 153.0, "status": status,
        "dealerBalanceBefore": 446.28, "dealerBalanceAfter": 293.28,
    }
    data.update(data_overrides)
    return {"statusCode": status_code, "status": status, "message": "Recharge successful", "data": data}


@pytest.fixture
def cb():
    app = FastAPI()
    app.include_router(callback_router)
    with patch("app.callback.async_find_row_by_pyro_trans_id", new_callable=AsyncMock) as by_txn, \
         patch("app.callback.async_find_row_by_client_txn_id", new_callable=AsyncMock) as by_client, \
         patch("app.callback.async_insert_txn_log", new_callable=AsyncMock) as txn_log, \
         patch("app.callback.async_mark_as_success", new_callable=AsyncMock, return_value=1) as success, \
         patch("app.callback.async_mark_as_failed", new_callable=AsyncMock, return_value=1) as failed, \
         patch.object(settings, "callback_allowed_ips", ""):
        by_txn.return_value = _db_row()
        by_client.return_value = None
        yield {
            "client": TestClient(app), "by_txn": by_txn, "by_client": by_client,
            "log": txn_log, "success": success, "failed": failed,
        }


class TestCallbackValidation:

    def test_valid_success(self, cb):
        r = cb["client"].post("/callback/recharge", json=_body())
        assert r.json() == {"received": True}
        cb["success"].assert_awaited_once()
        assert cb["success"].call_args.args[5] == 43385

    @pytest.mark.parametrize("missing", ["clientTxnId", "destMsisdn", "amount"])
    def test_missing_field_is_rejected(self, cb, missing):
        body = _body()
        del body["data"][missing]
        r = cb["client"].post("/callback/recharge", json=body)
        assert r.json()["note"] == "callback validation failed"
        cb["success"].assert_not_called()

    @pytest.mark.parametrize("field,value", [
        ("clientTxnId", "00502"), ("destMsisdn", "9400000000"), ("amount", 199),
    ])
    def test_mismatch_is_rejected(self, cb, field, value):
        r = cb["client"].post("/callback/recharge", json=_body(**{field: value}))
        assert r.json()["note"] == "callback validation failed"
        cb["success"].assert_not_called()
        cb["failed"].assert_not_called()

    def test_client_txn_id_derived_from_reqid_when_column_null(self, cb):
        cb["by_txn"].return_value = _db_row(client_txn_id=None)
        cb["client"].post("/callback/recharge", json=_body())
        cb["success"].assert_awaited_once()

    def test_transaction_id_mismatch_on_fallback_row_is_rejected(self, cb):
        cb["by_txn"].return_value = None
        cb["by_client"].return_value = _db_row(pyro_trans_id=99999)
        r = cb["client"].post("/callback/recharge", json=_body())
        assert r.json()["note"] == "callback validation failed"
        cb["success"].assert_not_called()

    def test_string_status_code_is_normalised(self, cb):
        body = _body()
        body["statusCode"] = "2000"
        cb["client"].post("/callback/recharge", json=body)
        cb["success"].assert_awaited_once()

    @pytest.mark.parametrize("payload", [[1, 2], {"statusCode": 2000, "data": "x"}])
    def test_malformed_body_is_400(self, cb, payload):
        r = cb["client"].post("/callback/recharge", json=payload)
        assert r.status_code == 400

    def test_null_data_does_not_crash(self, cb):
        r = cb["client"].post("/callback/recharge", json={"statusCode": 2000, "data": None})
        assert r.status_code == 200 and r.json()["note"] == "missing transactionId"


class TestCallbackLookupAndTerminalRules:

    def test_falls_back_to_client_txn_id_and_backfills(self, cb):
        cb["by_txn"].return_value = None
        cb["by_client"].return_value = _db_row(pyro_trans_id=None, push_flag="F",
                                               push_remarks="RECONCILE: outcome unknown")
        cb["client"].post("/callback/recharge", json=_body())
        cb["by_client"].assert_awaited_once_with("00501")
        cb["success"].assert_awaited_once()
        assert cb["success"].call_args.args[5] == 43385

    def test_not_found_anywhere(self, cb):
        cb["by_txn"].return_value = None
        r = cb["client"].post("/callback/recharge", json=_body())
        assert r.json()["note"] == "transaction not found"

    def test_success_row_is_final(self, cb):
        cb["by_txn"].return_value = _db_row(push_flag="Y", final_status="SUCCESS")
        r = cb["client"].post("/callback/recharge", json=_body(5005, "FAILED"))
        assert r.json()["note"] == "already processed"
        cb["failed"].assert_not_called()

    def test_parked_row_resolved_by_late_success(self, cb):
        cb["by_txn"].return_value = _db_row(push_flag="F", final_status=None,
                                            push_remarks="RECONCILE: status check gave no final result")
        cb["client"].post("/callback/recharge", json=_body())
        cb["success"].assert_awaited_once()

    def test_parked_row_resolved_by_late_failure(self, cb):
        cb["by_txn"].return_value = _db_row(push_flag="F", final_status=None)
        cb["client"].post("/callback/recharge", json=_body(5005, "FAILED"))
        cb["failed"].assert_awaited_once()

    def test_failed_row_ignores_repeat_failure(self, cb):
        cb["by_txn"].return_value = _db_row(push_flag="F", final_status="FAILED")
        r = cb["client"].post("/callback/recharge", json=_body(5005, "FAILED"))
        assert r.json()["note"] == "already processed"
        cb["failed"].assert_not_called()

    def test_failed_row_upgraded_by_success(self, cb):
        cb["by_txn"].return_value = _db_row(push_flag="F", final_status="FAILED")
        cb["client"].post("/callback/recharge", json=_body())
        cb["success"].assert_awaited_once()


class TestCallbackAllowList:

    def test_unlisted_source_is_forbidden(self, cb):
        with patch.object(settings, "callback_allowed_ips", "10.44.24.20"):
            r = cb["client"].post("/callback/recharge", json=_body())
        assert r.status_code == 403
        cb["by_txn"].assert_not_called()

    def test_listed_source_via_trusted_proxy(self, cb):
        with patch.object(settings, "callback_allowed_ips", "10.44.24.20"), \
             patch.object(settings, "callback_trusted_proxies", "testclient"):
            r = cb["client"].post("/callback/recharge", json=_body(),
                                  headers={"X-Real-IP": "10.44.24.20"})
        assert r.status_code == 200
        cb["success"].assert_awaited_once()

    def test_spoofed_header_from_untrusted_peer_is_ignored(self, cb):
        with patch.object(settings, "callback_allowed_ips", "10.44.24.20"), \
             patch.object(settings, "callback_trusted_proxies", "127.0.0.1"):
            r = cb["client"].post("/callback/recharge", json=_body(),
                                  headers={"X-Real-IP": "10.44.24.20"})
        assert r.status_code == 403


# ══════════════════════════════════════════════════════════════════════════════
# #5  Status checker
# ══════════════════════════════════════════════════════════════════════════════

def _status_success(**overrides):
    data = {"clientTxnId": "00501", "transactionId": "43385", "destMsisdn": "9489902775",
            "amount": 153.0, "status": "SUCCESS",
            "dealerBalanceBefore": 446.28, "dealerBalanceAfter": 293.28}
    data.update(overrides)
    return {"statusCode": 2000, "status": "SUCCESS", "message": "Recharge successful", "data": data}


@pytest.fixture
def sc():
    with patch("app.status_checker.async_release_orphaned_claims", new_callable=AsyncMock, return_value=[]) as orphans, \
         patch("app.status_checker.async_park_stale_pushed_rows", new_callable=AsyncMock, return_value=[]) as stale, \
         patch("app.status_checker.async_fetch_pushed_rows_for_status_check", new_callable=AsyncMock) as fetch, \
         patch("app.status_checker.check_transaction_status", new_callable=AsyncMock) as check, \
         patch("app.status_checker.async_update_status_check_attempt", new_callable=AsyncMock) as attempt, \
         patch("app.status_checker.async_mark_as_success", new_callable=AsyncMock, return_value=1) as success, \
         patch("app.status_checker.async_mark_as_failed", new_callable=AsyncMock, return_value=1) as failed, \
         patch("app.status_checker.async_mark_for_reconciliation", new_callable=AsyncMock, return_value=1) as park:
        fetch.return_value = [{"reqid": 501, "pyro_trans_id": 43385, "caf_serial_no": "CAF501",
                               "gsmno": "9489902775", "batch_date": "2026-11-21",
                               "status_check_count": 4, "client_txn_id": "00501", "frcamt": 153}]
        yield {"stale": stale, "orphans": orphans, "fetch": fetch, "check": check,
               "attempt": attempt, "success": success, "failed": failed, "park": park}


@pytest.mark.asyncio
class TestStatusChecker:

    async def test_transport_error_does_not_count_or_finalise(self, sc):
        sc["check"].return_value = {"statusCode": -1, "message": "ConnectError"}
        summary = await run_status_checks()
        sc["attempt"].assert_not_called()
        sc["failed"].assert_not_called()
        sc["park"].assert_not_called()
        assert summary["transport_error"] == 1

    @pytest.mark.parametrize("code", [2002, 901, 777])
    async def test_exhaustion_parks_instead_of_failing(self, sc, code):
        sc["check"].return_value = {"statusCode": code, "message": "x"}
        summary = await run_status_checks()
        sc["park"].assert_awaited_once()
        sc["failed"].assert_not_called()
        assert summary["parked"] == 1

    async def test_terminal_code_still_fails(self, sc):
        sc["check"].return_value = {"statusCode": 5005, "message": "IN failure"}
        await run_status_checks()
        sc["failed"].assert_awaited_once()

    async def test_stale_pushes_are_swept_first(self, sc):
        sc["stale"].return_value = [11, 12]
        sc["fetch"].return_value = []
        summary = await run_status_checks()
        sc["stale"].assert_awaited_once_with(60)
        assert summary["parked"] == 2

    async def test_success_on_already_final_row_is_harmless(self, sc):
        sc["check"].return_value = _status_success()
        sc["success"].return_value = 0
        summary = await run_status_checks()
        assert summary["success"] == 1


# ══════════════════════════════════════════════════════════════════════════════
# #4  Guarded SQL
# ══════════════════════════════════════════════════════════════════════════════

def _capture(fn, *args, rowcount=1, fetchone=None, fetchall=None):
    cur = MagicMock()
    cur.rowcount = rowcount
    cur.fetchone.return_value = fetchone
    cur.fetchall.return_value = fetchall or []
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    with patch("app.db.postgres.get_pg_write_conn") as get_conn:
        get_conn.return_value.__enter__.return_value = conn
        result = fn(*args)
    sql, params = cur.execute.call_args.args
    return " ".join(sql.split()), params, result


class TestGuardedTransitions:

    def test_success_never_rewrites_success(self):
        sql, params, rc = _capture(pg.mark_as_success, 501, "{}", 1.0, 2.0, 2000, 43385, rowcount=0)
        assert "AND push_flag <> 'Y'" in sql
        assert rc == 0
        assert sql.count("%s") == len(params)

    @pytest.mark.parametrize("flag", ["E", "F"])
    def test_failed_guard_and_placeholders(self, flag):
        sql, params, _ = _capture(pg.mark_as_failed, 501, flag, "msg", "{}", 5005)
        assert "push_flag IN ('N', 'P', 'E')" in sql
        assert "push_flag = 'F' AND final_status IS NULL" in sql
        assert sql.count("%s") == len(params)
        assert params[-1] is (flag == "F")   # only a final failure may resolve a parked row

    def test_pushed_only_from_claimed(self):
        sql, params, _ = _capture(pg.mark_as_pushed, 501, 43385, "{}", "msg", 2002)
        assert "AND push_flag = 'P'" in sql
        assert sql.count("%s") == len(params)

    def test_reconciliation_never_rewrites_final(self):
        sql, params, _ = _capture(pg.mark_for_reconciliation, 501, "reason")
        assert "AND push_flag <> 'Y'" in sql
        assert "NOT (push_flag = 'F' AND final_status IS NOT NULL)" in sql
        assert sql.count("%s") == len(params)

    def test_stale_sweep_targets_only_pushed_rows(self):
        sql, params, parked = _capture(pg.park_stale_pushed_rows, 60, fetchall=[(7,), (8,)])
        assert "WHERE push_flag = 'P' AND push_date IS NOT NULL" in sql
        assert "final_status = NULL" in sql
        assert params[0].startswith(pg.RECONCILE_REMARK_PREFIX) and params[1] == 60
        assert parked == [7, 8]

    def test_callback_lookup_selects_validation_columns(self):
        sql, _, _ = _capture(pg.find_row_by_pyro_trans_id, 43385)
        for col in ("client_txn_id", "frcamt", "final_status", "pyro_trans_id"):
            assert re.search(rf"\b{col}\b", sql)


class TestClientTxnIdLookup:

    def test_exact_identity(self):
        _, params, row = _capture(pg.find_row_by_client_txn_id, "00501", fetchone={"reqid": 501})
        assert params == (501, "00501") and row == {"reqid": 501}

    def test_non_canonical_form_does_not_match(self):
        _, _, row = _capture(pg.find_row_by_client_txn_id, "501", fetchone={"reqid": 501})
        assert row is None

    @pytest.mark.parametrize("value", ["TEST74420761", "", None, "12a"])
    def test_foreign_ids_are_not_looked_up(self, value):
        assert pg.find_row_by_client_txn_id(value) is None


# ══════════════════════════════════════════════════════════════════════════════
# #7  Processor isolation
# ══════════════════════════════════════════════════════════════════════════════

def _rows():
    base = {"caf_serial_no": "C", "gsmno": "9400000000", "vendormsisdn": "8903696189",
            "ctopup_number": "8903696189", "frcamt": 153, "retry_count": 0,
            "max_retries": 3, "batch_date": "2026-11-21"}
    return [dict(base, reqid=1, mpin="bad"), dict(base, reqid=2, mpin="good")]


@pytest.mark.asyncio
class TestProcessorIsolation:

    async def _run(self, recharge_resp, pushed_side_effect=None, decrypt_side_effect=None):
        with patch("app.processor.async_fetch_pending_rows", new_callable=AsyncMock, return_value=_rows()), \
             patch("app.processor.token_manager") as tm, \
             patch("app.processor.decrypt", side_effect=decrypt_side_effect or (lambda v, k: "1234")), \
             patch("app.processor.recharge", new_callable=AsyncMock, return_value=recharge_resp) as rc, \
             patch("app.processor.async_mark_as_pushed", new_callable=AsyncMock,
                   side_effect=pushed_side_effect) as pushed, \
             patch("app.processor.async_mark_as_failed", new_callable=AsyncMock), \
             patch("app.processor.async_mark_for_reconciliation", new_callable=AsyncMock) as park, \
             patch("app.processor.async_release_unprocessed_claims", new_callable=AsyncMock) as release:
            tm.session_token = "s"
            tm.access_token = "a"
            summary = await process_pending_recharges(batch_size=2)
        return summary, rc, pushed, park, release

    async def test_bad_mpin_row_is_released_and_other_row_still_sent(self):
        def decrypt(v, k):
            if v == "bad":
                raise ValueError("bad padding")
            return "1234"
        resp = {"statusCode": 2002, "data": {"transactionId": 9}}
        summary, rc, pushed, park, release = await self._run(resp, decrypt_side_effect=decrypt)
        assert [c.kwargs["reqid"] for c in rc.call_args_list] == [2]
        release.assert_any_await([1])
        pushed.assert_awaited_once()
        park.assert_not_called()
        assert summary["registered"] == 1

    async def test_db_error_after_pyro_response_parks_with_txn_id(self):
        resp = {"statusCode": 2002, "data": {"transactionId": "43385"}}
        summary, rc, pushed, park, release = await self._run(
            resp, pushed_side_effect=RuntimeError("pg down"))
        assert rc.await_count == 2
        assert park.await_count == 2
        assert park.call_args.args[4] == 43385
        # never released: they were sent
        for call in release.call_args_list:
            assert 1 not in call.args[0] and 2 not in call.args[0]
        assert summary["reconciliation_required"] == 2
