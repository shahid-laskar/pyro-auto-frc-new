"""C1 (audit_report.md): success is decided by statusCode 2000, not by data.status.

data.status is not in either spec's parameter table (D4), so it may be absent.
- 2000 with no status anywhere           -> SUCCESS
- 2000 with SUCCESS in any case/position  -> SUCCESS
- 2000 with an explicit non-SUCCESS status -> parked for reconciliation, never FAILED
The callback, the status checker and the synchronous recharge path share one rule.
"""

from unittest.mock import AsyncMock, patch

import pytest

from app.pyro_outcome import OUTCOME_RECONCILE, OUTCOME_SUCCESS, classify_2000
from app.status_checker import run_status_checks

from tests.unit.test_p0_recharge_outcomes import _dispatch
from tests.unit.test_p1_state_integrity import _body, _status_success, cb, sc  # noqa: F401  (fixtures)


def _strip_status(body):
    body.pop("status", None)
    body["data"].pop("status", None)
    return body


# ── shared classifier ─────────────────────────────────────────────────────────

class TestClassify2000:

    @pytest.mark.parametrize("response", [
        {"statusCode": 2000, "data": {}},
        {"statusCode": 2000},
        {"statusCode": 2000, "data": None},
        {"statusCode": 2000, "status": "SUCCESS", "data": {}},
        {"statusCode": 2000, "data": {"status": "SUCCESS"}},
        {"statusCode": 2000, "status": "Success", "data": {"status": " success "}},
        {"statusCode": 2000, "status": "", "data": {"status": None}},
    ])
    def test_success(self, response):
        assert classify_2000(response)[0] == OUTCOME_SUCCESS

    @pytest.mark.parametrize("response", [
        {"statusCode": 2000, "status": "PENDING", "data": {}},
        {"statusCode": 2000, "data": {"status": "FAILED"}},
        {"statusCode": 2000, "status": "SUCCESS", "data": {"status": "In Process"}},
    ])
    def test_contradicting_status_is_reconcile(self, response):
        outcome, reason = classify_2000(response)
        assert outcome == OUTCOME_RECONCILE
        assert reason


# ── callback ──────────────────────────────────────────────────────────────────

@pytest.fixture
def cb_park(cb):  # noqa: F811
    with patch("app.callback.async_mark_for_reconciliation", new_callable=AsyncMock,
               return_value=1) as park:
        cb["park"] = park
        yield cb


class TestCallbackSuccessRule:

    def test_2000_without_any_status_is_success(self, cb_park):
        r = cb_park["client"].post("/callback/recharge", json=_strip_status(_body()))
        assert r.json() == {"received": True}
        cb_park["success"].assert_awaited_once()
        cb_park["failed"].assert_not_called()

    def test_2000_with_top_level_status_only_is_success(self, cb_park):
        body = _body()
        del body["data"]["status"]
        cb_park["client"].post("/callback/recharge", json=body)
        cb_park["success"].assert_awaited_once()
        cb_park["failed"].assert_not_called()

    def test_2000_lowercase_success_is_success(self, cb_park):
        cb_park["client"].post("/callback/recharge", json=_body(status="Success"))
        cb_park["success"].assert_awaited_once()

    @pytest.mark.parametrize("status", ["FAILED", "PENDING"])
    def test_2000_with_contradicting_status_is_parked_not_failed(self, cb_park, status):
        cb_park["client"].post("/callback/recharge", json=_body(status=status))
        cb_park["park"].assert_awaited_once()
        cb_park["failed"].assert_not_called()
        cb_park["success"].assert_not_called()

    def test_2000_without_status_still_validated(self, cb_park):
        r = cb_park["client"].post("/callback/recharge",
                                   json=_strip_status(_body(amount=999)))
        assert r.json()["note"] == "callback validation failed"
        cb_park["success"].assert_not_called()

    def test_failure_code_still_fails(self, cb_park):
        cb_park["client"].post("/callback/recharge", json=_body(5005, "FAILED"))
        cb_park["failed"].assert_awaited_once()
        cb_park["park"].assert_not_called()


# ── status checker ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
class TestStatusCheckerSuccessRule:

    async def test_2000_without_any_status_is_success(self, sc):  # noqa: F811
        resp = _status_success()
        resp.pop("status")
        resp["data"].pop("status")
        sc["check"].return_value = resp
        summary = await run_status_checks()
        sc["success"].assert_awaited_once()
        sc["park"].assert_not_called()
        assert summary["success"] == 1

    async def test_2000_without_status_and_bad_data_is_parked(self, sc):  # noqa: F811
        resp = _status_success(amount=999)
        resp["data"].pop("status")
        sc["check"].return_value = resp
        await run_status_checks()
        sc["park"].assert_awaited_once()
        sc["success"].assert_not_called()

    async def test_2000_with_failed_status_is_parked(self, sc):  # noqa: F811
        sc["check"].return_value = _status_success(status="FAILED")
        await run_status_checks()
        sc["park"].assert_awaited_once()
        sc["failed"].assert_not_called()


# ── synchronous recharge reply ────────────────────────────────────────────────

@pytest.mark.asyncio
class TestSyncRechargeSuccessRule:

    async def test_sync_2000_without_any_status_is_success(self):
        resp = {"statusCode": 2000, "message": "Recharge successful",
                "data": {"transactionId": "43385"}, "_request_sent": True}
        summary, m = await _dispatch(resp)
        m["success"].assert_awaited_once()
        m["reconcile"].assert_not_called()

    async def test_sync_2000_contradicting_inner_status_is_parked(self):
        resp = {"statusCode": 2000, "status": "SUCCESS", "message": "?",
                "data": {"status": "FAILED"}, "_request_sent": True}
        summary, m = await _dispatch(resp)
        m["reconcile"].assert_awaited_once()
        m["success"].assert_not_called()
