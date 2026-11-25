"""H4 (audit_report.md): v3.0 callback codes 5027 (IN request failed / null IN response)
and 1030 (txn_in_failed) are terminal FAILED (decision 2026-11-25).

- Callback: marked FAILED.
- Status checker: marked FAILED on the first reply, not polled and then parked.
- Callback and status checker share one terminal set.
"""

import pytest

from app import callback, status_checker
from app.pyro_outcome import TERMINAL_FAILURE_CODES
from app.status_checker import run_status_checks

from tests.unit.test_p1_state_integrity import _body, cb, sc  # noqa: F401  (fixtures)

NEW_CODES = [5027, 1030]


def test_terminal_set_is_shared():
    assert callback._TERMINAL_STATUS_CODES is TERMINAL_FAILURE_CODES
    assert status_checker._TERMINAL_STATUS_CODES is TERMINAL_FAILURE_CODES


def test_terminal_set_contents():
    assert TERMINAL_FAILURE_CODES == {5010, 5030, 5005, 5011, 5012, 902, 5027, 1030}


@pytest.mark.parametrize("code", NEW_CODES)
def test_callback_marks_failed(cb, code):  # noqa: F811
    r = cb["client"].post("/callback/recharge", json=_body(code, "FAILED"))
    assert r.json() == {"received": True}
    cb["failed"].assert_awaited_once()
    assert cb["failed"].call_args.args[4] == code
    cb["success"].assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("code", NEW_CODES)
async def test_status_checker_fails_on_first_reply(sc, code):  # noqa: F811
    sc["fetch"].return_value[0]["status_check_count"] = 0   # first attempt, not the last
    sc["check"].return_value = {"statusCode": code, "status": "FAILED", "message": "IN failed"}
    summary = await run_status_checks()
    sc["failed"].assert_awaited_once()
    assert sc["failed"].call_args.args[4] == code
    sc["park"].assert_not_called()
    assert summary["failed"] == 1 and summary["retry"] == 0
