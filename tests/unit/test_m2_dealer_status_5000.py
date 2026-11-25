"""M2 (audit_report.md), option (b): a synchronous 5000 that reports the dealer's status
(User blocked / Inactive user / User not available / not authorized) fails the row at
once instead of being retried up to max_retries. Per-row only: other rows of the same
dealer are still sent. Token 5000s and other 5000s keep their existing handling.
"""

import pytest

from app.db.postgres import FLAG_FAILED, FLAG_RETRY
from app.pyro_outcome import is_dealer_status_failure

from tests.unit.test_p0_recharge_outcomes import _dispatch

DEALER_MESSAGES = [
    "User blocked",
    "Inactive user",
    "In active user",                                 # v2.0 spelling
    "User not available",
    "You are not authorized to use this service",
    "You are not authorised to use this service.",    # British spelling
]


@pytest.mark.parametrize("msg", DEALER_MESSAGES)
def test_matcher_accepts_dealer_messages(msg):
    assert is_dealer_status_failure(5000, msg)


@pytest.mark.parametrize("code,msg", [
    (5000, "Invalid access token"),
    (5000, "Something went wrong"),
    (5000, None),
    (500, "User blocked"),
    (406, "You are not authorised to use this service."),
])
def test_matcher_rejects_others(code, msg):
    assert not is_dealer_status_failure(code, msg)


@pytest.mark.asyncio
class TestProcessorDealerStatus5000:

    @pytest.mark.parametrize("msg", DEALER_MESSAGES)
    async def test_fails_row_on_first_attempt(self, msg):
        resp = {"statusCode": 5000, "status": "FAILED", "message": msg, "_request_sent": True}
        summary, m = await _dispatch(resp, retry_count=0)
        m["failed"].assert_awaited_once()
        args = m["failed"].call_args.args
        assert args[1] == FLAG_FAILED and args[2] == msg and args[4] == 5000
        m["reconcile"].assert_not_called()
        assert summary["perm_failed"] == 1

    async def test_other_5000_is_still_retried(self):
        resp = {"statusCode": 5000, "status": "FAILED", "message": "Something went wrong",
                "_request_sent": True}
        summary, m = await _dispatch(resp, retry_count=0)
        assert m["failed"].call_args.args[1] == FLAG_RETRY
        assert summary["perm_failed"] == 0
