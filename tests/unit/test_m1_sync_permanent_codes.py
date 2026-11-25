"""M1 (audit_report.md): a synchronous 5005 (allowed by v2.0) is a permanent failure.

Retrying it reuses the same clientTxnId, Pyro answers 5018 and the row ends up parked.
5027 and 1030 (v3.0 callback-only, terminal per H4) are treated the same way if they
ever arrive synchronously.
"""

import pytest

from app.db.postgres import FLAG_FAILED, PERMANENT_FAILURE_CODES

from tests.unit.test_p0_recharge_outcomes import _dispatch


@pytest.mark.parametrize("code", [5005, 5027, 1030])
def test_code_is_permanent(code):
    assert code in PERMANENT_FAILURE_CODES


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [5005, 5027, 1030])
async def test_sync_reply_fails_row_without_retry(code):
    resp = {"statusCode": code, "status": "FAILED", "message": "IN failure", "_request_sent": True}
    summary, m = await _dispatch(resp, retry_count=0)
    m["failed"].assert_awaited_once()
    args = m["failed"].call_args.args
    assert args[1] == FLAG_FAILED and args[4] == code
    m["reconcile"].assert_not_called()
    assert summary["perm_failed"] == 1
