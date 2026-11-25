"""P2 FRC fixes.

#8a Auth/infrastructure failures (no action token, 506, 5001/5002, 5000 'Invalid token',
    Pyro unreachable) are retried WITHOUT using up max_retries, so rows can no longer
    get stuck in 'E' with retry_count > max_retries.
#8b Rows claimed but never pushed are released once no live batch can still hold them.
#12 The status checker only records SUCCESS when data.status is SUCCESS and the
    transaction data matches our row; otherwise the row is parked.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.config import settings
from app.db import postgres as pg
from app.processor import _is_token_failure, process_pending_recharges
from app.status_checker import run_status_checks


def _capture(fn, *args, fetchall=None, **kwargs):
    cur = MagicMock()
    cur.rowcount = 1
    cur.fetchall.return_value = fetchall or []
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    with patch("app.db.postgres.get_pg_write_conn") as get_conn:
        get_conn.return_value.__enter__.return_value = conn
        result = fn(*args, **kwargs)
    sql, params = cur.execute.call_args.args
    return " ".join(sql.split()), params, result


# ══════════════════════════════════════════════════════════════════════════════
# #8a
# ══════════════════════════════════════════════════════════════════════════════

class TestCountAttempt:

    @pytest.mark.parametrize("flag,count_attempt,expected", [
        ("E", True, True), ("E", False, False), ("F", True, False), ("F", False, False),
    ])
    def test_retry_count_increment_flag(self, flag, count_attempt, expected):
        sql, params, _ = _capture(pg.mark_as_failed, 501, flag, "m", "{}", 506,
                                  count_attempt=count_attempt)
        assert "retry_count = CASE WHEN %s THEN retry_count + 1 ELSE retry_count END" in sql
        assert params[4] is expected


@pytest.mark.parametrize("code,msg,expected", [
    (5000, "Invalid access token", True),
    (5000, "Invalid token", True),
    (5000, "Invalid credintials", True),
    (5000, "Invalid mpin", False),
    (5000, "User blocked", False),
    (405, "token", False),
    (5000, None, False),
])
def test_is_token_failure(code, msg, expected):
    assert _is_token_failure(code, msg) is expected


def _row(reqid=1):
    return {"reqid": reqid, "caf_serial_no": "C", "gsmno": "9400000000",
            "vendormsisdn": "8903696189", "ctopup_number": "8903696189", "frcamt": 153,
            "mpin": "m", "retry_count": 3, "max_retries": 3, "batch_date": "2026-11-21"}


async def _dispatch(response, rows=None):
    rows = rows or [_row()]
    with patch("app.processor.async_fetch_pending_rows", new_callable=AsyncMock, return_value=rows), \
         patch("app.processor.token_manager") as tm, \
         patch("app.processor.decrypt", return_value="1234"), \
         patch("app.processor.recharge", new_callable=AsyncMock, return_value=response) as rc, \
         patch("app.processor.async_mark_as_failed", new_callable=AsyncMock) as failed, \
         patch("app.processor.async_mark_for_reconciliation", new_callable=AsyncMock), \
         patch("app.processor.async_release_unprocessed_claims", new_callable=AsyncMock) as release:
        tm.session_token = "s"
        tm.access_token = "a"
        tm.authenticate = AsyncMock(return_value=True)
        summary = await process_pending_recharges(batch_size=len(rows))
    return summary, failed, rc, release, tm


@pytest.mark.asyncio
class TestInfraFailuresDoNotBurnRetries:

    @pytest.mark.parametrize("response", [
        {"statusCode": -1, "message": "Failed to generate action token", "_request_sent": False},
        {"statusCode": -1, "message": "Connection failed: refused", "_request_sent": False},
        {"statusCode": 506, "message": "Invalid token"},
        {"statusCode": 5001, "message": "Invalid credentials"},
        {"statusCode": 5000, "message": "Invalid access token"},
    ])
    async def test_retry_without_count_and_abort(self, response):
        summary, failed, rc, release, _ = await _dispatch(response, [_row(1), _row(2)])
        flag = failed.call_args.args[1]
        assert flag == "E"
        assert failed.call_args.kwargs.get("count_attempt") is False
        # batch aborted: second row not sent and released back
        assert rc.await_count == 1
        release.assert_awaited()

    async def test_token_5000_triggers_reauth(self):
        _, _, _, _, tm = await _dispatch({"statusCode": 5000, "message": "Invalid access token"})
        tm.authenticate.assert_awaited_once()

    async def test_business_5000_still_counts(self):
        _, failed, _, _, _ = await _dispatch({"statusCode": 5000, "message": "Some thing went wrong"})
        # retry_count 3 == max_retries 3 -> exhausted -> final failure via _handle_transient
        assert failed.call_args.args[1] == "F"
        assert failed.call_args.kwargs.get("count_attempt", True) is True


# ══════════════════════════════════════════════════════════════════════════════
# #8b
# ══════════════════════════════════════════════════════════════════════════════

class TestOrphanedClaims:

    def test_age_covers_worst_case_batch(self):
        with patch.object(settings, "recharge_batch_size", 500), \
             patch.object(settings, "recharge_concurrency", 4), \
             patch.object(settings, "pyro_request_timeout_seconds", 30.0):
            # 125 waves * 65 s = 135.4 min, + 15 min margin
            assert pg.orphan_claim_age_minutes() == 150

    def test_age_has_floor(self):
        with patch.object(settings, "recharge_batch_size", 4), \
             patch.object(settings, "recharge_concurrency", 4), \
             patch.object(settings, "pyro_request_timeout_seconds", 5.0):
            assert pg.orphan_claim_age_minutes() == 30

    def test_release_targets_only_never_pushed_claims(self):
        sql, params, released = _capture(pg.release_orphaned_claims, 150, fetchall=[(9,)])
        assert "WHERE push_flag = 'P' AND push_date IS NULL AND pyro_trans_id IS NULL" in sql
        assert "submitted_at < CURRENT_TIMESTAMP" in sql
        assert "CASE WHEN retry_count > 0 THEN 'E' ELSE 'N' END" in sql
        assert params == (150,) and released == [9]


# ══════════════════════════════════════════════════════════════════════════════
# #12
# ══════════════════════════════════════════════════════════════════════════════

def _status(**overrides):
    data = {"clientTxnId": "00501", "transactionId": "43385", "destMsisdn": "9489902775",
            "amount": 153.0, "status": "SUCCESS",
            "dealerBalanceBefore": 446.28, "dealerBalanceAfter": 293.28}
    data.update(overrides)
    return {"statusCode": 2000, "status": "SUCCESS", "message": "Recharge successful", "data": data}


@pytest.fixture
def sc():
    with patch("app.status_checker.async_release_orphaned_claims", new_callable=AsyncMock, return_value=[]) as orphans, \
         patch("app.status_checker.async_park_stale_pushed_rows", new_callable=AsyncMock, return_value=[]), \
         patch("app.status_checker.async_fetch_pushed_rows_for_status_check", new_callable=AsyncMock) as fetch, \
         patch("app.status_checker.check_transaction_status", new_callable=AsyncMock) as check, \
         patch("app.status_checker.async_update_status_check_attempt", new_callable=AsyncMock), \
         patch("app.status_checker.async_mark_as_success", new_callable=AsyncMock, return_value=1) as success, \
         patch("app.status_checker.async_mark_for_reconciliation", new_callable=AsyncMock) as park:
        fetch.return_value = [{"reqid": 501, "pyro_trans_id": 43385, "caf_serial_no": "CAF501",
                               "gsmno": "9489902775", "batch_date": "2026-11-21",
                               "status_check_count": 0, "client_txn_id": "00501", "frcamt": 153}]
        yield {"orphans": orphans, "check": check, "success": success, "park": park}


@pytest.mark.asyncio
class TestStatusCheckSuccessValidation:

    async def test_matching_success_is_recorded(self, sc):
        sc["check"].return_value = _status()
        await run_status_checks()
        sc["success"].assert_awaited_once()
        sc["park"].assert_not_called()

    async def test_2000_with_contradicting_status_is_parked(self, sc):
        sc["check"].return_value = _status(status="PENDING")
        summary = await run_status_checks()
        sc["success"].assert_not_called()
        sc["park"].assert_awaited_once()
        assert summary["parked"] == 1

    async def test_2000_with_null_data_status_is_success(self, sc):
        # C1: data.status is not in the spec's parameter table; statusCode 2000 decides.
        sc["check"].return_value = _status(status=None)
        summary = await run_status_checks()
        sc["success"].assert_awaited_once()
        sc["park"].assert_not_called()
        assert summary["success"] == 1

    @pytest.mark.parametrize("overrides", [
        {"amount": 199.0}, {"destMsisdn": "9400000000"}, {"clientTxnId": "00502"},
        {"transactionId": "99999"},
    ])
    async def test_mismatched_success_is_parked(self, sc, overrides):
        sc["check"].return_value = _status(**overrides)
        await run_status_checks()
        sc["success"].assert_not_called()
        assert "mismatch" in sc["park"].call_args.args[1]

    async def test_orphan_sweep_uses_computed_age(self, sc):
        sc["check"].return_value = _status()
        with patch("app.status_checker.orphan_claim_age_minutes", return_value=150):
            await run_status_checks()
        sc["orphans"].assert_awaited_once_with(150)
