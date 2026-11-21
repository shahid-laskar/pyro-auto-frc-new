
import json
import logging

from app.db.postgres import (
    FLAG_FAILED,
    async_fetch_pushed_rows_for_status_check,
    async_mark_as_failed,
    async_mark_as_success,
    async_mark_for_reconciliation,
    async_park_stale_pushed_rows,
    async_update_status_check_attempt,
)
from app.pyro_client import check_transaction_status
from app.config import settings

logger = logging.getLogger(__name__)

# Codes that are terminal when received from status check (stop polling immediately)
_TERMINAL_STATUS_CODES = {5010, 5030, 5005, 5011, 5012, 902}

# Pushed rows still without a final status after this long are parked for
# reconciliation (they stay resolvable by a late callback).
STALE_PUSH_MINUTES = 60


async def run_status_checks() -> dict:
    """Check status of pushed-but-no-callback rows.

    Every state change is a guarded UPDATE, so a callback that finalises a row while
    this loop is running is never overwritten.
    """
    parked = 0
    try:
        stale = await async_park_stale_pushed_rows(STALE_PUSH_MINUTES)
    except Exception as exc:
        stale = []
        logger.error("Status checker: stale-push sweep failed: %s", exc)
    if stale:
        parked += len(stale)
        logger.critical(
            "Status checker: parked %d pushed row(s) with no final status after %d min "
            "for reconciliation: %s", len(stale), STALE_PUSH_MINUTES, stale[:50],
        )

    limit = getattr(settings, "status_check_batch_size", 100)
    rows = await async_fetch_pushed_rows_for_status_check(limit=limit)
    if not rows:
        logger.info("Status checker: no rows pending")
        return {"checked": 0, "success": 0, "failed": 0, "retry": 0,
                "parked": parked, "transport_error": 0}

    logger.info("Status checker: checking %d row(s)", len(rows))
    success = failed = retry = transport_error = 0

    for row in rows:
        reqid         = row["reqid"]
        pyro_trans_id = row["pyro_trans_id"]
        caf           = row["caf_serial_no"]
        gsmno         = row["gsmno"]
        batch_date    = row["batch_date"]

        attempt_no = (row.get("status_check_count") or 0) + 1
        response = await check_transaction_status(
            reqid=reqid,
            caf_serial_no=caf,
            gsmno=gsmno,
            batch_date=batch_date,
            pyro_trans_id=str(pyro_trans_id),
            attempt_no=attempt_no,
        )

        status_code   = response.get("statusCode")
        response_text = json.dumps(response)
        inner         = response.get("data") or {}
        pyro_msg      = response.get("message")

        if status_code == -1:
            # Transport / parse error: says nothing about the transaction, so it does
            # not use up an attempt. Rows that never get an answer are parked by the
            # stale-push sweep instead of being failed.
            logger.warning("reqid=%s STATUS CHECK transport error (not counted): %s", reqid, pyro_msg)
            transport_error += 1
            continue

        await async_update_status_check_attempt(reqid)

        if status_code == 2000:
            updated = await async_mark_as_success(
                reqid, response_text,
                inner.get("dealerBalanceBefore", 0.0), inner.get("dealerBalanceAfter", 0.0),
                status_code,
            )
            if updated == 0:
                logger.info("reqid=%s STATUS CHECK SUCCESS -- row already final, unchanged", reqid)
            else:
                logger.info("reqid=%s STATUS CHECK SUCCESS", reqid)
            success += 1

        elif status_code in _TERMINAL_STATUS_CODES:
            # Terminal IN-switch or network failure — stop polling immediately.
            # 5010 = "Only First Recharge accounts allowed" (already FRC done at switch).
            await async_mark_as_failed(reqid, FLAG_FAILED, pyro_msg, response_text, status_code)
            logger.warning("reqid=%s STATUS CHECK TERMINAL [%s]: %s", reqid, status_code, pyro_msg)
            failed += 1

        elif attempt_no >= settings.status_check_max_attempts:
            # Still in process (2002), not found (901) or unexpected code after the last
            # attempt. The recharge may still complete, so this is not a failure:
            # park it, a late callback can still resolve it.
            await async_mark_for_reconciliation(
                reqid,
                f"status check gave no final result after {attempt_no} attempts "
                f"[{status_code}] {pyro_msg or ''}".strip(),
                response_text, status_code,
            )
            logger.critical(
                "reqid=%s STATUS CHECK: no final result after %d attempts [%s] -- parked for reconciliation",
                reqid, attempt_no, status_code,
            )
            parked += 1

        else:
            logger.warning(
                "reqid=%s STATUS CHECK: [%s] retry %d/%d: %s",
                reqid, status_code, attempt_no, settings.status_check_max_attempts, pyro_msg,
            )
            retry += 1

    summary = {"checked": len(rows), "success": success, "failed": failed, "retry": retry,
               "parked": parked, "transport_error": transport_error}
    logger.info("Status checker complete -- %s", summary)
    return summary
