
import json
import logging

from app.db.postgres import (
    FLAG_FAILED,
    async_fetch_pushed_rows_for_status_check,
    async_mark_as_failed,
    async_mark_as_success,
    async_update_status_check_attempt,
)
from app.pyro_client import check_transaction_status
from app.config import settings

logger = logging.getLogger(__name__)

# Codes that are terminal when received from status check (stop polling immediately)
_TERMINAL_STATUS_CODES = {5010, 5030, 5005, 5011, 5012, 902}


async def run_status_checks() -> dict:
    """Check status of pushed-but-no-callback rows."""
    limit = getattr(settings, "status_check_batch_size", 100)
    rows = await async_fetch_pushed_rows_for_status_check(limit=limit)
    if not rows:
        logger.info("Status checker: no rows pending")
        return {"checked": 0, "success": 0, "failed": 0, "retry": 0}

    logger.info("Status checker: checking %d row(s)", len(rows))
    success = failed = retry = 0

    for row in rows:
        reqid         = row["reqid"]
        pyro_trans_id = row["pyro_trans_id"]
        caf           = row["caf_serial_no"]
        gsmno         = row["gsmno"]
        batch_date    = row["batch_date"]

        await async_update_status_check_attempt(reqid)

        attempt_no = row.get("status_check_count", 0) + 1
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
        inner         = response.get("data", {})

        if status_code == 2000:
            balance_before = inner.get("dealerBalanceBefore", 0.0)
            balance_after  = inner.get("dealerBalanceAfter", 0.0)
            await async_mark_as_success(reqid, response_text, balance_before, balance_after, status_code)
            logger.info("reqid=%s STATUS CHECK SUCCESS", reqid)
            success += 1

        elif status_code in _TERMINAL_STATUS_CODES:
            # Terminal IN-switch or network failure — stop polling immediately.
            # 5010 = "Only First Recharge accounts allowed" (already FRC done at switch).
            pyro_msg = response.get("message")
            await async_mark_as_failed(reqid, FLAG_FAILED, pyro_msg, response_text, status_code)
            logger.warning("reqid=%s STATUS CHECK TERMINAL [%s]: %s", reqid, status_code, pyro_msg)
            failed += 1

        elif status_code in (2002, 901):
            # Still in-process or not found yet — keep polling while within attempt limit
            if attempt_no >= settings.status_check_max_attempts:
                pyro_msg = response.get("message")
                await async_mark_as_failed(reqid, FLAG_FAILED, pyro_msg, response_text, status_code)
                logger.warning("reqid=%s STATUS CHECK: final failure after %d attempts [%s]",
                               reqid, attempt_no, status_code)
                failed += 1
            else:
                logger.warning(
                    "reqid=%s STATUS CHECK: [%s] retry %d/%d",
                    reqid, status_code, attempt_no, settings.status_check_max_attempts,
                )
                retry += 1

        else:
            # Unknown/unexpected code
            if attempt_no >= settings.status_check_max_attempts:
                pyro_msg = response.get("message")
                await async_mark_as_failed(reqid, FLAG_FAILED, pyro_msg, response_text, status_code)
                logger.warning("reqid=%s STATUS CHECK final failure [%s]: %s",
                               reqid, status_code, pyro_msg)
                failed += 1
            else:
                logger.warning(
                    "reqid=%s STATUS CHECK unknown code [%s] retry %d/%d: %s",
                    reqid, status_code, attempt_no, settings.status_check_max_attempts,
                    response.get("message"),
                )
                retry += 1

    summary = {"checked": len(rows), "success": success,
               "failed": failed, "retry": retry}
    logger.info("Status checker complete -- %s", summary)
    return summary