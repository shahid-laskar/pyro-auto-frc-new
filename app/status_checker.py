
import asyncio
import json
import logging

from app.db.oracle import (
    BCD_STATUS_F,
    BCD_STATUS_NR,
    BCD_STATUS_P,
    update_bcd_status,
)
from app.db.postgres import (
    FLAG_FAILED,
    FLAG_RETRY,
    async_fetch_pushed_rows_for_status_check,
    async_mark_as_failed,
    async_mark_as_success,
    async_update_status_check_attempt,
)
from app.pyro_client import check_transaction_status
from app.config import settings
logger = logging.getLogger(__name__)


async def run_status_checks() -> dict:
    """Check status of pushed-but-no-callback rows."""
    rows = await async_fetch_pushed_rows_for_status_check()
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

        # First time status check is initiated -> BCD: NR (No Response)
        if row.get("status_check_count", 0) == 0:
            await asyncio.to_thread(
                update_bcd_status, caf, reqid, BCD_STATUS_NR,
                f"No callback received. Initiating status check. pyroTxnId={pyro_trans_id}"
            )
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
            balance_after = inner.get("dealerBalanceAfter", 0.0)
            await async_mark_as_success(reqid, response_text,balance_before, balance_after, status_code)
            await asyncio.to_thread(
                update_bcd_status, caf, reqid, BCD_STATUS_P,
                f"Recharge successful via status check. pyroTxnId={pyro_trans_id}"
            )
            logger.info("reqid=%s STATUS CHECK SUCCESS", reqid)
            success += 1

        elif status_code == 902:
            remarks = f"[902] Transaction failed on Pyro"
            await async_mark_as_failed(reqid, FLAG_FAILED, remarks,
                                       response_text, status_code)
            await asyncio.to_thread(
                update_bcd_status, caf, reqid, BCD_STATUS_F,
                f"Transaction failed on Pyro. pyroTxnId={pyro_trans_id}"
            )
            logger.warning("reqid=%s STATUS CHECK: FAILED (902)", reqid)
            failed += 1

        elif status_code == 901:
            if attempt_no >= settings.status_check_max_attempts:
                remarks = (
                    f"[901] Transaction not found after {attempt_no} "
                    "status-check attempts"
                )
                await async_mark_as_failed(reqid, FLAG_FAILED, remarks,
                                           response_text, status_code)
                await asyncio.to_thread(
                    update_bcd_status, caf, reqid, BCD_STATUS_F, remarks
                )
                logger.warning("reqid=%s STATUS CHECK: final failed after 901", reqid)
                failed += 1
            else:
                logger.warning(
                    "reqid=%s STATUS CHECK: not found (901) -- retry %d/%d",
                    reqid, attempt_no, settings.status_check_max_attempts,
                )
                retry += 1

        else:
            remarks = f"[{status_code}] {response.get('message', 'Unknown')}"
            if attempt_no >= settings.status_check_max_attempts:
                final_remarks = (
                    f"{remarks}; max status-check attempts reached "
                    f"({attempt_no}/{settings.status_check_max_attempts})"
                )
                await async_mark_as_failed(reqid, FLAG_FAILED, final_remarks,
                                           response_text, status_code)
                await asyncio.to_thread(
                    update_bcd_status, caf, reqid, BCD_STATUS_F, final_remarks
                )
                logger.warning("reqid=%s STATUS CHECK final failure: %s",
                               reqid, final_remarks)
                failed += 1
            else:
                logger.warning(
                    "reqid=%s STATUS CHECK unknown retry %d/%d: %s",
                    reqid, attempt_no, settings.status_check_max_attempts, remarks,
                )
                retry += 1

    summary = {"checked": len(rows), "success": success,
               "failed": failed, "retry": retry}
    logger.info("Status checker complete -- %s", summary)
    return summary