import asyncio
import json
import logging
from typing import Optional, Sequence

from app.auth.token_manager import token_manager
from app.context import ExecutionContext
from app.db.oracle import (
    BCD_STATUS_F,
    BCD_STATUS_ID,
    BCD_STATUS_W,
    INVALID_DATA_CODES,
    update_bcd_status,
    bcd_status_for_pyro_failure,
)
from app.db.postgres import (
    FLAG_FAILED,
    FLAG_RETRY,
    PERMANENT_FAILURE_CODES,
    async_fetch_pending_rows,
    async_mark_as_failed,
    async_mark_as_pushed,
    async_release_unprocessed_claims,
)
from app.pyro_client import recharge
from app.config import settings
from app.encryption import decrypt

logger = logging.getLogger(__name__)

async def _handle_transient(row, reqid, caf, remarks, response_text, status_code):
    attempt = int(row["retry_count"]) + 1
    flag = FLAG_FAILED if attempt >= int(row["max_retries"]) else FLAG_RETRY
    await async_mark_as_failed(reqid, flag, remarks, response_text, status_code)

    if attempt >= int(row["max_retries"]):
        await asyncio.to_thread(
            update_bcd_status, caf, reqid, BCD_STATUS_F,
            f"Max retries exhausted. Last error: {remarks}"
        )
        logger.warning("reqid=%s max retries exhausted -> BCD=F: %s", reqid, remarks)
    else:
        logger.warning("reqid=%s TRANSIENT (retry %d/%d): %s",
                       reqid, attempt, row["max_retries"], remarks)
    return flag

async def process_pending_recharges(
    batch_size: int = 500,
    circle_codes: Optional[Sequence[int]] = None,
    context: Optional[ExecutionContext] = None,
) -> dict:
    if context is not None and circle_codes is None:
        circle_codes = context.circle_codes

    if context:
        logger.info(
            "Processor started [exec_id=%s][source=%s][zones=%s][mode=%s] (batch_size=%d)",
            context.execution_id,
            context.source,
            list(context.zone_codes),
            context.mode,
            batch_size,
        )
    else:
        logger.info("Processor started (batch_size=%d)", batch_size)

    rows = await async_fetch_pending_rows(batch_size, circle_codes=circle_codes)
    if not rows:
        logger.info("Processor: no pending rows")
        return {
            "execution_id":      context.execution_id if context else None,
            "source":            context.source if context else None,
            "zones":             list(context.zone_codes) if context else None,
            "mode":              context.mode if context else None,
            "claimed":           0,
            "submitted":         0,
            "success":           0,
            "transient_failure": 0,
            "permanent_failure": 0,
            "retry":             0,
            "processed":         0,
            "registered":        0,
            "perm_failed":       0,
            "retryable":         0,
        }

    if not token_manager.session_token or not token_manager.access_token:
        logger.warning("Processor: Pyro tokens missing before recharge batch; authenticating once")
        if not await token_manager.authenticate():
            logger.error("Processor: Pyro authentication unavailable; releasing claimed rows and deferring")
            await async_release_unprocessed_claims([r["reqid"] for r in rows])
            return {
                "execution_id":      context.execution_id if context else None,
                "source":            context.source if context else None,
                "zones":             list(context.zone_codes) if context else None,
                "mode":              context.mode if context else None,
                "claimed":           len(rows),
                "submitted":         0,
                "success":           0,
                "transient_failure": 0,
                "permanent_failure": 0,
                "retry":             0,
                "processed":         0,
                "registered":        0,
                "perm_failed":       0,
                "retryable":         0,
                "auth_failed":       True,
            }

    registered = perm_failed = retryable = 0
    exhausted_dealers = set()
    processed_reqids = set()
    try:
        for row in rows:
            reqid         = row["reqid"]
            caf           = row["caf_serial_no"]
            gsmno         = row["gsmno"]
            dealer_msisdn = row["vendormsisdn"] or row["ctopup_number"]
            amount        = int(row["frcamt"])
            mpin          = decrypt(row["mpin"], settings.pyro_secret_key)  # already 3DES-encrypted in DB
            attempt       = int(row["retry_count"]) + 1
            # client_txn_id = row.get("client_txn_id") or str(reqid).zfill(5)[:15]

            logger.info("Processing reqid=%s gsmno=%s amount=%s attempt=%s",
                        reqid, gsmno, amount, attempt)
            if dealer_msisdn in exhausted_dealers:
                remarks = f"[405] Skipped — dealer {dealer_msisdn} had insufficient balance earlier in this batch"
                logger.error("SKIPPED: reqid=%s dealerMsisdn=%s — %s", reqid, dealer_msisdn, remarks)
                await async_release_unprocessed_claims([reqid])
                processed_reqids.add(reqid)
                retryable += 1
                continue
            
            processed_reqids.add(reqid)
            response = await recharge(
                reqid=reqid,
                caf_serial_no=caf,
                gsmno=gsmno,
                batch_date=row["batch_date"],
                dealer_msisdn=dealer_msisdn,
                dest_msisdn=gsmno,
                amount=amount,            
                mpin=mpin,
                attempt_no=attempt,
            )

            status_code   = response.get("statusCode")
            response_text = json.dumps(response)
            inner         = response.get("data", {})

            if status_code == 2002:
                # Registered -- await callback
                pyro_trans_id = inner.get("transactionId")
                await async_mark_as_pushed(
                    reqid, pyro_trans_id, response_text,
                    f"dealerMsisdn={dealer_msisdn} destMsisdn={gsmno} amount={amount}",
                    status_code,
                )
                # BCD: W = Waiting for callback
                await asyncio.to_thread(
                    update_bcd_status, caf, reqid, BCD_STATUS_W,
                    f"FRC submitted to Pyro. pyroTxnId={pyro_trans_id}"
                )
                logger.info("reqid=%s registered -- pyroTxnId=%s", reqid, pyro_trans_id)
                registered += 1

            elif status_code == -1 and response.get("message") == "Failed to generate action token":
                remarks = "[-1] Failed to generate action token"
                await async_mark_as_failed(reqid, FLAG_RETRY, remarks,
                                           response_text, status_code)
                retryable += 1
                logger.warning(
                    "reqid=%s TRANSIENT: action token unavailable; aborting remainder of batch",
                    reqid,
                )
                break

            elif status_code == 405:
                exhausted_dealers.add(dealer_msisdn)
                remarks = f"[405] Insufficient dealer balance — dealerMsisdn={dealer_msisdn}"
                await async_mark_as_failed(reqid, FLAG_RETRY, remarks,
                                           response_text, status_code)
                logger.error(                         
                    "DEALER BALANCE EXHAUSTED: dealerMsisdn=%s reqid=%s — "
                    "remaining rows for this dealer will be skipped this batch run",
                    dealer_msisdn, reqid,
                )
                retryable += 1

            elif status_code == 506:
                logger.error("Invalid token (506) — triggering re-auth and aborting batch")
                await token_manager.authenticate()
                # Mark current row for retry, then break — tokens will be fresh next run
                await async_mark_as_failed(reqid, FLAG_RETRY,
                                        "[506] Invalid token — will retry after re-auth",
                                        response_text, status_code)
                retryable += 1
                break 

            elif status_code in PERMANENT_FAILURE_CODES:
                remarks = f"[{status_code}] {response.get('message', 'Permanent failure')}"
                await async_mark_as_failed(reqid, FLAG_FAILED, remarks,
                                           response_text, status_code)
                # BCD: ID for data errors, F for others
                bcd_status = bcd_status_for_pyro_failure(status_code)
                await asyncio.to_thread(
                    update_bcd_status, caf, reqid, bcd_status, remarks
                )
                logger.warning("reqid=%s PERMANENT [%s]: %s", reqid, bcd_status, remarks)
                perm_failed += 1

            elif status_code in (5001, 5002):
                logger.error("Auth credential error (%s) — triggering re-auth and aborting batch", status_code)
                await token_manager.authenticate()
                await async_mark_as_failed(reqid, FLAG_RETRY,
                                        f"[{status_code}] Auth error — will retry after credential fix",
                                        response_text, status_code)
                retryable += 1
                break
            
            elif status_code in (415, 5016):
                remarks = f"[{status_code}] Duplicate subscriber window — retry after 15 min"
                await _handle_transient(row, reqid, caf, remarks, response_text, status_code)
                retryable += 1

            elif status_code in (500, 5000):
                remarks = f"[{status_code}] Pyro/IN transient error — will retry"
                await _handle_transient(row, reqid, caf, remarks, response_text, status_code)
                retryable += 1

            else:
                remarks = f"[{status_code}] {response.get('message', 'Transient error')}"
                await _handle_transient(row, reqid, caf, remarks, response_text, status_code)
                retryable += 1

    finally:
        unprocessed_reqids = [r["reqid"] for r in rows if r["reqid"] not in processed_reqids]
        if unprocessed_reqids:
            logger.warning(
                "Processor: releasing %d claimed rows that were not submitted due to batch abort/break",
                len(unprocessed_reqids),
            )
            await async_release_unprocessed_claims(unprocessed_reqids)

    summary = {
        "execution_id":      context.execution_id if context else None,
        "source":            context.source if context else None,
        "zones":             list(context.zone_codes) if context else None,
        "mode":              context.mode if context else None,
        "claimed":           len(rows),
        "submitted":         len(processed_reqids),
        "success":           registered,
        "transient_failure": retryable,
        "permanent_failure": perm_failed,
        "retry":             retryable,
        # Legacy/detailed keys preserved:
        "processed":         len(processed_reqids),
        "registered":        registered,
        "perm_failed":       perm_failed,
        "retryable":         retryable,
    }
    logger.info(
        "Processor complete [exec_id=%s][source=%s][zones=%s][mode=%s]: "
        "claimed=%d submitted=%d success=%d transient_failure=%d permanent_failure=%d retry=%d",
        summary["execution_id"], summary["source"], summary["zones"], summary["mode"],
        summary["claimed"], summary["submitted"], summary["success"],
        summary["transient_failure"], summary["permanent_failure"], summary["retry"],
    )
    return summary
