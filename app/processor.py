import asyncio
import json
import logging
from typing import Optional, Sequence

from app.auth.token_manager import token_manager
from app.context import ExecutionContext
from app.db.postgres import (
    FLAG_FAILED,
    FLAG_RETRY,
    PERMANENT_FAILURE_CODES,
    async_fetch_pending_rows,
    async_mark_as_failed,
    async_mark_as_pushed,
    async_mark_as_success,
    async_mark_for_reconciliation,
    async_release_unprocessed_claims,
)
from app.pyro_client import recharge
from app.pyro_outcome import (
    OUTCOME_SUCCESS, classify_2000, is_dealer_status_failure, is_token_failure,
)
from app.config import settings
from app.encryption import decrypt

logger = logging.getLogger(__name__)


async def _handle_transient(row, reqid, pyro_msg, response_text, status_code):
    """Handle transient (retryable) failure for 405 dealer-balance errors.

    Increments retry_count. If max_retries exhausted, marks FLAG_FAILED;
    otherwise marks FLAG_RETRY. Always uses the verbatim Pyro message.
    """
    attempt = int(row["retry_count"]) + 1
    flag = FLAG_FAILED if attempt >= int(row["max_retries"]) else FLAG_RETRY
    await async_mark_as_failed(reqid, flag, pyro_msg, response_text, status_code)
    if flag == FLAG_FAILED:
        logger.warning("reqid=%s max retries exhausted -> FLAG_FAILED: %s", reqid, pyro_msg)
    else:
        logger.warning("reqid=%s TRANSIENT (retry %d/%d): %s",
                       reqid, attempt, row["max_retries"], pyro_msg)
    return flag


_is_token_failure = is_token_failure  # shared with the status checker (app.pyro_outcome)


def _to_int_or_none(value) -> Optional[int]:
    try:
        return int(value) if value is not None and str(value).strip() != "" else None
    except (TypeError, ValueError):
        return None


async def _park(reqid, reason, response_text, status_code, pyro_trans_id=None) -> None:
    """Stop retrying a row whose recharge may already have been executed by Pyro."""
    await async_mark_for_reconciliation(
        reqid, reason, response_text, status_code, _to_int_or_none(pyro_trans_id),
    )
    logger.critical(
        "reqid=%s RECONCILIATION REQUIRED [%s]: %s — not retried (Pyro may have executed it)",
        reqid, status_code, reason,
    )


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
            "reconciliation_required": 0,
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
                "reconciliation_required": 0,
                "auth_failed":       True,
            }

    registered = perm_failed = retryable = reconcile = 0
    exhausted_dealers = set()
    processed_reqids = set()
    abort_batch = False
    concurrency = getattr(settings, "recharge_concurrency", 4)
    sem = asyncio.Semaphore(concurrency)

    async def _dispatch_safely(row: dict) -> None:
        """Isolate one row: an exception never escapes into gather() or other rows."""
        nonlocal retryable, reconcile
        progress: dict = {}
        try:
            await _dispatch_single_row(row, progress)
        except Exception as exc:
            reqid = row.get("reqid")
            response = progress.get("response")
            if response is None:
                # Failed before anything was sent to Pyro (e.g. MPIN decrypt / bad row
                # data / DB error): release the claim without burning a retry.
                logger.error("reqid=%s dispatch error before send (%s: %s) -- releasing claim",
                             reqid, type(exc).__name__, exc)
                try:
                    await async_release_unprocessed_claims([reqid])
                except Exception as rel_exc:
                    logger.error("reqid=%s could not release claim: %s", reqid, rel_exc)
                processed_reqids.add(reqid)
                retryable += 1
                return
            # Pyro already answered but recording the result failed: never retry it.
            logger.critical("reqid=%s error AFTER Pyro response (%s: %s) -- parking. response=%s",
                            reqid, type(exc).__name__, exc, json.dumps(response)[:500])
            try:
                await _park(
                    reqid, f"internal error after Pyro response: {type(exc).__name__}: {exc}",
                    json.dumps(response), response.get("statusCode"),
                    (response.get("data") or {}).get("transactionId"),
                )
            except Exception as park_exc:
                logger.critical("reqid=%s could not park after error: %s -- row stays 'P' "
                                "for manual reconciliation", reqid, park_exc)
            reconcile += 1

    async def _dispatch_single_row(row: dict, progress: dict) -> None:
        nonlocal abort_batch, registered, perm_failed, retryable, reconcile

        reqid         = row["reqid"]
        caf           = row["caf_serial_no"]
        gsmno         = row["gsmno"]
        dealer_msisdn = row["vendormsisdn"] or row["ctopup_number"]
        amount        = int(row["frcamt"])
        attempt       = int(row["retry_count"]) + 1

        async with sem:
            if abort_batch:
                return

            mpin = decrypt(row["mpin"], settings.pyro_secret_key)

            if dealer_msisdn in exhausted_dealers:
                # Release without any synthetic Pyro message — dealer skipped, not failed
                logger.warning(
                    "SKIPPED: reqid=%s dealerMsisdn=%s — dealer had insufficient balance earlier in batch",
                    reqid, dealer_msisdn,
                )
                await async_release_unprocessed_claims([reqid])
                processed_reqids.add(reqid)
                retryable += 1
                return

            processed_reqids.add(reqid)
            logger.info("Processing reqid=%s gsmno=%s amount=%s attempt=%s",
                        reqid, gsmno, amount, attempt)

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
            progress["response"] = response

            status_code   = response.get("statusCode")
            response_text = json.dumps(response)
            inner         = response.get("data") or {}
            pyro_msg      = response.get("message")

            if status_code == 2002:
                # Registered -- await callback / status poller
                pyro_trans_id = inner.get("transactionId")
                await async_mark_as_pushed(
                    reqid, pyro_trans_id, response_text,
                    f"dealerMsisdn={dealer_msisdn} destMsisdn={gsmno} amount={amount}",
                    status_code,
                )
                logger.info("reqid=%s registered -- pyroTxnId=%s", reqid, pyro_trans_id)
                registered += 1

            elif status_code == 2000:
                # Synchronous completion (spec §9: "Recharge executed successfully").
                outcome, reconcile_reason = classify_2000(response)
                if outcome == OUTCOME_SUCCESS:
                    await async_mark_as_success(
                        reqid, response_text,
                        inner.get("dealerBalanceBefore"), inner.get("dealerBalanceAfter"),
                        status_code, _to_int_or_none(inner.get("transactionId")),
                    )
                    logger.info("reqid=%s SUCCESS (synchronous 2000) pyroTxnId=%s",
                                reqid, inner.get("transactionId"))
                    registered += 1
                else:
                    await _park(reqid, f"{reconcile_reason}: {pyro_msg}",
                                response_text, status_code, inner.get("transactionId"))
                    reconcile += 1

            elif status_code == -1 and pyro_msg == "Failed to generate action token":
                abort_batch = True
                await async_mark_as_failed(reqid, FLAG_RETRY, pyro_msg,
                                           response_text, status_code, count_attempt=False)
                retryable += 1
                logger.warning(
                    "reqid=%s TRANSIENT: action token unavailable; aborting remainder of batch",
                    reqid,
                )

            elif status_code == -1 and response.get("_request_sent", True):
                # Timeout / transport error / unparseable body after the request was sent:
                # Pyro may have executed it. A retry would only return 5018 and end FAILED.
                await _park(reqid, f"outcome unknown after send: {pyro_msg}",
                            response_text, status_code)
                reconcile += 1

            elif status_code == -1:
                # Never reached Pyro (connect/pool failure): an infrastructure problem,
                # not the row's fault. Retry without burning an attempt and stop the batch.
                abort_batch = True
                await async_mark_as_failed(reqid, FLAG_RETRY, pyro_msg,
                                           response_text, status_code, count_attempt=False)
                retryable += 1
                logger.error("reqid=%s Pyro unreachable (%s) -- aborting remainder of batch",
                             reqid, pyro_msg)

            elif status_code == 5018 or (status_code == 5016 and attempt > 1):
                # 5018: this clientTxnId already exists at Pyro -> an earlier attempt registered.
                # 5016 on a retry: most likely our own earlier attempt went through.
                await _park(reqid, f"[{status_code}] earlier attempt may have succeeded: {pyro_msg}",
                            response_text, status_code)
                reconcile += 1

            elif status_code == 405:
                # Dealer balance exhausted — mark this dealer as done for this batch
                exhausted_dealers.add(dealer_msisdn)
                flag = await _handle_transient(row, reqid, pyro_msg, response_text, status_code)
                if flag == FLAG_FAILED:
                    perm_failed += 1
                else:
                    retryable += 1
                logger.error(
                    "DEALER BALANCE EXHAUSTED: dealerMsisdn=%s reqid=%s — "
                    "remaining rows for this dealer will be skipped this batch run",
                    dealer_msisdn, reqid,
                )

            elif status_code == 506 or _is_token_failure(status_code, pyro_msg):
                abort_batch = True
                logger.error("Invalid token (%s: %s) — triggering re-auth and aborting batch",
                             status_code, pyro_msg)
                await token_manager.authenticate()
                await async_mark_as_failed(reqid, FLAG_RETRY, pyro_msg,
                                           response_text, status_code, count_attempt=False)
                retryable += 1

            elif status_code in (415, 5016):
                # Duplicate subscriber window — IMMEDIATE terminal stop, no retry.
                # Retrying risks double-debiting dealer balance.
                await async_mark_as_failed(reqid, FLAG_FAILED, pyro_msg, response_text, status_code)
                logger.warning("reqid=%s Duplicate window [%s] — terminal stop: %s",
                               reqid, status_code, pyro_msg)
                perm_failed += 1

            elif status_code in PERMANENT_FAILURE_CODES:
                await async_mark_as_failed(reqid, FLAG_FAILED, pyro_msg,
                                           response_text, status_code)
                logger.warning("reqid=%s PERMANENT [%s]: %s", reqid, status_code, pyro_msg)
                perm_failed += 1

            elif status_code in (5001, 5002):
                abort_batch = True
                logger.error("Auth credential error (%s) — triggering re-auth and aborting batch", status_code)
                await token_manager.authenticate()
                await async_mark_as_failed(reqid, FLAG_RETRY, pyro_msg,
                                           response_text, status_code, count_attempt=False)
                retryable += 1

            elif is_dealer_status_failure(status_code, pyro_msg):
                # Dealer blocked / inactive / purged / not authorized (audit M2): retrying
                # cannot succeed, so fail this row now. Other rows are still sent.
                await async_mark_as_failed(reqid, FLAG_FAILED, pyro_msg, response_text, status_code)
                logger.warning("reqid=%s DEALER STATUS [%s] dealerMsisdn=%s: %s",
                               reqid, status_code, dealer_msisdn, pyro_msg)
                perm_failed += 1

            elif status_code in (500, 5000):
                flag = await _handle_transient(row, reqid, pyro_msg, response_text, status_code)
                if flag == FLAG_FAILED:
                    perm_failed += 1
                else:
                    retryable += 1

            else:
                flag = await _handle_transient(row, reqid, pyro_msg, response_text, status_code)
                if flag == FLAG_FAILED:
                    perm_failed += 1
                else:
                    retryable += 1

    try:
        # Every task handles its own errors; return_exceptions makes sure gather only
        # returns once ALL tasks have finished, so the release below never races a
        # task that is still about to send.
        results = await asyncio.gather(*(_dispatch_safely(row) for row in rows),
                                       return_exceptions=True)
        for row, result in zip(rows, results):
            if isinstance(result, Exception):
                logger.critical("reqid=%s unexpected dispatch failure: %r", row.get("reqid"), result)
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
        "reconciliation_required": reconcile,
    }
    logger.info(
        "Processor complete [exec_id=%s][source=%s][zones=%s][mode=%s]: "
        "claimed=%d submitted=%d success=%d transient_failure=%d permanent_failure=%d retry=%d "
        "reconciliation_required=%d",
        summary["execution_id"], summary["source"], summary["zones"], summary["mode"],
        summary["claimed"], summary["submitted"], summary["success"],
        summary["transient_failure"], summary["permanent_failure"], summary["retry"],
        summary["reconciliation_required"],
    )
    return summary
