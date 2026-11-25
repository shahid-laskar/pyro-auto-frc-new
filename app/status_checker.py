
import json
import logging

from app.db.postgres import (
    FLAG_FAILED,
    async_fetch_pushed_rows_for_status_check,
    async_mark_as_failed,
    async_mark_as_success,
    async_mark_for_reconciliation,
    async_park_stale_pushed_rows,
    async_release_orphaned_claims,
    async_update_status_check_attempt,
    orphan_claim_age_minutes,
)
from app.callback import validate_txn_data
from app.pyro_client import check_transaction_status
from app.auth.token_manager import token_manager
from app.pyro_outcome import (
    OUTCOME_RECONCILE, OUTCOME_SUCCESS, TERMINAL_FAILURE_CODES, classify_2000, is_token_failure,
)
from app.config import settings

logger = logging.getLogger(__name__)

# Codes that are terminal when received from status check (stop polling immediately)
_TERMINAL_STATUS_CODES = TERMINAL_FAILURE_CODES

# Pushed rows still without a final status after this long are parked for
# reconciliation (they stay resolvable by a late callback).
STALE_PUSH_MINUTES = 60


def _to_int(value):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


async def run_status_checks() -> dict:
    """Check status of pushed-but-no-callback rows.

    Every state change is a guarded UPDATE, so a callback that finalises a row while
    this loop is running is never overwritten.
    """
    parked = 0
    try:
        age = orphan_claim_age_minutes()
        orphans = await async_release_orphaned_claims(age)
        if orphans:
            logger.warning(
                "Status checker: released %d orphaned claim(s) (claimed > %d min ago, never pushed): %s",
                len(orphans), age, orphans[:50],
            )
    except Exception as exc:
        logger.error("Status checker: orphaned-claim sweep failed: %s", exc)

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
                "parked": parked, "transport_error": 0, "token_error": 0}

    logger.info("Status checker: checking %d row(s)", len(rows))
    success = failed = retry = transport_error = token_error = 0

    for idx, row in enumerate(rows):
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

        if is_token_failure(status_code, pyro_msg):
            # Pyro rejected our token, not the transaction (H1): do not use up an
            # attempt. Stop this run -- every remaining row would fail the same way,
            # and refreshing/logging in per row risks a vendor lockout (H3).
            # The next run starts with a refreshed (or re-authenticated) token.
            token_manager.invalidate_access_token()
            token_error += 1
            logger.error(
                "reqid=%s STATUS CHECK token rejected [%s] %s -- not counted; "
                "stopping this run (%d row(s) left for the next run)",
                reqid, status_code, pyro_msg, len(rows) - idx - 1,
            )
            break

        await async_update_status_check_attempt(reqid)

        # statusCode 2000 decides success; a present-but-contradicting status parks the row (C1).
        outcome, reconcile_reason = classify_2000(response) if status_code == 2000 else (None, None)

        txn_errors = []
        if outcome == OUTCOME_SUCCESS:
            txn_id = _to_int(inner.get("transactionId"))
            txn_errors = validate_txn_data(row, inner, txn_id if txn_id is not None else _to_int(pyro_trans_id))

        if outcome == OUTCOME_RECONCILE:
            await async_mark_for_reconciliation(
                reqid, f"status check {reconcile_reason}", response_text, status_code,
            )
            logger.critical("reqid=%s STATUS CHECK %s -- parked", reqid, reconcile_reason)
            parked += 1

        elif status_code == 2000 and txn_errors:
            await async_mark_for_reconciliation(
                reqid, f"status check SUCCESS but data mismatch: {'; '.join(txn_errors)}",
                response_text, status_code,
            )
            logger.critical("reqid=%s STATUS CHECK SUCCESS with mismatched data %s -- parked",
                            reqid, txn_errors)
            parked += 1

        elif status_code == 2000:
            updated = await async_mark_as_success(
                reqid, response_text,
                inner.get("dealerBalanceBefore"), inner.get("dealerBalanceAfter"),
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
               "parked": parked, "transport_error": transport_error,
               "token_error": token_error}
    logger.info("Status checker complete -- %s", summary)
    return summary
