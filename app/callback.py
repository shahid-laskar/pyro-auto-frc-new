import json
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from app.config import settings
from app.db.postgres import (
    FLAG_FAILED, FLAG_SUCCESS,
    async_find_row_by_client_txn_id,
    async_find_row_by_pyro_trans_id,
    async_insert_txn_log,
    async_mark_as_failed,
    async_mark_as_success,
)

logger = logging.getLogger(__name__)
router = APIRouter()

# Codes that are terminal when received in a callback (stop processing immediately)
_TERMINAL_STATUS_CODES = {5010, 5030, 5005, 5011, 5012, 902}


def _csv_set(value: str) -> set:
    return {part.strip() for part in (value or "").split(",") if part.strip()}


def _client_ip(request: Request) -> Optional[str]:
    peer = request.client.host if request.client else None
    if peer in _csv_set(settings.callback_trusted_proxies):
        forwarded = request.headers.get("x-real-ip") or \
            (request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
        return forwarded or peer
    return peer


def _to_int(value) -> Optional[int]:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _values_match(expected, actual) -> bool:
    """Strict: a value we expect must be present in the callback and equal."""
    if expected is None:
        return True
    if actual is None:
        return False
    return str(expected).strip() == str(actual).strip()


def _amount_matches(expected, actual) -> bool:
    if expected is None:
        return True
    if actual is None:
        return False
    try:
        return float(expected) == float(actual)
    except (TypeError, ValueError):
        return False


def _validate(row: dict, data: dict, pyro_txn_id_int: int) -> list:
    errors = []
    expected_client = row.get("client_txn_id") or str(row["reqid"]).zfill(5)[:15]
    if not _values_match(expected_client, data.get("clientTxnId")):
        errors.append(f"clientTxnId mismatch expected={expected_client} got={data.get('clientTxnId')}")
    if not _values_match(row.get("gsmno"), data.get("destMsisdn")):
        errors.append(f"destMsisdn mismatch expected={row.get('gsmno')} got={data.get('destMsisdn')}")
    if not _amount_matches(row.get("frcamt"), data.get("amount")):
        errors.append(f"amount mismatch expected={row.get('frcamt')} got={data.get('amount')}")
    stored_txn = row.get("pyro_trans_id")
    if stored_txn is not None and _to_int(stored_txn) != pyro_txn_id_int:
        errors.append(f"transactionId mismatch expected={stored_txn} got={pyro_txn_id_int}")
    return errors


@router.post("/callback/recharge")
async def recharge_callback(request: Request):
    """
    Callback URL for Pyro:
        POST https://smpyrogateway.bsnl.co.in/api/callback/recharge

    """
    allowed = _csv_set(settings.callback_allowed_ips)
    if allowed:
        ip = _client_ip(request)
        if ip not in allowed:
            logger.warning("Callback REJECTED from non-allow-listed source ip=%s", ip)
            return JSONResponse(status_code=403, content={"received": False, "note": "forbidden"})

    try:
        body = await request.json()
    except Exception:
        raw = await request.body()
        logger.error("Callback: invalid JSON: %s", raw[:200])
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    if not isinstance(body, dict):
        logger.error("Callback: body is not a JSON object: %s", str(body)[:200])
        raise HTTPException(status_code=400, detail="Invalid callback body")

    logger.info("Callback received: %s", json.dumps(body)[:300])

    data = body.get("data") or {}
    if not isinstance(data, dict):
        logger.error("Callback: 'data' is not an object: %s", str(data)[:200])
        raise HTTPException(status_code=400, detail="Invalid callback data")

    raw_status    = body.get("statusCode")
    status_code   = _to_int(raw_status) if raw_status is not None else None
    pyro_txn_id   = data.get("transactionId")
    client_txn_id = data.get("clientTxnId")
    received_at   = datetime.now(timezone.utc)

    if pyro_txn_id is None or str(pyro_txn_id).strip() == "":
        logger.error("Callback: missing transactionId: %s", body)
        return {"received": True, "note": "missing transactionId"}
    pyro_txn_id_int = _to_int(pyro_txn_id)
    if pyro_txn_id_int is None:
        logger.error("Callback: invalid transactionId: %s", pyro_txn_id)
        return {"received": True, "note": "invalid transactionId"}

    row = await async_find_row_by_pyro_trans_id(pyro_txn_id_int)
    if not row and client_txn_id is not None:
        # transactionId not stored yet (callback raced mark_as_pushed, or the submit
        # timed out and the row was parked): fall back to our own clientTxnId.
        row = await async_find_row_by_client_txn_id(client_txn_id)
        if row:
            logger.info("Callback: pyroTxnId=%s matched reqid=%s via clientTxnId=%s",
                        pyro_txn_id, row["reqid"], client_txn_id)
    if not row:
        logger.warning("Callback: no row for pyroTxnId=%s clientTxnId=%s", pyro_txn_id, client_txn_id)
        return {"received": True, "note": "transaction not found"}

    reqid        = row["reqid"]
    caf          = row["caf_serial_no"]
    gsmno        = row.get("gsmno", "")
    batch_date   = row.get("batch_date")
    current_flag = row["push_flag"]
    body_text    = json.dumps(body)
    is_success   = status_code == 2000 and data.get("status") == "SUCCESS"

    validation_errors = _validate(row, data, pyro_txn_id_int)
    if validation_errors:
        logger.warning(
            "Callback validation failed: reqid=%s pyroTxnId=%s errors=%s",
            reqid, pyro_txn_id, validation_errors,
        )
        await async_insert_txn_log(
            frc_reqid=reqid, caf_serial_no=caf, gsmno=gsmno,
            batch_date=batch_date,
            client_txn_id=str(client_txn_id) if client_txn_id is not None else None,
            api_stage="CALLBACK_RECV", api_endpoint=None, http_method=None,
            attempt_no=1, request_headers=None, request_body=None,
            response_http_code=422, response_body=body_text,
            pyro_status_code=status_code, pyro_status_text=data.get("status"),
            pyro_txn_id=pyro_txn_id_int,
            call_started_at=received_at, call_ended_at=received_at, duration_ms=0,
            is_success="N", is_perm_failure="N",
            error_class="CallbackValidationError",
            error_detail="; ".join(validation_errors),
        )
        return {"received": True, "note": "callback validation failed"}

    # Idempotency fast path (the UPDATEs below are guarded as well):
    #  - SUCCESS is final;
    #  - a definitive failure is final unless Pyro now reports SUCCESS;
    #  - a row parked for reconciliation (F, final_status NULL) is resolved by any final callback.
    if current_flag == FLAG_SUCCESS or (
        current_flag == FLAG_FAILED and row.get("final_status") is not None and not is_success
    ):
        logger.info("Callback: reqid=%s already terminal (%s/%s) -- ignored",
                    reqid, current_flag, row.get("final_status"))
        return {"received": True, "note": "already processed"}

    await async_insert_txn_log(
        frc_reqid=reqid, caf_serial_no=caf, gsmno=gsmno,
        batch_date=batch_date,
        client_txn_id=str(client_txn_id) if client_txn_id is not None else None,
        api_stage="CALLBACK_RECV", api_endpoint=None, http_method=None,
        attempt_no=1, request_headers=None, request_body=None,
        response_http_code=200, response_body=body_text,
        pyro_status_code=status_code, pyro_status_text=data.get("status"),
        pyro_txn_id=pyro_txn_id_int,
        call_started_at=received_at, call_ended_at=received_at, duration_ms=0,
        is_success="Y" if status_code == 2000 else "N",
        is_perm_failure="Y" if status_code not in (2000, 2002) else "N",
    )

    if is_success:
        if current_flag == FLAG_FAILED:
            logger.warning(
                "Callback SUCCESS for reqid=%s previously %s (final_status=%s, remarks=%s) -- "
                "recording success", reqid, current_flag, row.get("final_status"), row.get("push_remarks"),
            )
        balance_before = data.get("dealerBalanceBefore", 0.0)
        balance_after  = data.get("dealerBalanceAfter", 0.0)
        updated = await async_mark_as_success(
            reqid, body_text, balance_before, balance_after, status_code, pyro_txn_id_int,
        )
        if updated == 0:
            logger.info("Callback SUCCESS: reqid=%s already final -- unchanged", reqid)
        else:
            logger.info("Callback SUCCESS: reqid=%s pyroTxnId=%s gsmno=%s amount=%s bal_after=%s",
                        reqid, pyro_txn_id, data.get("destMsisdn"),
                        data.get("amount"), balance_after)

    elif status_code == 2002:
        logger.info("Callback: reqid=%s still IN_PROCESS (2002) -- awaiting final callback", reqid)

    elif status_code in _TERMINAL_STATUS_CODES:
        # 5010 = "Only First Recharge accounts allowed" (already FRC done at switch)
        # Stop immediately — do not retry
        pyro_msg = body.get("message")
        await async_mark_as_failed(reqid, FLAG_FAILED, pyro_msg, body_text, status_code)
        logger.warning("Callback TERMINAL [%s]: reqid=%s msg=%s", status_code, reqid, pyro_msg)

    else:
        pyro_msg = body.get("message")
        await async_mark_as_failed(reqid, FLAG_FAILED, pyro_msg, body_text, status_code)
        logger.warning("Callback FAILURE [%s]: reqid=%s msg=%s", status_code, reqid, pyro_msg)

    return {"received": True}
