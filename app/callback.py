import asyncio
import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request

from app.db.oracle import BCD_STATUS_F, BCD_STATUS_P,BCD_STATUS_ID, update_bcd_status, bcd_status_for_pyro_failure
from app.db.postgres import (
    FLAG_FAILED, FLAG_SUCCESS, INVALID_DATA_CODES,
    async_find_row_by_pyro_trans_id,
    async_insert_txn_log,
    async_mark_as_failed,
    async_mark_as_success,
)

logger = logging.getLogger(__name__)
router = APIRouter()


def _values_match(expected, actual) -> bool:
    if expected is None or actual is None:
        return True
    return str(expected).strip() == str(actual).strip()

def _amount_matches(expected, actual) -> bool:
    if expected is None or actual is None:
        return True
    try:
        return float(expected) == float(actual)
    except (TypeError, ValueError):
        return False
    
@router.post("/callback/recharge")
async def recharge_callback(request: Request):
    """
    Callback URL for Pyro:
        POST https://smpyrogateway.bsnl.co.in/api/callback/recharge
    
    """
    try:
        body = await request.json()
    except Exception:
        raw = await request.body()
        logger.error("Callback: invalid JSON: %s", raw[:200])
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    logger.info("Callback received: %s", json.dumps(body)[:300])

    data          = body.get("data", {})
    status_code   = body.get("statusCode")
    pyro_txn_id   = data.get("transactionId")
    client_txn_id = data.get("clientTxnId")
    received_at   = datetime.now(timezone.utc)

    if not pyro_txn_id:
        logger.error("Callback: missing transactionId: %s", body)
        return {"received": True, "note": "missing transactionId"}
    try:
        pyro_txn_id_int = int(pyro_txn_id)
    except (TypeError, ValueError):
        logger.error("Callback: invalid transactionId: %s", pyro_txn_id)
        return {"received": True, "note": "invalid transactionId"}
    
    row = await async_find_row_by_pyro_trans_id(pyro_txn_id_int)
    if not row:
        logger.warning("Callback: no row for pyroTxnId=%s", pyro_txn_id)
        return {"received": True, "note": "transaction not found"}

    reqid        = row["reqid"]
    caf          = row["caf_serial_no"]
    gsmno        = row.get("gsmno", "")
    batch_date   = row.get("batch_date")
    current_flag = row["push_flag"]
    body_text    = json.dumps(body)

    validation_errors = []
    if not _values_match(row.get("client_txn_id"), client_txn_id):
        validation_errors.append(
            f"clientTxnId mismatch expected={row.get('client_txn_id')} got={client_txn_id}"
        )
    if not _values_match(row.get("gsmno"), data.get("destMsisdn")):
        validation_errors.append(
            f"destMsisdn mismatch expected={row.get('gsmno')} got={data.get('destMsisdn')}"
        )
    if not _amount_matches(row.get("frcamt"), data.get("amount")):
        validation_errors.append(
            f"amount mismatch expected={row.get('frcamt')} got={data.get('amount')}"
        )

    if validation_errors:
        logger.warning(
            "Callback validation failed: reqid=%s pyroTxnId=%s errors=%s",
            reqid, pyro_txn_id, validation_errors,
        )
        await async_insert_txn_log(
            frc_reqid=reqid, caf_serial_no=caf, gsmno=gsmno,
            batch_date=batch_date,
            client_txn_id=str(client_txn_id) if client_txn_id else None,
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

    if current_flag in (FLAG_SUCCESS, FLAG_FAILED):
        logger.info("Callback: reqid=%s already terminal (%s) -- ignored",
                    reqid, current_flag)
        return {"received": True, "note": "already processed"}

    await async_insert_txn_log(
        frc_reqid=reqid, caf_serial_no=caf, gsmno=gsmno,
        batch_date=batch_date,
        client_txn_id=str(client_txn_id) if client_txn_id else None,
        api_stage="CALLBACK_RECV", api_endpoint=None, http_method=None,
        attempt_no=1, request_headers=None, request_body=None,
        response_http_code=200, response_body=body_text,
        pyro_status_code=status_code, pyro_status_text=data.get("status"),
        pyro_txn_id=pyro_txn_id_int,
        call_started_at=received_at, call_ended_at=received_at, duration_ms=0,
        is_success="Y" if status_code == 2000 else "N",
        is_perm_failure="Y" if status_code not in (2000, 2002) else "N",
    )

    if status_code == 2000 and data.get("status") == "SUCCESS":
        balance_before = data.get("dealerBalanceBefore", 0.0)
        balance_after = data.get("dealerBalanceAfter", 0.0)
        await async_mark_as_success(reqid, body_text, balance_before, balance_after, status_code)
       
        await asyncio.to_thread(
            update_bcd_status, caf, reqid, BCD_STATUS_P,
            f"Recharge successful via callback. pyroTxnId={pyro_txn_id} "
            f"gsmno={data.get('destMsisdn')} amount={data.get('amount')}"
        )
        logger.info("Callback SUCCESS: reqid=%s pyroTxnId=%s gsmno=%s amount=%s bal_after=%s",
                    reqid, pyro_txn_id, data.get("destMsisdn"),
                    data.get("amount"), balance_after)
    elif status_code == 2002:
        logger.info("Callback: reqid=%s still IN_PROCESS (2002) -- awaiting final callback", reqid)
    else:
        remarks = f"[{status_code}] {body.get('message', 'Callback failure')}"
        await async_mark_as_failed(reqid, FLAG_FAILED, remarks, body_text, status_code)
        bcd_status = bcd_status_for_pyro_failure(status_code)
        await asyncio.to_thread(
            update_bcd_status, caf, reqid, bcd_status, remarks
        )
        logger.warning("Callback FAILURE: reqid=%s remarks=%s", reqid, remarks)

    return {"received": True}
