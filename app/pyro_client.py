
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx

from app.auth.token_manager import token_manager
from app.config import settings
from app.db.postgres import (
    PERMANENT_FAILURE_CODES,
    async_insert_txn_log,
)
from app.encryption import decrypt, encrypt

logger = logging.getLogger(__name__)

STATUS_SUCCESS    = 2000
STATUS_REGISTERED = 2002


def _parse_pyro_response(resp: httpx.Response, label: str) -> dict:
    
    raw = resp.text.strip()
    logger.debug("%s raw response: %s", label, raw[:200])

    try:
        return json.loads(decrypt(raw, settings.pyro_secret_key))
    except Exception as dec_err:
        logger.debug("%s: decrypt failed (%s) -- trying plain JSON", label, dec_err)

    try:
        return resp.json()
    
    except Exception as json_err:
        logger.error("%s: both plain JSON and decrypt failed. Raw: %s", label, raw[:300])
        return {
            "statusCode": -1,
            "status": "ERROR",
            "message": f"Response parse failed: {json_err}",
        }


def _mask_body(body: dict) -> str:
    """Mask sensitive fields before logging request body."""
    masked = body.copy()
    if "mpin"     in masked: masked["mpin"]     = "***"
    if "password" in masked: masked["password"] = "***"
    return json.dumps(masked)


async def _log(
    reqid:              int,
    caf_serial_no:      str,
    gsmno:              str,
    batch_date,
    client_txn_id:      Optional[str],
    api_stage:          str,
    api_endpoint:       str,
    http_method:        str,
    attempt_no:         int,
    request_headers:    Optional[str],
    request_body:       Optional[str],
    response_http_code: Optional[int],
    response_body:      Optional[str],
    pyro_status_code:   Optional[int],
    pyro_status_text:   Optional[str],
    pyro_txn_id:        Optional[int],
    call_started_at:    datetime,
    call_ended_at:      Optional[datetime],
    duration_ms:        Optional[int],
    is_success:         str,
    is_perm_failure:    str = "N",
    error_class:        Optional[str] = None,
    error_detail:       Optional[str] = None,
) -> None:
    """Insert one row into frc_txn_log. Never raises -- log failures are non-fatal."""
    await async_insert_txn_log(
        frc_reqid=reqid,
        caf_serial_no=caf_serial_no,
        gsmno=gsmno,
        batch_date=batch_date,
        client_txn_id=client_txn_id,
        api_stage=api_stage,
        api_endpoint=api_endpoint,
        http_method=http_method,
        attempt_no=attempt_no,
        request_headers=request_headers,
        request_body=request_body,
        response_http_code=response_http_code,
        response_body=response_body,
        pyro_status_code=pyro_status_code,
        pyro_status_text=pyro_status_text,
        pyro_txn_id=pyro_txn_id,
        call_started_at=call_started_at,
        call_ended_at=call_ended_at,
        duration_ms=duration_ms,
        is_success=is_success,
        is_perm_failure=is_perm_failure,
        error_class=error_class,
        error_detail=error_detail,
    )


async def recharge(
    reqid:         int,
    caf_serial_no: str,
    gsmno:         str,
    batch_date,
    dealer_msisdn: str,
    dest_msisdn:   str,
    amount:        int,
    mpin:          str,
    attempt_no:    int = 1,
) -> dict:
    
    client_txn_id = str(reqid).zfill(5)[:15]
    url           = f"{settings.pyro_base_url}/epin-vendor-api/recharge"
    started_at    = datetime.now(timezone.utc)

    # Generate fresh action token immediately before POST
    try:
        action_token = await token_manager.get_action_token()
    except Exception as exc:
        action_token = None
        logger.error("Failed to generate action token: %s", exc)

    if not action_token:        
        ended_at = datetime.now(timezone.utc)
        duration = int((ended_at - started_at).total_seconds() * 1000)
        data = {"statusCode": -1, "status": "ERROR",
                "message": "Failed to generate action token"}
        await _log(
            reqid, caf_serial_no, gsmno, batch_date, client_txn_id,
            "RECHARGE", url, "POST", attempt_no,
            None, None, None, json.dumps(data),
            -1, "ERROR", None,
            started_at, ended_at, duration,
            "N", "N", None, "Action token generation failed",
        )
        return data

    payload = {
        "dealerMsisdn": dealer_msisdn,
        "destMsisdn":   dest_msisdn,
        "amount":       amount,
        "clientTxnId":  client_txn_id,
        "mpin":         mpin,
    }
    encrypted_body = encrypt(json.dumps(payload), settings.pyro_secret_key)

    headers = {
        "apiKey":       settings.pyro_api_key,
        "sessionToken": token_manager.session_token,
        "accessToken":  token_manager.access_token,
        "actionToken":  action_token,
    }
    masked_headers = json.dumps({k: "***" for k in headers})

    try:
        async with httpx.AsyncClient(verify=True, timeout=30.0) as client:
            resp = await client.post(url, headers=headers, content=encrypted_body)

        ended_at    = datetime.now(timezone.utc)
        duration    = int((ended_at - started_at).total_seconds() * 1000)
        data        = _parse_pyro_response(resp, f"RECHARGE reqid={reqid}")
        status_code = data.get("statusCode", -1)
        is_success  = "Y" if status_code in (STATUS_SUCCESS, STATUS_REGISTERED) else "N"
        is_perm     = "Y" if status_code in PERMANENT_FAILURE_CODES else "N"
        pyro_txn_id = data.get("data", {}).get("transactionId")

        await _log(
            reqid, caf_serial_no, gsmno, batch_date, client_txn_id,
            "RECHARGE", url, "POST", attempt_no,
            masked_headers, _mask_body(payload),
            resp.status_code, json.dumps(data),
            status_code, data.get("status"), pyro_txn_id,
            started_at, ended_at, duration,
            is_success, is_perm, None, None,
        )

        logger.info("RECHARGE reqid=%s clientTxnId=%s statusCode=%s duration=%dms",
                    reqid, client_txn_id, status_code, duration)
        return data

    except httpx.TimeoutException as exc:
        ended_at = datetime.now(timezone.utc)
        duration = int((ended_at - started_at).total_seconds() * 1000)
        await _log(
            reqid, caf_serial_no, gsmno, batch_date, client_txn_id,
            "RECHARGE", url, "POST", attempt_no,
            masked_headers, _mask_body(payload),
            None, None, -1, "TIMEOUT", None,
            started_at, ended_at, duration,
            "N", "N", "httpx.TimeoutException", str(exc),
        )
        logger.error("RECHARGE timed out reqid=%s", reqid)
        return {"statusCode": -1, "status": "ERROR", "message": "Request timed out"}

    except Exception as exc:
        ended_at = datetime.now(timezone.utc)
        duration = int((ended_at - started_at).total_seconds() * 1000)
        await _log(
            reqid, caf_serial_no, gsmno, batch_date, client_txn_id,
            "RECHARGE", url, "POST", attempt_no,
            masked_headers, _mask_body(payload),
            None, None, -1, "ERROR", None,
            started_at, ended_at, duration,
            "N", "N", type(exc).__name__, str(exc),
        )
        logger.error("RECHARGE error reqid=%s: %s", reqid, exc)
        return {"statusCode": -1, "status": "ERROR", "message": str(exc)}


async def check_transaction_status(
    reqid:         int,
    caf_serial_no: str,
    gsmno:         str,
    batch_date,
    pyro_trans_id: str,
    attempt_no:    int = 1,
) -> dict:

    client_txn_id  = str(reqid).zfill(5)[:15]
    url            = f"{settings.pyro_base_url}/epin-vendor-api/transaction-status"
    started_at     = datetime.now(timezone.utc)

    payload        = {"transactionId": str(pyro_trans_id), "clientTxnId": client_txn_id}
    encrypted_body = encrypt(json.dumps(payload), settings.pyro_secret_key)
    access_token   = await token_manager.get_access_token()
    headers        = {"apiKey": settings.pyro_api_key, "accessToken": access_token}
    masked_headers = json.dumps({"apiKey": "***", "accessToken": "***"})

    try:
        async with httpx.AsyncClient(verify=True, timeout=30.0) as client:
            resp = await client.post(url, headers=headers, content=encrypted_body)

        ended_at    = datetime.now(timezone.utc)
        duration    = int((ended_at - started_at).total_seconds() * 1000)
        data        = _parse_pyro_response(resp, f"STATUS_CHECK reqid={reqid}")
        status_code = data.get("statusCode", -1)
        is_success  = "Y" if status_code == STATUS_SUCCESS else "N"
        is_perm     = "Y" if status_code == 902 else "N"

        await _log(
            reqid, caf_serial_no, gsmno, batch_date, client_txn_id,
            "STATUS_CHECK", url, "POST", attempt_no,
            masked_headers, json.dumps(payload),
            resp.status_code, json.dumps(data),
            status_code, data.get("status"), int(pyro_trans_id),
            started_at, ended_at, duration,
            is_success, is_perm, None, None,
        )

        logger.info("STATUS_CHECK reqid=%s pyroTxnId=%s statusCode=%s duration=%dms",
                    reqid, pyro_trans_id, status_code, duration)
        return data

    except Exception as exc:
        ended_at = datetime.now(timezone.utc)
        duration = int((ended_at - started_at).total_seconds() * 1000)
        await _log(
            reqid, caf_serial_no, gsmno, batch_date, client_txn_id,
            "STATUS_CHECK", url, "POST", attempt_no,
            masked_headers, json.dumps(payload),
            None, None, -1, "ERROR", None,
            started_at, ended_at, duration,
            "N", "N", type(exc).__name__, str(exc),
        )
        logger.error("STATUS_CHECK error reqid=%s: %s", reqid, exc)
        return {"statusCode": -1, "status": "ERROR", "message": str(exc)}