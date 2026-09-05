import asyncio
import functools
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Generator, List, Optional

import psycopg2
import psycopg2.pool
import psycopg2.extras

from app.config import settings

logger = logging.getLogger(__name__)

def _pg_retry(fn):
    """Retry a synchronous DB function once on OperationalError.

    When a pooled connection goes stale, get_pg_conn discards it and raises
    OperationalError. A single retry is enough — the pool always creates a
    fresh connection for the second attempt.
    """
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except psycopg2.OperationalError as exc:
            logger.warning(
                "Postgres: %s failed with OperationalError (%s) — retrying once",
                fn.__name__, exc,
            )
            return fn(*args, **kwargs)
    return wrapper

_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None

# push_flag state constants
FLAG_PENDING = "N"
FLAG_PUSHED  = "P"
FLAG_SUCCESS = "Y"
FLAG_FAILED  = "F"
FLAG_RETRY   = "E"

# Pyro codes -> permanent failure (no auto-retry)
PERMANENT_FAILURE_CODES = {406, 505, 5006, 5007, 5011, 5012, 5030}
# Subset: invalid data errors -> BCD status 'ID'
INVALID_DATA_CODES = {5006, 5011, 5012, 5030}


# ── Pool lifecycle ─────────────────────────────────────────────────────────────

def init_pg_pool() -> None:
    global _pool
    _pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=settings.pg_min_conn,
        maxconn=settings.pg_max_conn,
        host=settings.pg_host,
        port=settings.pg_port,
        database=settings.pg_database,
        user=settings.pg_user,
        password=settings.pg_password,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    logger.info("Postgres pool initialised (min=%d max=%d)",
                settings.pg_min_conn, settings.pg_max_conn)


def close_pg_pool() -> None:
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None
        logger.info("Postgres pool closed")


@contextmanager
def get_pg_conn() -> Generator:
    conn = _pool.getconn()
    # The pool has no built-in liveness check — swap out any connection the
    # server dropped while it was idle (presents as conn.closed != 0).
    if conn.closed:
        logger.warning("Postgres: stale connection detected on checkout — replacing")
        _pool.putconn(conn, close=True)
        conn = _pool.getconn()
    discard = False
    try:
        yield conn
        conn.commit()
    except Exception as exc:        
        discard = isinstance(exc, psycopg2.OperationalError) or bool(conn.closed)
        try:
            conn.rollback()
        except Exception:
            discard = True  # rollback itself failed — connection is unusable
            logger.warning("Postgres: rollback failed — discarding connection")
        raise
    finally:
        _pool.putconn(conn, close=discard)
        if discard:
            logger.warning("Postgres: broken connection discarded from pool")
    


# ── Source data queries (batch population) ────────────────────────────────────
@_pg_retry
def fetch_cos_bcd_for_gsms(gsm_numbers: List[str]) -> List[dict]:
   
    if not gsm_numbers:
        return []

    sql = """
        -- ── EKYC branch (cos_bcd) ─────────────────────────────────────────────
        SELECT
            cb.gsmnumber,
            cb.caf_serial_no,
            cb.de_csccode,
            cb.circle_code::TEXT AS circle_code,
            cb.live_photo_time                  AS live_photo_time,
            cb.frc_plan_name                    AS frc_plan_name,
            cb.frc_plan_code                    AS frc_plan_code,
            cb.frc_category_code                AS frc_category_code,
            fp.frc_amount                       AS frcamt,
            cb.frc_ctopup_number                AS ctopup_number,
            cb.frc_ctopup_number_mpin           AS mpin_raw,
            cm.pos_unique_code                  AS vendorid,
            cm.ctopupno                         AS vendormsisdn,
            'EKYC'                              AS kyc_mode 
        FROM public.cos_bcd cb
        JOIN public.ctop_master cm
            ON cm.ctopupno = cb.frc_ctopup_number
        JOIN public.frc_plan_table fp
            ON fp.plan_code = cb.frc_plan_code
           AND (fp.circle_code = cb.circle_code::TEXT OR fp.circle_code = '9999')
           AND (fp.end_date IS NULL OR fp.end_date >= CURRENT_DATE)
        WHERE cb.gsmnumber = ANY(%(gsms)s)
          AND cb.frc_plan_name          IS NOT NULL
          AND cb.frc_plan_code          IS NOT NULL
          AND cb.frc_category_code      IS NOT NULL
          AND cb.frc_ctopup_number      IS NOT NULL
          AND cb.frc_ctopup_number_mpin IS NOT NULL

        UNION ALL

        -- ── DKYC branch (cos_bcd_dkyc) ────────────────────────────────────────
        -- Differences vs EKYC:
        --   live_photo_time  <- customer_photo_time
        --   ctopup_number    <- parent_ctopup_number
        --   mpin_raw         <- mpin  (stored directly in table)
        --   plan join        <- plan_name = tariff_plan  (not plan_code)
        --   circle match     <- exact only  (no '9999' fallback)
        --   frc_plan_code    <- fp.plan_code  (from plan table, not in dkyc table)
        --   frc_category_code <- fp.category_code  (from plan table)
        SELECT
            cb.gsmnumber,
            cb.caf_serial_no,
            cb.de_csccode,            
            cb.circle_code::TEXT AS circle_code,
            cb.customer_photo_time              AS live_photo_time,
            fp.plan_name                        AS frc_plan_name,
            fp.plan_code                        AS frc_plan_code,
            fp.category_code                    AS frc_category_code,
            fp.frc_amount                       AS frcamt,
            cb.parent_ctopup_number             AS ctopup_number,
            cb.mpin                             AS mpin_raw,
            cm.pos_unique_code                  AS vendorid,
            cm.ctopupno                         AS vendormsisdn,
            'DKYC'                              AS kyc_mode
        FROM public.cos_bcd_dkyc cb
        JOIN public.ctop_master cm
            ON cm.ctopupno = cb.parent_ctopup_number
        JOIN public.frc_plan_table fp
            ON fp.plan_name  = cb.tariff_plan
           AND fp.circle_code = cb.circle_code::TEXT
           AND (fp.end_date IS NULL OR fp.end_date >= CURRENT_DATE)
        WHERE cb.gsmnumber = ANY(%(gsms)s)
          AND cb.tariff_plan            IS NOT NULL
          AND cb.parent_ctopup_number   IS NOT NULL
          AND cb.mpin                   IS NOT NULL
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, {"gsms": gsm_numbers})
            rows = [dict(r) for r in cur.fetchall()]

    ekyc_count = sum(1 for r in rows if r["kyc_mode"] == "EKYC")
    dkyc_count = sum(1 for r in rows if r["kyc_mode"] == "DKYC")
    logger.info(
        "cos_bcd join: %d/%d GSMs matched (EKYC=%d DKYC=%d)",
        len(rows), len(gsm_numbers), ekyc_count, dkyc_count,
    )
    return rows

@_pg_retry
def bulk_insert_frc_requests(rows: List[dict]) -> List[dict]:
    
    if not rows:
        return []

    sql = """
        INSERT INTO public.frc_pyro_request_data (
            caf_serial_no, gsmno, csccode, circle_code,
            edate, reqdate,
            frc_plan_name, frc_plan_code, frc_category_code, frcamt,
            ctopup_number, vendormsisdn, vendorid,
            mpin, mpin_length,max_retries,
            kyc_mode,
            in_status, pyro_status, push_flag,
            batch_date, created_at
        ) VALUES (
            %(caf_serial_no)s, %(gsmno)s, %(csccode)s, %(circle_code)s,
            %(edate)s, %(reqdate)s,
            %(frc_plan_name)s, %(frc_plan_code)s, %(frc_category_code)s, %(frcamt)s,
            %(ctopup_number)s, %(vendormsisdn)s, %(vendorid)s,
            %(mpin)s, %(mpin_length)s, %(max_retries)s,
            %(kyc_mode)s,
            'C', 'N', 'N',
            CURRENT_DATE, CURRENT_TIMESTAMP
        )
        ON CONFLICT (batch_date, caf_serial_no) DO NOTHING
        RETURNING reqid, caf_serial_no
    """
    inserted_pairs = []
    with get_pg_conn() as conn: 
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(sql, row)
                result = cur.fetchone()
                if result:
                    inserted_pairs.append({
                        "reqid":         result[0],
                        "caf_serial_no": result[1],
                    })

    logger.info("Postgres: inserted %d/%d rows into frc_pyro_request_data",
                len(inserted_pairs), len(rows))
    return inserted_pairs


# ── Recharge state machine ─────────────────────────────────────────────────────
@_pg_retry
def fetch_pending_rows(batch_size: int = 500) -> List[dict]:
    sql = """
        SELECT
            reqid, caf_serial_no, gsmno, batch_date, kyc_mode,
            vendormsisdn, ctopup_number, frcamt, mpin, mpin_length,
            push_flag, retry_count, max_retries, client_txn_id
        FROM public.frc_pyro_request_data
        WHERE in_status   = 'C'
          AND push_flag   IN ('N', 'E')
          AND retry_count <= max_retries
        ORDER BY created_at ASC
        LIMIT %s
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (batch_size,))
            return [dict(r) for r in cur.fetchall()]

@_pg_retry
def mark_as_pushed(reqid: int, pyro_trans_id: int, response_text: str,
                   msg2pyro: str, initial_statuscode: int) -> None:
    client_txn_id = str(reqid).zfill(5)[:15]
    sql = """
        UPDATE public.frc_pyro_request_data
        SET
            push_flag               = 'P',
            push_date               = CURRENT_TIMESTAMP,
            push_remarks            = 'Submitted to Pyro - awaiting callback',
            pyro_trans_id           = %s,
            pyro_initial_statuscode = %s,
            submitted_at            = CURRENT_TIMESTAMP,
            status_check_eligible_at   = CURRENT_TIMESTAMP + INTERVAL '45 seconds',
            msg2pyro                = %s,
            msg_afterreq            = %s,
            pyro_status             = 'REG',
            client_txn_id           = %s,
            updated_ts              = CURRENT_TIMESTAMP
        WHERE reqid = %s
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (pyro_trans_id, initial_statuscode,
                              msg2pyro, response_text, client_txn_id, reqid))

@_pg_retry
def mark_as_success(reqid: int, response_text: str,balance_before: float,
                    balance_after: float, final_statuscode: int) -> None:
    sql = """
        UPDATE public.frc_pyro_request_data
        SET
            push_flag               = 'Y',
            final_status            = 'SUCCESS',
            pyro_status             = 'SUC',
            pyro_final_statuscode   = %s,
            completed_at            = CURRENT_TIMESTAMP,
            callback_received_at    = CURRENT_TIMESTAMP,
            dealer_bal_before       = %s,
            dealer_bal_after        = %s,
            msg_aftertr             = %s,
            replyrecvd_date         = CURRENT_TIMESTAMP,
            push_remarks            = 'Recharge successful',
            updated_ts              = CURRENT_TIMESTAMP
        WHERE reqid = %s
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (final_statuscode, balance_before,balance_after, response_text, reqid))

@_pg_retry
def mark_as_failed(reqid: int, push_flag: str, remarks: str,
                   response_text: Optional[str] = None,
                   final_statuscode: Optional[int] = None) -> None:
    is_permanent = (push_flag == FLAG_FAILED)
    client_txn_id = str(reqid).zfill(5)[:15]
    sql = """
        UPDATE public.frc_pyro_request_data
        SET
            client_txn_id           = COALESCE(client_txn_id, %s),
            push_flag               = %s,
            push_remarks            = %s,
            last_error_msg          = %s,
            msg_afterreq            = COALESCE(%s, msg_afterreq),
            retry_count             = CASE WHEN %s THEN retry_count ELSE retry_count + 1 END,
            final_status            = CASE WHEN %s THEN 'FAILED' ELSE final_status END,
            pyro_final_statuscode   = COALESCE(%s, pyro_final_statuscode),
            completed_at            = CASE WHEN %s THEN CURRENT_TIMESTAMP ELSE completed_at END,
            pyro_status             = CASE WHEN %s THEN 'FAL' ELSE pyro_status END,
            updated_ts              = CURRENT_TIMESTAMP
        WHERE reqid = %s
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                client_txn_id, push_flag, remarks[:200], remarks[:500], response_text,
                is_permanent, is_permanent, final_statuscode,
                is_permanent, is_permanent, reqid,
            ))

@_pg_retry
def fetch_pushed_rows_for_status_check() -> List[dict]:
    sql = """
        SELECT
            reqid, pyro_trans_id, client_txn_id,
            gsmno, caf_serial_no, batch_date,
            status_check_count
        FROM public.frc_pyro_request_data
        WHERE push_flag = 'P'
          AND status_check_eligible_at <= CURRENT_TIMESTAMP
          AND push_date >= CURRENT_TIMESTAMP - INTERVAL '60 minutes'
          AND push_date <= CURRENT_TIMESTAMP - INTERVAL '2 minutes'
        ORDER BY push_date ASC
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return [dict(r) for r in cur.fetchall()]

@_pg_retry
def update_status_check_attempt(reqid: int) -> None:
    sql = """
        UPDATE public.frc_pyro_request_data
        SET status_check_count   = status_check_count + 1,
            last_status_check_at = CURRENT_TIMESTAMP,
            updated_ts          = CURRENT_TIMESTAMP
        WHERE reqid = %s
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (reqid,))

@_pg_retry
def find_row_by_pyro_trans_id(pyro_trans_id: int) -> Optional[dict]:
    sql = """
        SELECT reqid, caf_serial_no, gsmno, batch_date, push_flag
        FROM public.frc_pyro_request_data
        WHERE pyro_trans_id = %s
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (pyro_trans_id,))
            row = cur.fetchone()
            return dict(row) if row else None


# ── Transaction log ────────────────────────────────────────────────────────────

def insert_txn_log(
    frc_reqid, caf_serial_no, gsmno, batch_date, client_txn_id,
    api_stage, api_endpoint, http_method, attempt_no,
    request_headers, request_body, response_http_code, response_body,
    pyro_status_code, pyro_status_text, pyro_txn_id,
    call_started_at, call_ended_at, duration_ms,
    is_success, is_perm_failure="N", error_class=None, error_detail=None,
) -> None:
    sql = """
        INSERT INTO public.frc_txn_log (
            frc_reqid, caf_serial_no, gsmno, batch_date, client_txn_id,
            api_stage, api_endpoint, http_method, attempt_no,
            request_headers, request_body, response_http_code, response_body,
            pyro_status_code, pyro_status_text, pyro_txn_id,
            call_started_at, call_ended_at, duration_ms,
            is_success, is_perm_failure, error_class, error_detail
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    try:
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    frc_reqid, caf_serial_no, gsmno, batch_date, client_txn_id,
                    api_stage, api_endpoint, http_method, attempt_no,
                    request_headers, request_body, response_http_code, response_body,
                    pyro_status_code, pyro_status_text, pyro_txn_id,
                    call_started_at, call_ended_at, duration_ms,
                    is_success, is_perm_failure, error_class, error_detail,
                ))
    except Exception as exc:
        logger.error("txn_log insert failed reqid=%s stage=%s: %s",
                     frc_reqid, api_stage, exc)


# ── Async wrappers ─────────────────────────────────────────────────────────────

async def async_fetch_pending_rows(batch_size):
    return await asyncio.to_thread(fetch_pending_rows, batch_size)

async def async_mark_as_pushed(reqid, pyro_trans_id, response_text, msg2pyro, sc):
    await asyncio.to_thread(mark_as_pushed, reqid, pyro_trans_id,
                            response_text, msg2pyro, sc)

async def async_mark_as_success(reqid, response_text, balance_before, balance_after, final_statuscode):
    await asyncio.to_thread(mark_as_success, reqid, response_text,
                            balance_before, balance_after, final_statuscode)

async def async_mark_as_failed(reqid, push_flag, remarks,
                                response_text=None, final_statuscode=None):
    await asyncio.to_thread(mark_as_failed, reqid, push_flag, remarks,
                            response_text, final_statuscode)

async def async_find_row_by_pyro_trans_id(pyro_trans_id):
    return await asyncio.to_thread(find_row_by_pyro_trans_id, pyro_trans_id)

async def async_fetch_pushed_rows_for_status_check():
    return await asyncio.to_thread(fetch_pushed_rows_for_status_check)

async def async_update_status_check_attempt(reqid):
    await asyncio.to_thread(update_status_check_attempt, reqid)

async def async_insert_txn_log(*args, **kwargs):
    await asyncio.to_thread(insert_txn_log, *args, **kwargs)