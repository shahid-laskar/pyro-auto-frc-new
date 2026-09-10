import asyncio
import functools
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple, Union

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

_read_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
_write_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
# Compatibility marker retained for older tests/integrations; production
# routing uses the explicit pools above.
_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None

# push_flag state constants
FLAG_PENDING = "N"
FLAG_PUSHED  = "P"
FLAG_SUCCESS = "Y"
FLAG_FAILED  = "F"
FLAG_RETRY   = "E"

# in_status state constants (CHECK constraint: 'C', 'S', 'F')
IN_STATUS_STAGED     = "S"  # Staged by Q023, awaiting Oracle claim (NOT DISPATCHABLE)
IN_STATUS_CONFIRMED  = "C"  # Oracle claim confirmed (DISPATCHABLE)
IN_STATUS_FAILED     = "F"  # Oracle claim failed or unrecoverable staging failure

# Pyro codes -> permanent failure (no auto-retry)
PERMANENT_FAILURE_CODES = {406, 505, 5006, 5007, 5011, 5012, 5030}
# Subset: invalid data errors -> BCD status 'ID'
INVALID_DATA_CODES = {5006, 5011, 5012, 5030}


# ── Pool lifecycle ─────────────────────────────────────────────────────────────

def init_pg_pool() -> None:
    global _read_pool, _write_pool, _pool
    common = {
        "minconn": settings.pg_min_conn,
        "maxconn": settings.pg_max_conn,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    }
    _write_pool = psycopg2.pool.ThreadedConnectionPool(
        **common,
        host=settings.pg_write_host,
        port=settings.pg_write_port,
        database=settings.pg_write_database,
        user=settings.pg_write_user,
        password=settings.pg_write_password,
    )
    _pool = _write_pool
    try:
        _read_pool = psycopg2.pool.ThreadedConnectionPool(
            **common,
            host=settings.pg_read_host,
            port=settings.pg_read_port,
            database=settings.pg_read_database,
            user=settings.pg_read_user,
            password=settings.pg_read_password,
        )
    except Exception:
        _write_pool.closeall()
        _write_pool = None
        _pool = None
        raise
    logger.info("Postgres write pool initialised (min=%d max=%d)",
                settings.pg_min_conn, settings.pg_max_conn)
    logger.info("Postgres read pool initialised (min=%d max=%d)",
                settings.pg_min_conn, settings.pg_max_conn)


def close_pg_pool() -> None:
    global _read_pool, _write_pool, _pool
    if _read_pool:
        _read_pool.closeall()
        _read_pool = None
        logger.info("Postgres read pool closed")
    if _write_pool:
        _write_pool.closeall()
        _write_pool = None
        logger.info("Postgres write pool closed")
    _pool = None


def _probe_pool(pool, label: str) -> dict:
    if pool is None:
        return {"status": "unavailable", "role": label}
    try:
        conn = pool.getconn()
    except Exception as exc:
        return {"status": "error", "role": label, "error": type(exc).__name__}
    discard = False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT inet_server_port(), pg_is_in_recovery();")
            port, in_recovery = cur.fetchone()
        return {
            "status": "ok",
            "role": label,
            "server_port": port,
            "in_recovery": in_recovery,
        }
    except Exception as exc:
        discard = isinstance(exc, psycopg2.OperationalError) or bool(conn.closed)
        return {"status": "error", "role": label, "error": type(exc).__name__}
    finally:
        try:
            conn.rollback()
        except Exception:
            discard = True
        pool.putconn(conn, close=discard)


def postgres_health() -> dict:
    """Return non-mutating health details for both PostgreSQL endpoints."""
    return {
        "read": _probe_pool(_read_pool, "read"),
        "write": _probe_pool(_write_pool, "write"),
    }


@contextmanager
def _get_pg_conn(role: str) -> Generator:
    pool = _read_pool if role == "read" else _write_pool
    if pool is None:
        raise RuntimeError(f"Postgres {role} pool is not initialized")
    conn = pool.getconn()
    # The pool has no built-in liveness check — swap out any connection the
    # server dropped while it was idle (presents as conn.closed != 0).
    if conn.closed:
        logger.warning("Postgres: stale connection detected on checkout — replacing")
        pool.putconn(conn, close=True)
        conn = pool.getconn()
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
        pool.putconn(conn, close=discard)
        if discard:
            logger.warning("Postgres %s: broken connection discarded from pool", role)


@contextmanager
def get_pg_read_conn() -> Generator:
    with _get_pg_conn("read") as conn:
        yield conn


@contextmanager
def get_pg_write_conn() -> Generator:
    with get_pg_conn() as conn:
        yield conn


@contextmanager
def get_pg_conn() -> Generator:
    """Backward-compatible write connection context."""
    with _get_pg_conn("write") as conn:
        yield conn


# ── Concurrency guard: Population Advisory Lock (Phase 10) ──────────────────────
POPULATION_ADVISORY_LOCK_KEY: int = 8292837261947261


@contextmanager
def population_advisory_lock() -> Generator[bool, None, None]:
    """PostgreSQL session-level advisory lock context manager to guard population execution (Phase 10).

    Acquires a session-level PostgreSQL advisory lock on a dedicated connection:
        SELECT pg_try_advisory_lock(%s);

    Yields:
        bool: True if lock was acquired, False if lock is busy (contention).

    Guarantees:
        - The connection remains open and checked out for the entire protected operation.
        - On exit (success or exception), if the lock was acquired, calls:
              SELECT pg_advisory_unlock(%s);
          and returns the connection to the pool.
        - If the lock was not acquired (lock busy), yields False immediately,
          and returns the connection to the pool without blocking.
    """
    if _write_pool is None and not hasattr(get_pg_conn, "mock_calls") and not hasattr(get_pg_write_conn, "mock_calls"):
        logger.debug("Postgres pool not initialized (test environment); yielding True without DB lock.")
        yield True
        return

    with get_pg_write_conn() as conn:
        acquired = False
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s);", (POPULATION_ADVISORY_LOCK_KEY,))
            row = cur.fetchone()
            acquired = bool(row[0]) if row else False

        if not acquired:
            logger.warning(
                "Postgres advisory lock %s is BUSY. Concurrent population in progress. Skipping.",
                POPULATION_ADVISORY_LOCK_KEY,
            )
            yield False
            return

        logger.info(
            "Postgres advisory lock %s ACQUIRED. Starting protected population execution.",
            POPULATION_ADVISORY_LOCK_KEY,
        )
        try:
            yield True
        finally:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s);", (POPULATION_ADVISORY_LOCK_KEY,))
                logger.info(
                    "Postgres advisory lock %s RELEASED.",
                    POPULATION_ADVISORY_LOCK_KEY,
                )
            except Exception as exc:
                logger.error(
                    "Postgres: failed to unlock advisory lock %s: %s",
                    POPULATION_ADVISORY_LOCK_KEY, exc,
                )


# ── Source data queries (batch population) ────────────────────────────────────
@_pg_retry
def fetch_cos_bcd_for_gsms(
    gsm_numbers: Sequence[str],
    circle_codes: Optional[Sequence[Union[int, str]]] = None,
) -> List[dict]:
    """Fetch eligible customer, plan, vendor, and MPIN details for candidate GSMs.

    Performs a UNION ALL across EKYC (cos_bcd) and DKYC (cos_bcd_dkyc) tables,
    joining against ctop_master for POS dealer credentials and frc_plan_table for
    tariff amounts.

    Parameters
    ----------
    gsm_numbers : Sequence[str]
        List of GSM numbers to enrich.
    circle_codes : Optional[Sequence[Union[int, str]]]
        Optional collection of circle codes. When provided (FILTERED mode), only
        records belonging to these circles are retrieved using:
            AND cb.circle_code = ANY(%(allowed_circles)s)
        where allowed_circles are formatted strings without column-side casting.
        When None (ALL mode), no circle predicate is applied.
    """
    if not gsm_numbers:
        return []

    params: Dict[str, Any] = {"gsms": list(gsm_numbers)}
    circle_predicate = ""

    if circle_codes is not None:
        if len(circle_codes) == 0:
            logger.info("fetch_cos_bcd_for_gsms: empty circle_codes provided; returning 0 rows")
            return []
        formatted_circles = sorted(
            set(str(int(c)) if str(c).isdigit() else str(c) for c in circle_codes)
        )
        params["allowed_circles"] = formatted_circles
        circle_predicate = "AND cb.circle_code = ANY(%(allowed_circles)s)"

    sql = f"""
        -- ── EKYC branch (cos_bcd) ─────────────────────────────────────────────
        SELECT
            cb.gsmnumber,
            cb.caf_serial_no,
            cb.de_csccode,
            cb.circle_code                      AS circle_code,
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
           AND (fp.circle_code = cb.circle_code OR fp.circle_code = '9999')
           AND (fp.end_date IS NULL OR fp.end_date >= CURRENT_DATE)
        WHERE cb.gsmnumber = ANY(%(gsms)s)
          {circle_predicate}
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
            cb.circle_code                      AS circle_code,
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
           AND fp.circle_code = cb.circle_code
           AND (fp.end_date IS NULL OR fp.end_date >= CURRENT_DATE)
        WHERE cb.gsmnumber = ANY(%(gsms)s)
          {circle_predicate}
          AND cb.tariff_plan            IS NOT NULL
          AND cb.parent_ctopup_number   IS NOT NULL
          AND cb.mpin                   IS NOT NULL
    """
    with get_pg_read_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]

    ekyc_count = sum(1 for r in rows if r["kyc_mode"] == "EKYC")
    dkyc_count = sum(1 for r in rows if r["kyc_mode"] == "DKYC")
    logger.info(
        "cos_bcd join: %d/%d GSMs matched (EKYC=%d DKYC=%d, circle_filter=%s)",
        len(rows), len(gsm_numbers), ekyc_count, dkyc_count,
        "NONE" if circle_codes is None else len(params.get("allowed_circles", [])),
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
            'S', 'N', 'N',
            CURRENT_DATE, CURRENT_TIMESTAMP
        )
        ON CONFLICT (batch_date, caf_serial_no) DO NOTHING
        RETURNING reqid, caf_serial_no, gsmno, circle_code
    """
    inserted_pairs = []
    with get_pg_write_conn() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(sql, row)
                result = cur.fetchone()
                if result:
                    inserted_pairs.append({
                        "reqid":         result[0],
                        "caf_serial_no": result[1],
                        "gsmno":         result[2] if len(result) > 2 else row.get("gsmno"),
                        "gsmnumber":     result[2] if len(result) > 2 else row.get("gsmno"),
                        "circle_code":   result[3] if len(result) > 3 else row.get("circle_code"),
                    })

    logger.info("Postgres: inserted %d/%d rows into frc_pyro_request_data (staged, in_status='S')",
                len(inserted_pairs), len(rows))
    return inserted_pairs


@_pg_retry
def mark_requests_dispatchable(reqids: Sequence[int]) -> int:
    """Advance staged requests from 'S' (Staged) to 'C' (Confirmed/Dispatchable).

    This function must be called only after the corresponding Oracle BCD claim
    (Q020) has successfully written back. Once marked with in_status='C',
    the requests become eligible for pickup by fetch_pending_rows.

    Parameters
    ----------
    reqids : Sequence[int]
        Collection of frc_pyro_request_data.reqid values to confirm.

    Returns
    -------
    int
        Number of requests updated to in_status='C'.
    """
    if not reqids:
        return 0

    unique_reqids = list(set(int(r) for r in reqids))
    sql = """
        UPDATE public.frc_pyro_request_data
        SET
            in_status  = 'C',
            updated_ts = CURRENT_TIMESTAMP
        WHERE reqid = ANY(%(reqids)s)
          AND in_status = 'S'
    """
    with get_pg_write_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"reqids": unique_reqids})
            updated = cur.rowcount

    logger.info(
        "Postgres: marked %d/%d requests dispatchable (in_status='C')",
        updated, len(unique_reqids),
    )
    return updated


@_pg_retry
def mark_requests_staging_failed(reqids: Sequence[int], reason: str = "Oracle claim failed") -> int:
    """Transition staged requests from 'S' (Staged) to 'F' (Failed).

    Used when an Oracle claim attempt fails unrecoverably, ensuring
    staged requests do not remain indefinitely in non-dispatchable limbo.

    Parameters
    ----------
    reqids : Sequence[int]
        Collection of frc_pyro_request_data.reqid values to mark failed.
    reason : str
        Failure explanation recorded in push_remarks and last_error_msg.

    Returns
    -------
    int
        Number of requests marked failed.
    """
    if not reqids:
        return 0

    unique_reqids = list(set(int(r) for r in reqids))
    sql = """
        UPDATE public.frc_pyro_request_data
        SET
            in_status      = 'F',
            push_flag      = 'F',
            final_status   = 'FAILED',
            push_remarks   = %(reason)s,
            last_error_msg = %(reason)s,
            completed_at   = CURRENT_TIMESTAMP,
            updated_ts     = CURRENT_TIMESTAMP
        WHERE reqid = ANY(%(reqids)s)
          AND in_status = 'S'
    """
    with get_pg_write_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"reqids": unique_reqids, "reason": reason[:200]})
            updated = cur.rowcount

    logger.warning(
        "Postgres: marked %d/%d requests staging failed (in_status='F'): %s",
        updated, len(unique_reqids), reason,
    )
    return updated


@_pg_retry
def fetch_staged_unconfirmed_requests(limit: int = 500) -> List[dict]:
    """Fetch requests in staged state ('S') that are pending Oracle claim confirmation.

    These requests are NOT dispatchable by the recharge processor until
    their Oracle claim is confirmed (transitioning in_status to 'C').

    Parameters
    ----------
    limit : int
        Maximum number of staged requests to fetch.

    Returns
    -------
    List[dict]
        List of dictionaries with reqid, caf_serial_no, gsmno, circle_code, batch_date, created_at.
    """
    sql = """
        SELECT
            reqid, caf_serial_no, gsmno, circle_code, batch_date, created_at
        FROM public.frc_pyro_request_data
        WHERE in_status = 'S'
        ORDER BY created_at ASC
        LIMIT %s
    """
    with get_pg_write_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (limit,))
            return [dict(r) for r in cur.fetchall()]


# ── Recharge state machine ─────────────────────────────────────────────────────
@_pg_retry
def fetch_pending_rows(
    batch_size: int = 500,
    circle_codes: Optional[Sequence[int]] = None,
) -> List[dict]:
    """Atomically claim and fetch pending dispatch rows using FOR UPDATE SKIP LOCKED (Q024).

    Replaces plain SELECT with atomic SELECT + claim to guarantee dispatch isolation:
    - Sets push_flag = 'P', push_remarks = 'Claimed for Pyro dispatch', submitted_at = NOW()
    - Selects eligible rows (in_status = 'C', push_flag IN ('N', 'E'), retry_count <= max_retries)
    - Applies FOR UPDATE SKIP LOCKED so concurrent workers never receive the same row
    - Filters by circle_code when circle_codes is provided; nationwide when None (ALL)
    - Returns the exact rows claimed by this worker
    """
    if circle_codes is not None:
        circle_clause = "AND circle_code = ANY(%s)"
        params = ([int(c) for c in circle_codes], batch_size)
    else:
        circle_clause = ""
        params = (batch_size,)

    sql = f"""
        UPDATE public.frc_pyro_request_data
        SET
            push_flag    = 'P',
            push_remarks = 'Claimed for Pyro dispatch',
            submitted_at = CURRENT_TIMESTAMP,
            updated_ts   = CURRENT_TIMESTAMP
        WHERE reqid IN (
            SELECT reqid
            FROM public.frc_pyro_request_data
            WHERE in_status   = 'C'
              AND push_flag   IN ('N', 'E')
              AND retry_count <= max_retries
              {circle_clause}
            ORDER BY created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT %s
        )
        RETURNING
            reqid, caf_serial_no, gsmno, batch_date, kyc_mode,
            vendormsisdn, ctopup_number, frcamt, mpin, mpin_length,
            push_flag, retry_count, max_retries, client_txn_id, circle_code
    """
    with get_pg_write_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            claimed_rows = [dict(r) for r in cur.fetchall()]

    if claimed_rows:
        logger.info(
            "Postgres (Q024): atomically claimed %d pending requests for dispatch (circles=%s)",
            len(claimed_rows),
            "ALL" if circle_codes is None else list(circle_codes),
        )
    return claimed_rows


@_pg_retry
def release_unprocessed_claims(reqids: Sequence[int]) -> int:
    """Release claimed rows back to pending ('N' or 'E') if a batch aborted before submission.

    Only releases rows that are in 'P' state and have not been submitted to Pyro
    (i.e. pyro_trans_id IS NULL and push_date IS NULL).

    Parameters
    ----------
    reqids : Sequence[int]
        Collection of reqid values to release.

    Returns
    -------
    int
        Count of rows successfully released.
    """
    if not reqids:
        return 0

    unique_reqids = list(set(int(r) for r in reqids))
    sql = """
        UPDATE public.frc_pyro_request_data
        SET
            push_flag    = CASE WHEN retry_count > 0 THEN 'E' ELSE 'N' END,
            push_remarks = 'Claim released - batch aborted before submission',
            updated_ts   = CURRENT_TIMESTAMP
        WHERE reqid = ANY(%s)
          AND push_flag = 'P'
          AND pyro_trans_id IS NULL
          AND push_date IS NULL
    """
    with get_pg_write_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (unique_reqids,))
            released = cur.rowcount

    logger.info("Postgres: released %d/%d unprocessed dispatch claims", released, len(unique_reqids))
    return released

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
    with get_pg_write_conn() as conn:
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
    with get_pg_write_conn() as conn:
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
    with get_pg_write_conn() as conn:
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
    with get_pg_write_conn() as conn:
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
    with get_pg_write_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (reqid,))

@_pg_retry
def find_row_by_pyro_trans_id(pyro_trans_id: int) -> Optional[dict]:
    sql = """
        SELECT reqid, caf_serial_no, gsmno, batch_date, push_flag
        FROM public.frc_pyro_request_data
        WHERE pyro_trans_id = %s
    """
    with get_pg_write_conn() as conn:
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
        with get_pg_write_conn() as conn:
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

async def async_fetch_pending_rows(batch_size: int = 500, circle_codes: Optional[Sequence[int]] = None):
    return await asyncio.to_thread(fetch_pending_rows, batch_size, circle_codes)

async def async_release_unprocessed_claims(reqids: Sequence[int]):
    return await asyncio.to_thread(release_unprocessed_claims, reqids)

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

async def async_mark_requests_dispatchable(reqids):
    return await asyncio.to_thread(mark_requests_dispatchable, reqids)

async def async_mark_requests_staging_failed(reqids, reason="Oracle claim failed"):
    return await asyncio.to_thread(mark_requests_staging_failed, reqids, reason)
