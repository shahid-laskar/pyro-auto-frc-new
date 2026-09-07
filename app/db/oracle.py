
import logging
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional, Sequence


import oracledb as cx_Oracle


from app.config import settings

logger = logging.getLogger(__name__)

_pool: Optional[cx_Oracle.SessionPool] = None

# BCD frc_flow_status constants
BCD_STATUS_NP = "NP"
BCD_STATUS_RQ = "RQ"
BCD_STATUS_W  = "W"
BCD_STATUS_NR = "NR"
BCD_STATUS_P  = "P"   # Success 
BCD_STATUS_ID = "ID"  # Invalid data permanent failure
BCD_STATUS_F  = "F"   # General failure

# Pyro codes -> ID (invalid data): wrong number, denom, MPIN, suspended account
INVALID_DATA_CODES = {5006, 5011, 5012, 5030}


def bcd_status_for_pyro_failure(pyro_status_code: int) -> str:
    """Map Pyro error code to BCD frc_flow_status."""
    return BCD_STATUS_ID if pyro_status_code in INVALID_DATA_CODES else BCD_STATUS_F


# Pool lifecycle

def init_oracle_pool() -> None:
    global _pool
    _pool = cx_Oracle.SessionPool(
        user=settings.oracle_user,
        password=settings.oracle_password,
        dsn=settings.oracle_dsn,
        min=1, max=5, increment=1,
        encoding="UTF-8",
    )
    logger.info("Oracle pool initialised (BCD, min=1 max=5)")


def close_oracle_pool() -> None:
    global _pool
    if _pool:
        _pool.close()
        _pool = None
        logger.info("Oracle pool closed")


@contextmanager
def get_oracle_conn() -> Generator[cx_Oracle.Connection, None, None]:
    conn = _pool.acquire()
    try:
        yield conn
    finally:
        _pool.release(conn)


# READ

def fetch_eligible_bcd_records(
    fetch_size: int = 500,
    circle_codes: Optional[Sequence[int]] = None,
) -> List[dict]:
    binds: Dict[str, Any] = {
        "status_np": BCD_STATUS_NP,
        "fetch_size": fetch_size,
    }

    if circle_codes is not None:
        unique_circles = sorted(set(int(c) for c in circle_codes))
        if not unique_circles:
            logger.info("Oracle BCD: empty circle_codes filter supplied; returning 0 records")
            return []
        placeholders = [f":c_{i}" for i in range(len(unique_circles))]
        for i, c in enumerate(unique_circles):
            binds[f"c_{i}"] = c
        circle_predicate = f"AND CIRCLE_CODE IN ({', '.join(placeholders)})"
    else:
        circle_predicate = ""

    sql = f"""
        SELECT * FROM (
            SELECT
                GSMNUMBER,
                CAF_SERIAL_NO,
                DE_CSCCODE,
                CIRCLE_CODE,
                HLR_FINAL_ACT_DATE
            FROM CAF_ADMIN.BCD
            WHERE ACTIVATION_STATUS  = 'C' 
            AND HLR_FINAL_ACT_DATE IS NOT NULL
            AND FRC_FLOW_STATUS    = :status_np
            AND FRC_REQID          IS NULL           
            {circle_predicate}
            ORDER BY HLR_FINAL_ACT_DATE ASC
        ) WHERE ROWNUM <= :fetch_size
    """
    with get_oracle_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, binds)
        if cur.description:
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        else:
            rows = []

    logger.info(
        "Oracle BCD: fetched %d eligible records (circles=%s)",
        len(rows),
        "ALL" if circle_codes is None else len(unique_circles),
    )
    return rows


# Custom exceptions for Q020 claim verification
class OracleClaimMismatchError(Exception):
    """Raised when actual Oracle claims do not match expected claims."""
    pass


class OracleClaimDataIntegrityError(Exception):
    """Raised when an Oracle claim updates more than 1 row for a single candidate identity."""
    pass


# WRITE -- BCD status transitions

def batch_writeback_bcd_rq(caf_reqid_pairs: Sequence[dict]) -> int:
    """Claim candidate BCD records in Oracle by advancing FRC_FLOW_STATUS to 'RQ'.

    Enforces exact Oracle identity verification (composite key + circle + current state):
        WHERE GSMNUMBER       = :gsmnumber
          AND CAF_SERIAL_NO   = :caf_serial_no
          AND CIRCLE_CODE     = :circle_code
          AND FRC_FLOW_STATUS = :status_np
          AND FRC_REQID IS NULL

    Enforces claim verification:
        - 0 updated on a record -> claim failure
        - 1 updated on a record -> success
        - >1 updated on a record -> data integrity alarm
        - Batch level: expected claims == actual claims
        - Any mismatch causes transaction rollback and raises OracleClaimMismatchError
          (or OracleClaimDataIntegrityError), triggering cross-database hold/recovery.

    Parameters
    ----------
    caf_reqid_pairs : Sequence[dict]
        List of dictionaries containing at least:
        'reqid', 'caf_serial_no' (or 'CAF_SERIAL_NO'),
        'gsmnumber' (or 'gsmno' or 'GSMNUMBER'),
        'circle_code' (or 'CIRCLE_CODE').

    Returns
    -------
    int
        Count of successfully claimed records (guaranteed equal to len(caf_reqid_pairs)).

    Raises
    -------
    OracleClaimMismatchError
        If any record fails to claim (0 rows updated) or total claims != expected.
    OracleClaimDataIntegrityError
        If any record updates >1 row in Oracle.
    ValueError
        If required identity fields are missing from any pair.
    """
    if not caf_reqid_pairs:
        return 0

    sql = """
        UPDATE CAF_ADMIN.BCD
        SET
            FRC_FLOW_STATUS        = :status,
            FRC_REQID              = :reqid,
            FRC_FLOW_STATUS_UPD_AT = CURRENT_TIMESTAMP,
            FRC_FLOW_REMARKS       = 'FRC request created - pending Pyro submission'
        WHERE GSMNUMBER       = :gsmnumber
          AND CAF_SERIAL_NO   = :caf_serial_no
          AND CIRCLE_CODE     = :circle_code
          AND FRC_FLOW_STATUS = :status_np
          AND FRC_REQID IS NULL
    """

    # Pre-validate all candidate identities before acquiring database connection
    validated_candidates = []
    for p in caf_reqid_pairs:
        reqid = int(p["reqid"])
        caf = str(p.get("caf_serial_no") or p.get("CAF_SERIAL_NO", "")).strip()
        gsm = str(p.get("gsmnumber") or p.get("gsmno") or p.get("GSMNUMBER", "")).strip()
        raw_circle = p.get("circle_code") if p.get("circle_code") is not None else p.get("CIRCLE_CODE")

        if not caf or not gsm or raw_circle is None:
            raise ValueError(
                f"Oracle Q020 claim requires complete identity (GSMNUMBER, CAF_SERIAL_NO, CIRCLE_CODE): got {p}"
            )
        circle = int(raw_circle)
        validated_candidates.append({
            "reqid": reqid,
            "caf_serial_no": caf,
            "gsmnumber": gsm,
            "circle_code": circle,
            "orig": p,
        })

    failed_claims: List[dict] = []
    alarm_claims: List[dict] = []
    successful_reqids: List[int] = []

    with get_oracle_conn() as conn:
        cur = conn.cursor()
        for cand in validated_candidates:
            reqid = cand["reqid"]
            caf = cand["caf_serial_no"]
            gsm = cand["gsmnumber"]
            circle = cand["circle_code"]
            p = cand["orig"]

            binds = {
                "status": BCD_STATUS_RQ,
                "reqid": reqid,
                "gsmnumber": gsm,
                "caf_serial_no": caf,
                "circle_code": circle,
                "status_np": BCD_STATUS_NP,
            }

            cur.execute(sql, binds)
            rc = cur.rowcount

            if rc == 1:
                successful_reqids.append(reqid)
            elif rc == 0:
                logger.warning(
                    "Oracle BCD claim failure (0 rows updated): reqid=%s GSM=%s CAF=%s circle=%s",
                    reqid, gsm, caf, circle,
                )
                failed_claims.append(p)
            else:
                logger.critical(
                    "Oracle BCD DATA-INTEGRITY ALARM (%d rows updated for single identity): reqid=%s GSM=%s CAF=%s circle=%s",
                    rc, reqid, gsm, caf, circle,
                )
                alarm_claims.append(p)

        # Batch-level verification: expected claims == actual claims
        if failed_claims or alarm_claims or len(successful_reqids) != len(caf_reqid_pairs):
            conn.rollback()
            err_msg = (
                f"Oracle Q020 claim verification mismatch: expected {len(caf_reqid_pairs)} claims, "
                f"got {len(successful_reqids)} successes, {len(failed_claims)} failures (0 rows), "
                f"{len(alarm_claims)} alarms (>1 rows). Transaction rolled back."
            )
            logger.error(err_msg)
            if alarm_claims:
                raise OracleClaimDataIntegrityError(err_msg)
            raise OracleClaimMismatchError(err_msg)

        conn.commit()

    logger.info(
        "Oracle BCD: %d/%d rows claimed as RQ with verified exact identity",
        len(successful_reqids), len(caf_reqid_pairs),
    )
    return len(successful_reqids)


def update_bcd_status(
    caf_serial_no: str,
    reqid: int,
    frc_flow_status: str,
    remarks: str,
) -> None:

    sql = """
        UPDATE CAF_ADMIN.BCD
        SET
            FRC_FLOW_STATUS        = :status,
            FRC_FLOW_STATUS_UPD_AT = CURRENT_TIMESTAMP,
            FRC_FLOW_REMARKS       = :remarks
        WHERE CAF_SERIAL_NO = :caf_serial_no
          AND FRC_REQID     = :reqid
    """
    try:
        with get_oracle_conn() as conn:
            cur = conn.cursor()
            cur.execute(sql, {
                "status":        frc_flow_status,
                "remarks":       remarks[:2000],
                "caf_serial_no": caf_serial_no,
                "reqid":         reqid,
            })
            conn.commit()
        logger.debug("BCD updated: caf=%s reqid=%s -> %s",
                     caf_serial_no, reqid, frc_flow_status)
    except Exception as exc:
        logger.error("BCD writeback failed (non-fatal): caf=%s reqid=%s status=%s err=%s",
                     caf_serial_no, reqid, frc_flow_status, exc)
