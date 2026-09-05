
import logging
from contextlib import contextmanager
from typing import Generator, List, Optional


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

def fetch_eligible_bcd_records(fetch_size: int = 500) -> List[dict]:
   
    sql = """
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
            ORDER BY HLR_FINAL_ACT_DATE ASC
        ) WHERE ROWNUM <= :fetch_size
    """
    with get_oracle_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, status_np=BCD_STATUS_NP, fetch_size=fetch_size)
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    logger.info("Oracle BCD: fetched %d eligible records", len(rows))
    return rows


# WRITE -- BCD status transitions

def batch_writeback_bcd_rq(caf_reqid_pairs: List[dict]) -> int:
  
    if not caf_reqid_pairs:
        return 0

    sql = """
        UPDATE CAF_ADMIN.BCD
        SET
            FRC_FLOW_STATUS        = :status,
            FRC_REQID              = :reqid,
            FRC_FLOW_STATUS_UPD_AT = CURRENT_TIMESTAMP,
            FRC_FLOW_REMARKS       = 'FRC request created - pending Pyro submission'
        WHERE CAF_SERIAL_NO   = :caf_serial_no
          AND FRC_FLOW_STATUS = 'NP'
    """
    data = [
        {"status": BCD_STATUS_RQ, "reqid": p["reqid"], "caf_serial_no": p["caf_serial_no"]}
        for p in caf_reqid_pairs
    ]
    with get_oracle_conn() as conn:
        cur = conn.cursor()
        cur.executemany(sql, data)
        updated = cur.rowcount
        conn.commit()

    logger.info("Oracle BCD: %d rows written back as RQ", updated)
    return updated


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
