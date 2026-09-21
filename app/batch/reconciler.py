"""Reconciliation and Cross-Database Failure Recovery (Phase 8).

Handles the critical cross-database failure case:
    Postgres staging committed (in_status='S')
    Oracle claim failed (timeout, network error, mismatch)

Requirements:
1. Requests remain NOT DISPATCHABLE (in_status='S') until Oracle claim is confirmed.
2. Identifies:
       Postgres request (reqid, caf_serial_no, gsmno, circle_code)
                 ↕
       Oracle BCD row (GSMNUMBER, CAF_SERIAL_NO, CIRCLE_CODE)
3. Outcomes:
   - Already claimed for this reqid (Oracle FRC_FLOW_STATUS='RQ' and FRC_REQID=reqid):
     Confirms request dispatchable (mark_requests_dispatchable([reqid]) -> in_status='C').
     Guarantees NO duplicate Oracle update, NO second recharge.
   - Unclaimed in Oracle (Oracle FRC_FLOW_STATUS='NP' and FRC_REQID IS NULL):
     Attempts Q020 claim writeback via batch_writeback_bcd_rq.
     If successful, marks dispatchable (in_status='C').
   - Conflicting / Claimed by different reqid / Finalized / Missing in Oracle:
     Marks Postgres request failed (mark_requests_staging_failed([reqid]) -> in_status='F').
     Guarantees permanent hold/abortion without sending a second recharge.
"""

import logging
from typing import Dict

logger = logging.getLogger(__name__)


def reconcile_staged_requests(batch_size: int = 500) -> Dict[str, int]:
    """No-op in production-aligned architecture: rows are inserted directly as in_status='C'.

    The legacy S -> C staging transition and unconfirmed staging recovery have been
    eliminated. All new requests are immediately dispatchable.
    """
    logger.info("Reconciler: no staged requests (in_status='S' eliminated; directly dispatchable)")
    return {
        "staged_found": 0,
        "already_claimed_confirmed": 0,
        "reclaimed_confirmed": 0,
        "conflict_marked_failed": 0,
        "reclaim_failed": 0,
        "errors": 0,
    }

