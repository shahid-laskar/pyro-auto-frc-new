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
from typing import Dict, List, Optional, Sequence

from app.db.oracle import (
    BCD_STATUS_NP,
    BCD_STATUS_RQ,
    batch_writeback_bcd_rq,
    fetch_bcd_claim_statuses,
)
from app.db.postgres import (
    fetch_staged_unconfirmed_requests,
    mark_requests_dispatchable,
    mark_requests_staging_failed,
)

logger = logging.getLogger(__name__)


def reconcile_staged_requests(batch_size: int = 500) -> Dict[str, int]:
    """Reconcile unconfirmed staged requests against Oracle BCD state.

    Parameters
    ----------
    batch_size : int
        Maximum number of staged requests to reconcile in this run.

    Returns
    -------
    dict
        Summary of reconciliation outcomes:
        - 'staged_found': total staged requests inspected
        - 'already_claimed_confirmed': requests where Oracle already recorded claim -> confirmed dispatchable
        - 'reclaimed_confirmed': requests unclaimed in Oracle -> claim succeeded -> confirmed dispatchable
        - 'conflict_marked_failed': requests where Oracle belongs to another reqid / terminal / missing -> marked F
        - 'reclaim_failed': requests where claim retry failed -> remain in 'S'
        - 'errors': unexpected exception count
    """
    summary = {
        "staged_found": 0,
        "already_claimed_confirmed": 0,
        "reclaimed_confirmed": 0,
        "conflict_marked_failed": 0,
        "reclaim_failed": 0,
        "errors": 0,
    }

    try:
        staged_requests = fetch_staged_unconfirmed_requests(limit=batch_size)
    except Exception as exc:
        logger.error("Reconciler: failed to fetch staged requests from Postgres: %s", exc)
        summary["errors"] += 1
        return summary

    if not staged_requests:
        logger.info("Reconciler: no staged requests pending confirmation (in_status='S')")
        return summary

    summary["staged_found"] = len(staged_requests)
    logger.info("Reconciler: found %d staged requests to reconcile against Oracle BCD", len(staged_requests))

    # Fetch Oracle claim statuses for all candidate identities
    try:
        oracle_statuses = fetch_bcd_claim_statuses(staged_requests)
    except Exception as exc:
        logger.error("Reconciler: failed to query Oracle BCD statuses: %s", exc)
        summary["errors"] += 1
        return summary

    confirmed_dispatchable_reqids: List[int] = []
    reclaim_candidates: List[dict] = []
    conflict_failed_reqids: List[int] = []

    for req in staged_requests:
        reqid = int(req["reqid"])
        gsm = str(req.get("gsmno") or req.get("gsmnumber", "")).strip()
        caf = str(req.get("caf_serial_no", "")).strip()
        circle_val = req.get("circle_code")
        if circle_val is None:
            logger.error("Reconciler: staged reqid=%s missing circle_code, marking failed", reqid)
            conflict_failed_reqids.append(reqid)
            continue
        circle = int(circle_val)
        key = (gsm, caf, circle)

        oracle_record = oracle_statuses.get(key)

        if not oracle_record:
            # Oracle record not found in BCD table
            logger.warning(
                "Reconciler: reqid=%s (GSM=%s CAF=%s circle=%s) not found in Oracle BCD. Marking failed.",
                reqid, gsm, caf, circle,
            )
            mark_requests_staging_failed(
                [reqid],
                reason=f"Reconciliation: record not found in Oracle BCD (GSM={gsm}, CAF={caf})"
            )
            summary["conflict_marked_failed"] += 1
            continue

        flow_status = oracle_record.get("FRC_FLOW_STATUS")
        oracle_reqid = oracle_record.get("FRC_REQID")

        # Scenario 1: Oracle already successfully recorded this exact claim!
        # (e.g. earlier claim succeeded in Oracle but network dropped before Postgres update)
        if flow_status == BCD_STATUS_RQ and oracle_reqid == reqid:
            logger.info(
                "Reconciler: reqid=%s already claimed in Oracle (RQ, reqid=%s). Confirming dispatchable.",
                reqid, oracle_reqid,
            )
            confirmed_dispatchable_reqids.append(reqid)

        # Scenario 2: Oracle record is still completely unclaimed
        # (earlier batch claim failed / rolled back before updating Oracle)
        elif flow_status == BCD_STATUS_NP and oracle_reqid is None:
            reclaim_candidates.append(req)

        # Scenario 3: Conflict - claimed by another request, already finalized, or invalid state
        else:
            logger.warning(
                "Reconciler: reqid=%s conflict in Oracle (FRC_FLOW_STATUS=%s, FRC_REQID=%s). Marking failed.",
                reqid, flow_status, oracle_reqid,
            )
            mark_requests_staging_failed(
                [reqid],
                reason=f"Reconciliation conflict: Oracle status={flow_status}, reqid={oracle_reqid}"
            )
            summary["conflict_marked_failed"] += 1

    # Apply batch confirmation for already claimed requests
    if confirmed_dispatchable_reqids:
        try:
            confirmed_count = mark_requests_dispatchable(confirmed_dispatchable_reqids)
            summary["already_claimed_confirmed"] = confirmed_count
        except Exception as exc:
            logger.error("Reconciler: failed to mark confirmed requests dispatchable: %s", exc)
            summary["errors"] += 1

    # Attempt reclaim for unclaimed records
    for cand in reclaim_candidates:
        cand_reqid = int(cand["reqid"])
        try:
            claimed_count = batch_writeback_bcd_rq([cand])
            if claimed_count == 1:
                mark_requests_dispatchable([cand_reqid])
                summary["reclaimed_confirmed"] += 1
                logger.info("Reconciler: successfully claimed and confirmed reqid=%s", cand_reqid)
            else:
                summary["reclaim_failed"] += 1
        except Exception as exc:
            logger.warning("Reconciler: claim retry failed for reqid=%s: %s", cand_reqid, exc)
            summary["reclaim_failed"] += 1

    logger.info("Reconciler: completed reconciliation run -- %s", summary)
    return summary
