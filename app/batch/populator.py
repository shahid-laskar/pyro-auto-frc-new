import logging
from datetime import date
from typing import List

from app.config import settings
from app.db.oracle import batch_writeback_bcd_rq, fetch_eligible_bcd_records
from app.db.postgres import bulk_insert_frc_requests, fetch_cos_bcd_for_gsms
from app.encryption import encrypt

logger = logging.getLogger(__name__)


def _encrypt_mpin(mpin: str) -> str:
    return encrypt(mpin, settings.pyro_secret_key)


def run_batch_population() -> dict:
   
    today = date.today().isoformat()
    logger.info("Batch population started for %s", today)

    summary = {
        "batch_date":        today,
        "oracle_fetched":    0,
        "ekyc_matched":      0,
        "dkyc_matched":      0,
        "skipped_no_frc":    0,
        "skipped_no_ctop":   0,
        "skipped_no_plan":   0,
        "skipped_mpin_err":  0,
        "inserted":          0,
        "bcd_rq_updated":    0,
        "errors":            0,
    }

    # Step 1: Oracle BCD -- eligible records
    try:
        bcd_records = fetch_eligible_bcd_records(
            fetch_size=settings.oracle_batch_fetch_size
        )
    except Exception as exc:
        logger.error("Batch: Oracle fetch failed -- %s", exc)
        summary["errors"] += 1
        return summary

    if not bcd_records:
        logger.info("Batch: no eligible BCD records in Oracle")
        return summary

    summary["oracle_fetched"] = len(bcd_records)
    gsm_list   = [r["GSMNUMBER"]     for r in bcd_records]
    bcd_by_gsm = {r["GSMNUMBER"]: r  for r in bcd_records}

    # Step 2: Postgres cos_bcd (EKYC) + cos_bcd_dkyc (DKYC) UNION join
    try:
        pg_rows = fetch_cos_bcd_for_gsms(gsm_list)
    except Exception as exc:
        logger.error("Batch: Postgres KYC fetch failed -- %s", exc)
        summary["errors"] += 1
        return summary

    summary["ekyc_matched"] = sum(1 for r in pg_rows if r["kyc_mode"] == "EKYC")
    summary["dkyc_matched"] = sum(1 for r in pg_rows if r["kyc_mode"] == "DKYC")
    matched_gsm_count = len({r["gsmnumber"] for r in pg_rows})
    summary["skipped_no_frc"] = len(gsm_list) - matched_gsm_count

    if not pg_rows:
        logger.info("Batch: no GSMs with complete FRC data in Postgres")
        return summary

    # Step 3: Build insert rows (identical logic for EKYC and DKYC)
    rows_to_insert: List[dict] = []

    for pg in pg_rows:
        gsm    = pg["gsmnumber"]
        caf    = pg["caf_serial_no"]
        oracle = bcd_by_gsm.get(gsm)

        if not oracle:
            logger.warning("Batch: no Oracle record for GSM=%s", gsm)
            summary["errors"] += 1
            continue

        # Vendor details must have resolved from ctop_master join
        if not pg.get("vendorid") or not pg.get("vendormsisdn"):
            logger.warning(
                "Batch: [%s] no ctop_master match CAF=%s GSM=%s ctopup=%s",
                pg.get("kyc_mode"), caf, gsm, pg.get("ctopup_number")
            )
            summary["skipped_no_ctop"] += 1
            continue

        # frc_plan_table join must have resolved
        if pg.get("frcamt") is None:
            logger.warning(
                "Batch: [%s] no frc_plan_table match CAF=%s GSM=%s",
                pg.get("kyc_mode"), caf, gsm
            )
            summary["skipped_no_plan"] += 1
            continue

        # Encrypt MPIN before storing
        # mpin_raw = frc_ctopup_number_mpin for EKYC, mpin for DKYC
        # Both aliased to 'mpin_raw' in the UNION SQL
        raw_mpin = pg.get("mpin_raw", "")
        try:
            encrypted_mpin = _encrypt_mpin(raw_mpin)
            mpin_length    = len(raw_mpin)
        except Exception as exc:
            logger.error("Batch: MPIN encrypt failed CAF=%s -- %s", caf, exc)
            summary["skipped_mpin_err"] += 1
            continue
        # logger.info("Batch: rows_to_insert=%d", len(rows_to_insert))
        rows_to_insert.append({
            "caf_serial_no":     caf,
            "gsmno":             gsm,
            "csccode":           pg.get("de_csccode") or oracle.get("DE_CSCCODE"),
            "circle_code":       oracle.get("CIRCLE_CODE"),
            "edate":             oracle.get("HLR_FINAL_ACT_DATE"),
            "reqdate":           pg.get("live_photo_time"),  # normalised in SQL
            "frc_plan_name":     pg.get("frc_plan_name"),
            "frc_plan_code":     pg.get("frc_plan_code"),
            "frc_category_code": pg.get("frc_category_code"),
            "frcamt":            int(pg.get("frcamt", 0)),
            "ctopup_number":     pg.get("ctopup_number"),   # normalised in SQL
            "vendormsisdn":      pg.get("vendormsisdn"),
            "vendorid":          pg.get("vendorid"),
            "mpin":              encrypted_mpin,
            "mpin_length":       mpin_length,
            "max_retries": settings.recharge_max_retries,
            # kyc_mode from Postgres (EKYC or DKYC) — Oracle always NULL
            "kyc_mode":          pg.get("kyc_mode", "EKYC"),
        })

    if not rows_to_insert:
        logger.info("Batch: no rows to insert after validation")
        return summary

    # Step 4: Bulk insert -- returns (reqid, caf_serial_no) per inserted row
    try:
        inserted_pairs = bulk_insert_frc_requests(rows_to_insert)
        summary["inserted"] = len(inserted_pairs)
    except Exception as exc:
        logger.error("Batch: bulk insert failed -- %s", exc)
        summary["errors"] += 1
        return summary

    # Step 5: BCD writeback -> RQ (primary idempotency guard)
    # Must happen after successful Postgres insert.
    # If BCD writeback fails, Postgres UNIQUE constraint prevents re-insert
    # on next batch run (ON CONFLICT DO NOTHING).
    if inserted_pairs:
        try:
            updated = batch_writeback_bcd_rq(inserted_pairs)
            summary["bcd_rq_updated"] = updated
        except Exception as exc:
            logger.error(
                "Batch: BCD writeback failed for %d rows -- %s. "
                "Postgres rows inserted. Next batch skips via UNIQUE constraint.",
                len(inserted_pairs), exc,
            )
            summary["errors"] += 1

    logger.info(
        "Batch complete: oracle=%d ekyc=%d dkyc=%d inserted=%d bcd_rq=%d "
        "skip_no_frc=%d skip_no_ctop=%d skip_no_plan=%d skip_mpin=%d errors=%d",
        summary["oracle_fetched"],
        summary["ekyc_matched"], summary["dkyc_matched"],
        summary["inserted"], summary["bcd_rq_updated"],
        summary["skipped_no_frc"], summary["skipped_no_ctop"],
        summary["skipped_no_plan"], summary["skipped_mpin_err"],
        summary["errors"],
    )
    return summary
