"""Phase 18A: Restore original database state before test data preparation.

This script restores:
1. Oracle CAF_ADMIN.BCD for the 10 rows (FRC_FLOW_STATUS='RQ', original FRC_REQID).
2. PostgreSQL public.cos_bcd (restores original 5 dummy rows).
3. PostgreSQL public.cos_bcd_dkyc (restores original 5 rows).
4. PostgreSQL public.frc_plan_table (removes circle 2 row and resets end_dates).
"""

import json
import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, REPO_ROOT)

import oracledb
import psycopg2

from app.config import settings


def execute_restore() -> None:
    print("[1/4] Connecting to local databases...")
    ora_conn = oracledb.connect(
        user=settings.oracle_user,
        password=settings.oracle_password,
        dsn=settings.oracle_dsn,
    )
    ora_cur = ora_conn.cursor()

    pg_conn = psycopg2.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        dbname=settings.pg_database,
        user=settings.pg_user,
        password=settings.pg_password,
    )
    pg_cur = pg_conn.cursor()

    snapshots_dir = os.path.join(REPO_ROOT, "docs", "snapshots")

    # 1. Oracle restore
    print("[2/4] Restoring Oracle CAF_ADMIN.BCD rows...")
    with open(os.path.join(snapshots_dir, "oracle_bcd_phase18_backup.json"), "r") as f:
        oracle_backup = json.load(f)

    for r in oracle_backup:
        ora_cur.execute("""
            UPDATE CAF_ADMIN.BCD
            SET FRC_FLOW_STATUS = :status,
                FRC_REQID = :reqid
            WHERE GSMNUMBER = :gsm AND CAF_SERIAL_NO = :caf AND CIRCLE_CODE = :circle
        """, {
            "status": r["FRC_FLOW_STATUS"],
            "reqid": r["FRC_REQID"],
            "gsm": r["GSMNUMBER"],
            "caf": r["CAF_SERIAL_NO"],
            "circle": r["CIRCLE_CODE"],
        })
    ora_conn.commit()
    print(f"  Oracle restored {len(oracle_backup)} rows.")

    # 2. Postgres restore
    print("[3/4] Restoring PostgreSQL cos_bcd & cos_bcd_dkyc rows...")
    with open(os.path.join(snapshots_dir, "postgres_phase18_backup.json"), "r") as f:
        pg_backup = json.load(f)

    # cos_bcd
    for r in pg_backup["cos_bcd"]:
        pg_cur.execute("""
            UPDATE public.cos_bcd
            SET gsmnumber = %(gsm)s,
                circle_code = %(circle)s,
                de_csccode = %(csc)s,
                frc_plan_name = %(plan_name)s,
                frc_plan_code = %(plan_code)s,
                frc_category_code = %(cat_code)s,
                frc_ctopup_number = %(ctop)s,
                frc_ctopup_number_mpin = %(mpin)s
            WHERE caf_serial_no = %(caf)s;
        """, {
            "gsm": r["gsmnumber"],
            "circle": r["circle_code"],
            "csc": r["de_csccode"],
            "plan_name": r["frc_plan_name"],
            "plan_code": r["frc_plan_code"],
            "cat_code": r["frc_category_code"],
            "ctop": r["frc_ctopup_number"],
            "mpin": r["frc_ctopup_number_mpin"],
            "caf": r["caf_serial_no"],
        })

    # cos_bcd_dkyc
    for r in pg_backup["cos_bcd_dkyc"]:
        pg_cur.execute("""
            UPDATE public.cos_bcd_dkyc
            SET gsmnumber = %(gsm)s,
                caf_serial_no = %(caf)s,
                circle_code = %(circle)s,
                de_csccode = %(csc)s,
                tariff_plan = %(plan)s,
                parent_ctopup_number = %(ctop)s,
                mpin = %(mpin)s
            WHERE id = %(id)s;
        """, {
            "gsm": r["gsmnumber"],
            "caf": r["caf_serial_no"],
            "circle": r["circle_code"],
            "csc": r["de_csccode"],
            "plan": r["tariff_plan"],
            "ctop": r["parent_ctopup_number"],
            "mpin": r["mpin"],
            "id": r["id"],
        })

    # frc_plan_table
    print("[4/4] Restoring PostgreSQL frc_plan_table...")
    pg_cur.execute("DELETE FROM public.frc_plan_table WHERE plan_code = '1002000' AND circle_code = '2';")
    pg_cur.execute("""
        UPDATE public.frc_plan_table
        SET end_date = '2026-09-12'
        WHERE plan_code = '1002000' AND circle_code IN ('55', '56', '59', '60', '61', '62', '64', '65');
    """)
    pg_cur.execute("""
        UPDATE public.frc_plan_table
        SET end_date = '2026-01-31'
        WHERE plan_code = '1002000' AND circle_code = '9999';
    """)
    pg_conn.commit()
    print("  PostgreSQL restored successfully.")

    ora_cur.close()
    ora_conn.close()
    pg_cur.close()
    pg_conn.close()
    print("RESTORE COMPLETE.")


if __name__ == "__main__":
    execute_restore()
