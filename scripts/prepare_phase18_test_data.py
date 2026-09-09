"""Phase 18A: Prepare 10 Matching Local BCD Records for Phase 18 Controlled Pilot.

This script executes LOCAL DEVELOPMENT ONLY data preparation:
1. Backs up existing state of target rows in Oracle and PostgreSQL (saved in docs/snapshots/).
2. Updates Oracle CAF_ADMIN.BCD for exactly 10 existing NZ rows to FRC_FLOW_STATUS='NP', FRC_REQID=NULL.
3. Updates PostgreSQL public.cos_bcd (5 EKYC) and public.cos_bcd_dkyc (5 DKYC).
4. Ensures public.frc_plan_table has active ₹1 plan across all NZ circles.
5. Performs cross-database validation, Q019 validation, and Q022 validation.
6. Asserts frc_pyro_request_data remains 0.
"""

import json
import os
import sys
from decimal import Decimal
from typing import Dict, List, Any

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, REPO_ROOT)

import oracledb
import psycopg2
import psycopg2.extras

from app.config import settings
from app.db.oracle import fetch_eligible_bcd_records, init_oracle_pool, close_oracle_pool
from app.db.postgres import fetch_cos_bcd_for_gsms, init_pg_pool, close_pg_pool


# ── Safety Check ─────────────────────────────────────────────────────────────
def verify_safety_guards() -> None:
    print("[1/6] Verifying safety guards & environment coordinates...")
    assert "docker" in settings.oracle_dsn or "localhost" in settings.oracle_dsn or "127.0.0.1" in settings.oracle_dsn, \
        f"ABORT: Non-local Oracle DSN: {settings.oracle_dsn}"
    assert settings.pg_host in ("localhost", "127.0.0.1"), \
        f"ABORT: Non-local PostgreSQL host: {settings.pg_host}"
    print(f"  Oracle DSN: {settings.oracle_dsn} (Verified local)")
    print(f"  PostgreSQL: {settings.pg_host}:{settings.pg_port}/{settings.pg_database} (Verified local)")


# ── Baseline Original Snapshots ──────────────────────────────────────────────
BASELINE_ORACLE_BACKUP = [
  {
    "GSMNUMBER": "6826185828",
    "CAF_SERIAL_NO": "BEC4018706",
    "CIRCLE_CODE": 2,
    "ACTIVATION_STATUS": "C",
    "HLR_DATE": "2026-08-13 18:48:15",
    "FRC_FLOW_STATUS": "RQ",
    "FRC_REQID": 2864984,
    "DE_CSCCODE": "DL07CSC01"
  },
  {
    "GSMNUMBER": "8988076675",
    "CAF_SERIAL_NO": "BEC4024252",
    "CIRCLE_CODE": 55,
    "ACTIVATION_STATUS": "C",
    "HLR_DATE": "2026-08-13 20:28:14",
    "FRC_FLOW_STATUS": "RQ",
    "FRC_REQID": 2868906,
    "DE_CSCCODE": "HPSOLDSA1629781"
  },
  {
    "GSMNUMBER": "8988246286",
    "CAF_SERIAL_NO": "BEC4015117",
    "CIRCLE_CODE": 55,
    "ACTIVATION_STATUS": "C",
    "HLR_DATE": "2026-08-13 18:33:49",
    "FRC_FLOW_STATUS": "RQ",
    "FRC_REQID": 2864729,
    "DE_CSCCODE": "CSCSOL"
  },
  {
    "GSMNUMBER": "7380036199",
    "CAF_SERIAL_NO": "BEC4037604",
    "CIRCLE_CODE": 56,
    "ACTIVATION_STATUS": "C",
    "HLR_DATE": "2026-08-14 13:48:08",
    "FRC_FLOW_STATUS": "RQ",
    "FRC_REQID": 2879904,
    "DE_CSCCODE": "PBJALDSA1585074"
  },
  {
    "GSMNUMBER": "6375250315",
    "CAF_SERIAL_NO": "BEC4022792",
    "CIRCLE_CODE": 59,
    "ACTIVATION_STATUS": "C",
    "HLR_DATE": "2026-08-17 21:50:52",
    "FRC_FLOW_STATUS": "RQ",
    "FRC_REQID": 2977829,
    "DE_CSCCODE": "RJ14127"
  },
  {
    "GSMNUMBER": "7317297365",
    "CAF_SERIAL_NO": "BEC4015231",
    "CIRCLE_CODE": 60,
    "ACTIVATION_STATUS": "C",
    "HLR_DATE": "2026-08-17 23:17:43",
    "FRC_FLOW_STATUS": "RQ",
    "FRC_REQID": 2980470,
    "DE_CSCCODE": "UE11104"
  },
  {
    "GSMNUMBER": "8278247741",
    "CAF_SERIAL_NO": "BEC4017338",
    "CIRCLE_CODE": 61,
    "ACTIVATION_STATUS": "C",
    "HLR_DATE": "2026-08-13 18:33:43",
    "FRC_FLOW_STATUS": "RQ",
    "FRC_REQID": 2864874,
    "DE_CSCCODE": "HR08122"
  },
  {
    "GSMNUMBER": "7248011700",
    "CAF_SERIAL_NO": "BEC4023891",
    "CIRCLE_CODE": 62,
    "ACTIVATION_STATUS": "C",
    "HLR_DATE": "2026-08-17 23:13:27",
    "FRC_FLOW_STATUS": "RQ",
    "FRC_REQID": 2978646,
    "DE_CSCCODE": "UW01102"
  },
  {
    "GSMNUMBER": "7579017985",
    "CAF_SERIAL_NO": "BEC3988493",
    "CIRCLE_CODE": 64,
    "ACTIVATION_STATUS": "C",
    "HLR_DATE": "2026-08-13 09:43:18",
    "FRC_FLOW_STATUS": "RQ",
    "FRC_REQID": 2845429,
    "DE_CSCCODE": "UL01110"
  },
  {
    "GSMNUMBER": "9419276386",
    "CAF_SERIAL_NO": "BEC4022891",
    "CIRCLE_CODE": 65,
    "ACTIVATION_STATUS": "C",
    "HLR_DATE": "2026-08-13 21:18:06",
    "FRC_FLOW_STATUS": "RQ",
    "FRC_REQID": 2869729,
    "DE_CSCCODE": "JKUDHDSA1735974"
  }
]

BASELINE_PG_BACKUP = {
  "cos_bcd": [
    {
      "ctid": "(0,1)",
      "gsmnumber": "",
      "caf_serial_no": "E5486350929",
      "circle_code": None,
      "de_csccode": "9188767335",
      "frc_plan_name": "",
      "frc_plan_code": "",
      "frc_category_code": "",
      "frc_ctopup_number": "",
      "frc_ctopup_number_mpin": ""
    },
    {
      "ctid": "(0,2)",
      "gsmnumber": "00000000",
      "caf_serial_no": "BEC0465779",
      "circle_code": "50",
      "de_csccode": "TESTDSA145361",
      "frc_plan_name": "",
      "frc_plan_code": "",
      "frc_category_code": "",
      "frc_ctopup_number": "",
      "frc_ctopup_number_mpin": ""
    },
    {
      "ctid": "(0,3)",
      "gsmnumber": "00000000",
      "caf_serial_no": "BEC0466026",
      "circle_code": "50",
      "de_csccode": "TESTDSA145361",
      "frc_plan_name": "",
      "frc_plan_code": "",
      "frc_category_code": "",
      "frc_ctopup_number": "",
      "frc_ctopup_number_mpin": ""
    },
    {
      "ctid": "(0,4)",
      "gsmnumber": "0000000000",
      "caf_serial_no": "BEC0448253",
      "circle_code": "50",
      "de_csccode": "TESTDSA145361",
      "frc_plan_name": "",
      "frc_plan_code": "",
      "frc_category_code": "",
      "frc_ctopup_number": "",
      "frc_ctopup_number_mpin": ""
    },
    {
      "ctid": "(0,5)",
      "gsmnumber": "0000000000",
      "caf_serial_no": "BEC0401942",
      "circle_code": "50",
      "de_csccode": "TESTDSA145361",
      "frc_plan_name": "PREPAID-FRC-1",
      "frc_plan_code": "1002000",
      "frc_category_code": "1001",
      "frc_ctopup_number": "9447666700",
      "frc_ctopup_number_mpin": "123456"
    }
  ],
  "cos_bcd_dkyc": [
    {
      "id": 48,
      "gsmnumber": "9495950452",
      "caf_serial_no": "BDC0000090",
      "circle_code": "50",
      "de_csccode": "KETVMCTOCSC",
      "tariff_plan": "FRC-1",
      "parent_ctopup_number": "9497802714",
      "mpin": "123456"
    },
    {
      "id": 49,
      "gsmnumber": "9447470864",
      "caf_serial_no": "BDC0000091",
      "circle_code": "50",
      "de_csccode": "KECLTKPTCSR",
      "tariff_plan": "FRC-1",
      "parent_ctopup_number": "9495656279",
      "mpin": "123456"
    },
    {
      "id": 50,
      "gsmnumber": "9482102662",
      "caf_serial_no": "BDC0000092",
      "circle_code": "53",
      "de_csccode": "KT01115",
      "tariff_plan": "FRC-249",
      "parent_ctopup_number": "9448622220",
      "mpin": "123456"
    },
    {
      "id": 51,
      "gsmnumber": "8300289728",
      "caf_serial_no": "BDC0000093",
      "circle_code": "54",
      "de_csccode": "TN10106",
      "tariff_plan": "FRC-1",
      "parent_ctopup_number": "7598058579",
      "mpin": "123456"
    },
    {
      "id": 52,
      "gsmnumber": "8985729425",
      "caf_serial_no": "BDC0000094",
      "circle_code": "41",
      "de_csccode": "AP0748",
      "tariff_plan": "FRC-249",
      "parent_ctopup_number": "9491238422",
      "mpin": "123456"
    }
  ]
}

# ── Target Test Population Specification ─────────────────────────────────────
# 10 Real Oracle identities across all 9 NZ circles
SELECTED_POPULATION = [
    # EKYC (5 rows)
    {
        "index": 1,
        "kyc_mode": "EKYC",
        "circle_code": 2,
        "gsmnumber": "6826185828",
        "caf_serial_no": "BEC4018706",
        "de_csccode": "DL07CSC01",
        "ctopupno": "6026970352",
        "pos_unique_code": "71061972SEKHETIA",
        "plan_code": "1002000",
        "plan_name": "PREPAID-FRC-1",
        "category_code": "1001",
        "mpin": "123456",
    },
    {
        "index": 2,
        "kyc_mode": "EKYC",
        "circle_code": 55,
        "gsmnumber": "8988076675",
        "caf_serial_no": "BEC4024252",
        "de_csccode": "HPSOLDSA1629781",
        "ctopupno": "6026971146",
        "pos_unique_code": "75701974TAPAANTA",
        "plan_code": "1002000",
        "plan_name": "PREPAID-FRC-1",
        "category_code": "1001",
        "mpin": "123456",
    },
    {
        "index": 3,
        "kyc_mode": "EKYC",
        "circle_code": 56,
        "gsmnumber": "7380036199",
        "caf_serial_no": "BEC4037604",
        "de_csccode": "PBJALDSA1585074",
        "ctopupno": "6026972541",
        "pos_unique_code": "83161979PRASKSHI",
        "plan_code": "1002000",
        "plan_name": "PREPAID-FRC-1",
        "category_code": "1001",
        "mpin": "123456",
    },
    {
        "index": 4,
        "kyc_mode": "EKYC",
        "circle_code": 59,
        "gsmnumber": "6375250315",
        "caf_serial_no": "BEC4022792",
        "de_csccode": "RJ14127",
        "ctopupno": "6026973341",
        "pos_unique_code": "11892003BABISLAM",
        "plan_code": "1002000",
        "plan_name": "PREPAID-FRC-1",
        "category_code": "1001",
        "mpin": "123456",
    },
    {
        "index": 5,
        "kyc_mode": "EKYC",
        "circle_code": 60,
        "gsmnumber": "7317297365",
        "caf_serial_no": "BEC4015231",
        "de_csccode": "UE11104",
        "ctopupno": "6026974293",
        "pos_unique_code": "23101984ASHILAHI",
        "plan_code": "1002000",
        "plan_name": "PREPAID-FRC-1",
        "category_code": "1001",
        "mpin": "123456",
    },
    # DKYC (5 rows)
    {
        "index": 6,
        "kyc_mode": "DKYC",
        "circle_code": 55,
        "gsmnumber": "8988246286",
        "caf_serial_no": "BEC4015117",
        "de_csccode": "CSCSOL",
        "ctopupno": "6026974612",
        "pos_unique_code": "45741988HEMAADAS",
        "plan_code": "1002000",
        "plan_name": "PREPAID-FRC-1",
        "category_code": "1001",
        "mpin": "123456",
        "dkyc_id": 48,
    },
    {
        "index": 7,
        "kyc_mode": "DKYC",
        "circle_code": 61,
        "gsmnumber": "8278247741",
        "caf_serial_no": "BEC4017338",
        "de_csccode": "HR08122",
        "ctopupno": "6026974952",
        "pos_unique_code": "84621990KRISMILI",
        "plan_code": "1002000",
        "plan_name": "PREPAID-FRC-1",
        "category_code": "1001",
        "mpin": "123456",
        "dkyc_id": 49,
    },
    {
        "index": 8,
        "kyc_mode": "DKYC",
        "circle_code": 62,
        "gsmnumber": "7248011700",
        "caf_serial_no": "BEC4023891",
        "de_csccode": "UW01102",
        "ctopupno": "6026975116",
        "pos_unique_code": "85981978REKHORAH",
        "plan_code": "1002000",
        "plan_name": "PREPAID-FRC-1",
        "category_code": "1001",
        "mpin": "123456",
        "dkyc_id": 50,
    },
    {
        "index": 9,
        "kyc_mode": "DKYC",
        "circle_code": 64,
        "gsmnumber": "7579017985",
        "caf_serial_no": "BEC3988493",
        "de_csccode": "UL01110",
        "ctopupno": "7382006121",
        "pos_unique_code": "17911958CHINSHNA",
        "plan_code": "1002000",
        "plan_name": "PREPAID-FRC-1",
        "category_code": "1001",
        "mpin": "123456",
        "dkyc_id": 51,
    },
    {
        "index": 10,
        "kyc_mode": "DKYC",
        "circle_code": 65,
        "gsmnumber": "9419276386",
        "caf_serial_no": "BEC4022891",
        "de_csccode": "JKUDHDSA1735974",
        "ctopupno": "7382044973",
        "pos_unique_code": "19641972KOPPNATH",
        "plan_code": "1002000",
        "plan_name": "PREPAID-FRC-1",
        "category_code": "1001",
        "mpin": "123456",
        "dkyc_id": 52,
    },
]


def execute_preparation() -> None:
    verify_safety_guards()

    snapshots_dir = os.path.join(REPO_ROOT, "docs", "snapshots")
    os.makedirs(snapshots_dir, exist_ok=True)

    # ── Step 2: Save Baseline Snapshots ──────────────────────────────────────────
    print("[2/6] Ensuring baseline snapshots are stored...")
    ora_snap_path = os.path.join(snapshots_dir, "oracle_bcd_phase18_backup.json")
    if not os.path.exists(ora_snap_path):
        with open(ora_snap_path, "w") as f:
            json.dump(BASELINE_ORACLE_BACKUP, f, indent=2)
        print(f"  Saved {ora_snap_path}")

    pg_snap_path = os.path.join(snapshots_dir, "postgres_phase18_backup.json")
    if not os.path.exists(pg_snap_path):
        with open(pg_snap_path, "w") as f:
            json.dump(BASELINE_PG_BACKUP, f, indent=2)
        print(f"  Saved {pg_snap_path}")

    # ── Step 3: Apply Targeted Mutations ─────────────────────────────────────────
    print("[3/6] Applying targeted mutations to Oracle & PostgreSQL...")

    # Connect Oracle
    ora_conn = oracledb.connect(
        user=settings.oracle_user,
        password=settings.oracle_password,
        dsn=settings.oracle_dsn,
    )
    ora_cur = ora_conn.cursor()

    # 3a. Update Oracle CAF_ADMIN.BCD for 10 rows: FRC_FLOW_STATUS = 'NP', FRC_REQID = NULL
    for item in SELECTED_POPULATION:
        ora_cur.execute("""
            UPDATE CAF_ADMIN.BCD
            SET FRC_FLOW_STATUS = 'NP',
                FRC_REQID = NULL
            WHERE GSMNUMBER = :g
              AND CAF_SERIAL_NO = :caf
              AND CIRCLE_CODE = :c
        """, {"g": item["gsmnumber"], "caf": item["caf_serial_no"], "c": item["circle_code"]})
        assert ora_cur.rowcount == 1, f"Expected 1 row updated for {item['gsmnumber']}, got {ora_cur.rowcount}"
    ora_conn.commit()
    print("  Oracle: 10 rows successfully set to FRC_FLOW_STATUS='NP', FRC_REQID=NULL.")

    # Connect Postgres
    pg_conn = psycopg2.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        dbname=settings.pg_database,
        user=settings.pg_user,
        password=settings.pg_password,
    )
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # 3b. Ensure cos_bcd.circle_code column type is character varying(60)
    pg_cur.execute("""
        SELECT data_type FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'cos_bcd' AND column_name = 'circle_code';
    """)
    col_type = pg_cur.fetchone()["data_type"]
    if col_type != "character varying":
        pg_cur.execute("ALTER TABLE public.cos_bcd ALTER COLUMN circle_code TYPE character varying(60);")
        pg_conn.commit()
        print("  PostgreSQL: cos_bcd.circle_code altered to character varying(60).")

    # 3c. Update Postgres cos_bcd (5 EKYC rows)
    # Target by matching gsmnumber/caf_serial_no if already set, or target original rows
    original_cos_cafs = ["E5486350929", "BEC0465779", "BEC0466026", "BEC0448253", "BEC0401942"]
    for idx, item in enumerate([p for p in SELECTED_POPULATION if p["kyc_mode"] == "EKYC"]):
        orig_caf = original_cos_cafs[idx]
        pg_cur.execute("""
            UPDATE public.cos_bcd
            SET gsmnumber = %(gsm)s,
                caf_serial_no = %(caf)s,
                circle_code = %(circle)s,
                de_csccode = %(csc)s,
                frc_plan_name = %(plan_name)s,
                frc_plan_code = %(plan_code)s,
                frc_category_code = %(cat_code)s,
                frc_ctopup_number = %(ctop)s,
                frc_ctopup_number_mpin = %(mpin)s,
                live_photo_time = NOW()
            WHERE caf_serial_no = %(orig_caf)s OR (gsmnumber = %(gsm)s AND caf_serial_no = %(caf)s);
        """, {
            "gsm": item["gsmnumber"],
            "caf": item["caf_serial_no"],
            "circle": str(item["circle_code"]),
            "csc": item["de_csccode"],
            "plan_name": item["plan_name"],
            "plan_code": item["plan_code"],
            "cat_code": item["category_code"],
            "ctop": item["ctopupno"],
            "mpin": item["mpin"],
            "orig_caf": orig_caf,
        })
        assert pg_cur.rowcount >= 1, f"Expected update for EKYC row {item['gsmnumber']}, got {pg_cur.rowcount}"

    # 3d. Update Postgres cos_bcd_dkyc (5 DKYC rows)
    for item in [p for p in SELECTED_POPULATION if p["kyc_mode"] == "DKYC"]:
        pg_cur.execute("""
            UPDATE public.cos_bcd_dkyc
            SET gsmnumber = %(gsm)s,
                caf_serial_no = %(caf)s,
                circle_code = %(circle)s,
                de_csccode = %(csc)s,
                tariff_plan = %(plan_name)s,
                parent_ctopup_number = %(ctop)s,
                mpin = %(mpin)s,
                customer_photo_time = NOW()
            WHERE id = %(id)s;
        """, {
            "gsm": item["gsmnumber"],
            "caf": item["caf_serial_no"],
            "circle": str(item["circle_code"]),
            "csc": item["de_csccode"],
            "plan_name": item["plan_name"],
            "ctop": item["ctopupno"],
            "mpin": item["mpin"],
            "id": item["dkyc_id"],
        })
        assert pg_cur.rowcount == 1, f"Expected 1 row updated in cos_bcd_dkyc for id={item['dkyc_id']}"

    # 3e. Ensure frc_plan_table has Circle 2 and active end_dates for all NZ circles
    pg_cur.execute("SELECT id FROM public.frc_plan_table WHERE plan_code = '1002000' AND circle_code = '2';")
    c2_row = pg_cur.fetchone()
    if not c2_row:
        pg_cur.execute("SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM public.frc_plan_table;")
        next_id = pg_cur.fetchone()["next_id"]
        pg_cur.execute("""
            INSERT INTO public.frc_plan_table (id, plan_name, plan_code, category_code, start_date, end_date, circle_code, frc_amount)
            VALUES (%(id)s, 'PREPAID-FRC-1', '1002000', '1001', '2026-08-01', NULL, '2', 1);
        """, {"id": next_id})
        print(f"  PostgreSQL: Inserted circle 2 into frc_plan_table (id={next_id}).")

    # Set end_date = NULL for all circle-specific 1002000 plans in NZ
    pg_cur.execute("""
        UPDATE public.frc_plan_table
        SET end_date = NULL
        WHERE plan_code = '1002000'
          AND circle_code IN ('2', '55', '56', '59', '60', '61', '62', '64', '65');
    """)
    # Ensure 9999 has expired end_date to prevent duplicate joins on EKYC
    pg_cur.execute("""
        UPDATE public.frc_plan_table
        SET end_date = '2026-01-31'
        WHERE plan_code = '1002000'
          AND circle_code = '9999';
    """)
    pg_conn.commit()
    print("  PostgreSQL: Updated cos_bcd (5), cos_bcd_dkyc (5), and frc_plan_table successfully.")

    # ── Step 4: Cross-Database Identity Check ────────────────────────────────────
    print("[4/6] Performing cross-database identity validation...")
    print(f"{'#':<3} | {'Oracle GSM':<11} | {'Oracle CAF':<11} | {'Circle':<6} | {'PG EKYC GSM':<11} | {'PG EKYC CAF':<11} | {'PG DKYC GSM':<11} | {'PG DKYC CAF':<11}")
    print("-" * 86)
    for item in SELECTED_POPULATION:
        idx = item["index"]
        c = item["circle_code"]
        g = item["gsmnumber"]
        caf = item["caf_serial_no"]

        # Check cos_bcd
        pg_cur.execute("SELECT gsmnumber, caf_serial_no FROM public.cos_bcd WHERE gsmnumber = %s AND caf_serial_no = %s;", (g, caf))
        ekyc_match = pg_cur.fetchone()
        ekyc_gsm = ekyc_match["gsmnumber"] if ekyc_match else "-"
        ekyc_caf = ekyc_match["caf_serial_no"] if ekyc_match else "-"

        # Check cos_bcd_dkyc
        pg_cur.execute("SELECT gsmnumber, caf_serial_no FROM public.cos_bcd_dkyc WHERE gsmnumber = %s AND caf_serial_no = %s;", (g, caf))
        dkyc_match = pg_cur.fetchone()
        dkyc_gsm = dkyc_match["gsmnumber"] if dkyc_match else "-"
        dkyc_caf = dkyc_match["caf_serial_no"] if dkyc_match else "-"

        print(f"{idx:<3} | {g:<11} | {caf:<11} | {c:<6} | {ekyc_gsm:<11} | {ekyc_caf:<11} | {dkyc_gsm:<11} | {dkyc_caf:<11}")

        if item["kyc_mode"] == "EKYC":
            assert ekyc_gsm == g and ekyc_caf == caf, f"Mismatch in EKYC for {g}"
            assert dkyc_gsm == "-" and dkyc_caf == "-", f"Duplicate found in DKYC for {g}"
        else:
            assert dkyc_gsm == g and dkyc_caf == caf, f"Mismatch in DKYC for {g}"
            assert ekyc_gsm == "-" and ekyc_caf == "-", f"Duplicate found in EKYC for {g}"

    # ── Step 5: Read-Only Q019 & Q022 Validation ──────────────────────────────────
    print("[5/6] Executing Q019 and Q022 read-only query validations...")
    init_oracle_pool()
    init_pg_pool()

    try:
        nz_circles = [2, 55, 56, 59, 60, 61, 62, 64, 65]
        q019_rows = fetch_eligible_bcd_records(fetch_size=500, circle_codes=nz_circles)
        print(f"  Q019 Candidates returned: {len(q019_rows)} (Expected: exactly 10)")
        assert len(q019_rows) == 10, f"Q019 candidate count mismatch: expected 10, got {len(q019_rows)}"
        for r in q019_rows:
            assert r["CIRCLE_CODE"] in nz_circles, f"Non-NZ circle {r['CIRCLE_CODE']} in Q019 result"

        candidate_gsms = [r["GSMNUMBER"] for r in q019_rows]
        q022_rows = fetch_cos_bcd_for_gsms(candidate_gsms, circle_codes=nz_circles)
        print(f"  Q022 Matches returned: {len(q022_rows)} (Expected: exactly 10)")
        assert len(q022_rows) == 10, f"Q022 match count mismatch: expected 10, got {len(q022_rows)}"

        ekyc_count = sum(1 for r in q022_rows if r["kyc_mode"] == "EKYC")
        dkyc_count = sum(1 for r in q022_rows if r["kyc_mode"] == "DKYC")
        print(f"  Q022 Enrichment breakdown: {ekyc_count} EKYC, {dkyc_count} DKYC")
        assert ekyc_count == 5, f"Expected 5 EKYC, got {ekyc_count}"
        assert dkyc_count == 5, f"Expected 5 DKYC, got {dkyc_count}"

        for r in q022_rows:
            assert r["frcamt"] == Decimal("1"), f"Expected frcamt=1, got {r['frcamt']} for GSM {r['gsmnumber']}"
            assert r["vendorid"] is not None and len(r["vendorid"]) > 0, f"Missing vendorid for GSM {r['gsmnumber']}"
            assert r["ctopup_number"] is not None and len(r["ctopup_number"]) > 0, f"Missing ctopup_number for GSM {r['gsmnumber']}"
            assert r["mpin_raw"] is not None and len(r["mpin_raw"]) >= 4, f"Invalid MPIN for GSM {r['gsmnumber']}"

        # ── Step 6: Verify frc_pyro_request_data is 0 ─────────────────────────────
        print("[6/6] Verifying frc_pyro_request_data has 0 rows...")
        pg_cur.execute("SELECT COUNT(*) AS cnt FROM public.frc_pyro_request_data;")
        staging_count = pg_cur.fetchone()["cnt"]
        print(f"  frc_pyro_request_data row count: {staging_count} (Expected: 0)")
        assert staging_count == 0, f"frc_pyro_request_data is not clean! Got {staging_count} rows"

        # Generate SQL restore file
        restore_sql_path = os.path.join(snapshots_dir, "restore_phase18_data.sql")
        with open(restore_sql_path, "w") as f:
            f.write("-- ====================================================================\n")
            f.write("-- RESTORE SCRIPT FOR PHASE 18 TEST DATA PREPARATION\n")
            f.write("-- Generated by prepare_phase18_test_data.py\n")
            f.write("-- ====================================================================\n\n")

            f.write("-- 1. RESTORE ORACLE CAF_ADMIN.BCD\n")
            for r in BASELINE_ORACLE_BACKUP:
                req_val = r["FRC_REQID"] if r["FRC_REQID"] is not None else "NULL"
                f.write(f"UPDATE CAF_ADMIN.BCD SET FRC_FLOW_STATUS = '{r['FRC_FLOW_STATUS']}', FRC_REQID = {req_val} ")
                f.write(f"WHERE GSMNUMBER = '{r['GSMNUMBER']}' AND CAF_SERIAL_NO = '{r['CAF_SERIAL_NO']}' AND CIRCLE_CODE = {r['CIRCLE_CODE']};\n")
            f.write("COMMIT;\n\n")

            f.write("-- 2. RESTORE POSTGRESQL cos_bcd\n")
            for r in BASELINE_PG_BACKUP["cos_bcd"]:
                f.write(f"UPDATE public.cos_bcd SET gsmnumber = '{r['gsmnumber']}', circle_code = {'NULL' if r['circle_code'] is None else repr(str(r['circle_code']))}, ")
                f.write(f"de_csccode = {repr(r['de_csccode'])}, frc_plan_name = {repr(r['frc_plan_name'])}, frc_plan_code = {repr(r['frc_plan_code'])}, ")
                f.write(f"frc_category_code = {repr(r['frc_category_code'])}, frc_ctopup_number = {repr(r['frc_ctopup_number'])}, ")
                f.write(f"frc_ctopup_number_mpin = {repr(r['frc_ctopup_number_mpin'])} WHERE caf_serial_no = '{r['caf_serial_no']}';\n")
            f.write("\n")

            f.write("-- 3. RESTORE POSTGRESQL cos_bcd_dkyc\n")
            for r in BASELINE_PG_BACKUP["cos_bcd_dkyc"]:
                f.write(f"UPDATE public.cos_bcd_dkyc SET gsmnumber = '{r['gsmnumber']}', caf_serial_no = '{r['caf_serial_no']}', ")
                f.write(f"circle_code = {repr(str(r['circle_code']))}, de_csccode = {repr(r['de_csccode'])}, tariff_plan = {repr(r['tariff_plan'])}, ")
                f.write(f"parent_ctopup_number = {repr(r['parent_ctopup_number'])}, mpin = {repr(r['mpin'])} WHERE id = {r['id']};\n")
            f.write("\n")

            f.write("-- 4. RESTORE POSTGRESQL frc_plan_table\n")
            f.write("DELETE FROM public.frc_plan_table WHERE plan_code = '1002000' AND circle_code = '2';\n")
            f.write("UPDATE public.frc_plan_table SET end_date = '2026-09-12' WHERE plan_code = '1002000' AND circle_code IN ('55', '56', '59', '60', '61', '62', '64', '65');\n")
            f.write("UPDATE public.frc_plan_table SET end_date = '2026-01-31' WHERE plan_code = '1002000' AND circle_code = '9999';\n")

        print(f"  Restore SQL script written to: {restore_sql_path}")
        print("\n============================================================")
        print("PHASE 18A TEST DATA PREPARATION — READY FOR PILOT")
        print("============================================================")

    finally:
        close_oracle_pool()
        close_pg_pool()
        ora_cur.close()
        ora_conn.close()
        pg_cur.close()
        pg_conn.close()


if __name__ == "__main__":
    execute_preparation()
