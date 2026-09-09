# Phase 18A — Test Data Preparation Report: 10 Matching Local Oracle BCD Records

## 1. Executive Summary & Verification Context

This report documents the preparation and verification of **10 coherent local test identities** spanning local Oracle (`CAF_ADMIN.BCD`) and local PostgreSQL (`public.cos_bcd`, `public.cos_bcd_dkyc`, `public.ctop_master`, `public.frc_plan_table`).

In earlier testing, synthetic dummy GSMs and CAFs were identified as disconnecting the real local Oracle BCD records from PostgreSQL tables. Phase 18A resolves this by selecting **10 real existing rows from local Oracle `CAF_ADMIN.BCD`** spanning all 9 Northern Zone circles, updating their eligibility flags safely, and matching them into local PostgreSQL `cos_bcd` and `cos_bcd_dkyc` with valid CTOP dealers and ₹1 plans.

### Environment Confirmation
- **Task Scope:** Local Development Environment ONLY.
- **Oracle Target:** `host.docker.internal:1521/xepdb1` (`CAF_ADMIN.BCD`).
- **PostgreSQL Target:** `localhost:5432/postgres`.
- **Git Branch:** `feature/zonewise-migration`.
- **Pyro Gate:** No Pyro API transactions were invoked.
- **Scheduler State:** Schedulers remain disabled (`ENABLE_SCHEDULER=false`).
- **Staging State:** `public.frc_pyro_request_data` has 0 rows.

---

## 2. Selected Local Oracle BCD Identities

The 10 identities were selected directly from existing rows in local Oracle `CAF_ADMIN.BCD`. They represent a comprehensive spread across all 9 circles of the Northern Zone (`2, 55, 56, 59, 60, 61, 62, 64, 65`):

| # | Circle Code | Circle Name | GSMNUMBER | CAF_SERIAL_NO | Original DE_CSCCODE | KYC Mode |
|---|---:|---|---|---|---|---|
| 1 | 2 | UP West | 6826185828 | BEC4018706 | DL07CSC01 | EKYC |
| 2 | 55 | Himachal Pradesh | 8988076675 | BEC4024252 | HPSOLDSA1629781 | EKYC |
| 3 | 56 | Haryana | 7380036199 | BEC4037604 | PBJALDSA1585074 | EKYC |
| 4 | 59 | Jammu & Kashmir | 6375250315 | BEC4022792 | RJ14127 | EKYC |
| 5 | 60 | Punjab | 7317297365 | BEC4015231 | UE11104 | EKYC |
| 6 | 55 | Himachal Pradesh | 8988246286 | BEC4015117 | CSCSOL | DKYC |
| 7 | 61 | Uttarakhand | 8278247741 | BEC4017338 | HR08122 | DKYC |
| 8 | 62 | UP East | 7248011700 | BEC4023891 | UW01102 | DKYC |
| 9 | 64 | Rajasthan | 7579017985 | BEC3988493 | UL01110 | DKYC |
| 10 | 65 | Haryana (Alt) | 9419276386 | BEC4022891 | JKUDHDSA1735974 | DKYC |

---

## 3. Before & After State: Oracle `CAF_ADMIN.BCD`

Before mutation, a full baseline snapshot of the 10 rows was taken and saved to [`docs/snapshots/oracle_bcd_phase18_backup.json`](file:///D:/pyro/pyro_auto_frc/docs/snapshots/oracle_bcd_phase18_backup.json).

### State Transitions:
- `ACTIVATION_STATUS`: Remained `'C'` (no change).
- `HLR_FINAL_ACT_DATE`: Preserved existing timestamps (all non-null).
- `FRC_FLOW_STATUS`: Transitioned from `'RQ'` (processed in historical demo runs) to `'NP'` (New / Pending FRC).
- `FRC_REQID`: Reset from historical request IDs to `NULL`.

| # | GSMNUMBER | CAF_SERIAL_NO | Circle | Before Status | Before ReqId | After Status | After ReqId |
|---|---|---|---:|---|---:|---|:---:|
| 1 | 6826185828 | BEC4018706 | 2 | `RQ` | 2864984 | `NP` | `NULL` |
| 2 | 8988076675 | BEC4024252 | 55 | `RQ` | 2868906 | `NP` | `NULL` |
| 3 | 8988246286 | BEC4015117 | 55 | `RQ` | 2864729 | `NP` | `NULL` |
| 4 | 7380036199 | BEC4037604 | 56 | `RQ` | 2879904 | `NP` | `NULL` |
| 5 | 6375250315 | BEC4022792 | 59 | `RQ` | 2977829 | `NP` | `NULL` |
| 6 | 7317297365 | BEC4015231 | 60 | `RQ` | 2980470 | `NP` | `NULL` |
| 7 | 8278247741 | BEC4017338 | 61 | `RQ` | 2864874 | `NP` | `NULL` |
| 8 | 7248011700 | BEC4023891 | 62 | `RQ` | 2978646 | `NP` | `NULL` |
| 9 | 7579017985 | BEC3988493 | 64 | `RQ` | 2845429 | `NP` | `NULL` |
| 10 | 9419276386 | BEC4022891 | 65 | `RQ` | 2869729 | `NP` | `NULL` |

**Verification:**
- Exactly 10 rows were modified.
- No broad or wildcard updates were used (`WHERE GSMNUMBER = :g AND CAF_SERIAL_NO = :caf AND CIRCLE_CODE = :c`).
- All other rows in `CAF_ADMIN.BCD` remained completely untouched.

---

## 4. PostgreSQL Matching, CTOP, & Plan Relationships

Before mutation, baseline snapshots of the target rows in PostgreSQL `cos_bcd` and `cos_bcd_dkyc` were saved to [`docs/snapshots/postgres_phase18_backup.json`](file:///D:/pyro/pyro_auto_frc/docs/snapshots/postgres_phase18_backup.json).

### 4.1 EKYC Population (`public.cos_bcd`)
5 unused demo rows in `public.cos_bcd` with blank/zero GSMs (`E5486350929`, `BEC0465779`, `BEC0466026`, `BEC0448253`, `BEC0401942`) were updated to match Oracle identities 1–5:

| # | GSM | CAF | Circle | Plan Code | Plan Name | Category | CTOP Number | POS Vendor Code | MPIN Length |
|---|---|---|---:|---|---|---|---|---|:---:|
| 1 | 6826185828 | BEC4018706 | 2 | 1002000 | PREPAID-FRC-1 | 1001 | 6026970352 | 71061972SEKHETIA | 6 (Encrypted) |
| 2 | 8988076675 | BEC4024252 | 55 | 1002000 | PREPAID-FRC-1 | 1001 | 6026971146 | 75701974TAPAANTA | 6 (Encrypted) |
| 3 | 7380036199 | BEC4037604 | 56 | 1002000 | PREPAID-FRC-1 | 1001 | 6026972541 | 83161979PRASKSHI | 6 (Encrypted) |
| 4 | 6375250315 | BEC4022792 | 59 | 1002000 | PREPAID-FRC-1 | 1001 | 6026973341 | 11892003BABISLAM | 6 (Encrypted) |
| 5 | 7317297365 | BEC4015231 | 60 | 1002000 | PREPAID-FRC-1 | 1001 | 6026974293 | 23101984ASHILAHI | 6 (Encrypted) |

### 4.2 DKYC Population (`public.cos_bcd_dkyc`)
5 existing rows in `public.cos_bcd_dkyc` (`id` 48–52) were updated to match Oracle identities 6–10:

| # | ID | GSM | CAF | Circle | Tariff Plan | CTOP Number | POS Vendor Code | MPIN Length |
|---|---:|---|---|---:|---|---|---|:---:|
| 6 | 48 | 8988246286 | BEC4015117 | 55 | PREPAID-FRC-1 | 6026974612 | 45741988HEMAADAS | 6 (Encrypted) |
| 7 | 49 | 8278247741 | BEC4017338 | 61 | PREPAID-FRC-1 | 6026974952 | 84621990KRISMILI | 6 (Encrypted) |
| 8 | 50 | 7248011700 | BEC4023891 | 62 | PREPAID-FRC-1 | 6026975116 | 85981978REKHORAH | 6 (Encrypted) |
| 9 | 51 | 7579017985 | BEC3988493 | 64 | PREPAID-FRC-1 | 7382006121 | 17911958CHINSHNA | 6 (Encrypted) |
| 10 | 52 | 9419276386 | BEC4022891 | 65 | PREPAID-FRC-1 | 7382044973 | 19641972KOPPNATH | 6 (Encrypted) |

### 4.3 CTOP Dealer Integrity & Cartesian Explosion Avoidance
Each CTOP number selected from `public.ctop_master` was chosen from the set of dealer accounts where `COUNT(*) = 1` in `ctop_master`. This strictly eliminates 1-to-many Cartesian joins in Q022, guaranteeing exactly 1 enrichment record per candidate GSM.

### 4.4 Tariff Plan & FRC Amount (₹1)
In `public.frc_plan_table`:
- Added Circle 2 entry for plan `1002000` / `PREPAID-FRC-1` with `frc_amount = 1`, `category_code = '1001'`, `start_date = '2026-08-01'`, and `end_date = NULL`.
- Set `end_date = NULL` for all circle-specific `1002000` rows in NZ (`2, 55, 56, 59, 60, 61, 62, 64, 65`).
- Maintained `9999` with `end_date = '2026-01-31'` to ensure EKYC queries join uniquely on circle-specific rows rather than matching both the circle row and the fallback row.

---

## 5. Cross-Database Identity Check (Section 13)

The cross-database identity validation confirms zero identity mismatch across Oracle and PostgreSQL:

| # | Oracle GSM | Oracle CAF | Circle | PG EKYC GSM | PG EKYC CAF | PG DKYC GSM | PG DKYC CAF | Identity Match |
|---|---|---|---:|---|---|---|---|:---:|
| 1 | 6826185828 | BEC4018706 | 2 | 6826185828 | BEC4018706 | - | - | **MATCH** |
| 2 | 8988076675 | BEC4024252 | 55 | 8988076675 | BEC4024252 | - | - | **MATCH** |
| 3 | 7380036199 | BEC4037604 | 56 | 7380036199 | BEC4037604 | - | - | **MATCH** |
| 4 | 6375250315 | BEC4022792 | 59 | 6375250315 | BEC4022792 | - | - | **MATCH** |
| 5 | 7317297365 | BEC4015231 | 60 | 7317297365 | BEC4015231 | - | - | **MATCH** |
| 6 | 8988246286 | BEC4015117 | 55 | - | - | 8988246286 | BEC4015117 | **MATCH** |
| 7 | 8278247741 | BEC4017338 | 61 | - | - | 8278247741 | BEC4017338 | **MATCH** |
| 8 | 7248011700 | BEC4023891 | 62 | - | - | 7248011700 | BEC4023891 | **MATCH** |
| 9 | 7579017985 | BEC3988493 | 64 | - | - | 7579017985 | BEC3988493 | **MATCH** |
| 10 | 9419276386 | BEC4022891 | 65 | - | - | 9419276386 | BEC4022891 | **MATCH** |

---

## 6. Query Validations (Q019 & Q022)

The application read queries were executed against the live databases with `ENABLED_ZONES=NZ`:

### 6.1 Q019 Read-Only Discovery Validation
- Query executed: [`fetch_eligible_bcd_records(fetch_size=500, circle_codes=[2, 55, 56, 59, 60, 61, 62, 64, 65])`](file:///D:/pyro/pyro_auto_frc/app/db/oracle.py#L67).
- **Candidates Discovered:** Exactly **10** rows.
- **Zone Compliance:** 100% NZ (`[2, 55, 56, 59, 60, 61, 62, 64, 65]`). Zero non-NZ candidates.

### 6.2 Q022 Enrichment Validation
- Query executed: [`fetch_cos_bcd_for_gsms(candidate_gsms, circle_codes=[2, 55, 56, 59, 60, 61, 62, 64, 65])`](file:///D:/pyro/pyro_auto_frc/app/db/postgres.py#L176).
- **Enrichment Rows Returned:** Exactly **10** rows.
- **KYC Breakdown:** Exactly 5 EKYC and 5 DKYC.
- **Amount Check:** All 10 records have `frcamt = Decimal('1')`.
- **Vendor / CTOP Check:** All 10 records have non-empty `vendorid`, `ctopup_number`, and `vendormsisdn`.
- **MPIN Check:** Raw MPINs are present, 6 digits, and successfully encrypted via 3DES.

### 6.3 Staging Table Cleanliness
- Query: `SELECT COUNT(*) FROM public.frc_pyro_request_data;`
- **Count:** **0**. No staging rows have been manually created.

---

## 7. Automated Restore SQL

The complete restore script is persisted at [`docs/snapshots/restore_phase18_data.sql`](file:///D:/pyro/pyro_auto_frc/docs/snapshots/restore_phase18_data.sql) and executable via `python scripts/restore_phase18_test_data.py`:

```sql
-- 1. RESTORE ORACLE CAF_ADMIN.BCD
UPDATE CAF_ADMIN.BCD SET FRC_FLOW_STATUS = 'RQ', FRC_REQID = 2864984 WHERE GSMNUMBER = '6826185828' AND CAF_SERIAL_NO = 'BEC4018706' AND CIRCLE_CODE = 2;
UPDATE CAF_ADMIN.BCD SET FRC_FLOW_STATUS = 'RQ', FRC_REQID = 2868906 WHERE GSMNUMBER = '8988076675' AND CAF_SERIAL_NO = 'BEC4024252' AND CIRCLE_CODE = 55;
UPDATE CAF_ADMIN.BCD SET FRC_FLOW_STATUS = 'RQ', FRC_REQID = 2864729 WHERE GSMNUMBER = '8988246286' AND CAF_SERIAL_NO = 'BEC4015117' AND CIRCLE_CODE = 55;
UPDATE CAF_ADMIN.BCD SET FRC_FLOW_STATUS = 'RQ', FRC_REQID = 2879904 WHERE GSMNUMBER = '7380036199' AND CAF_SERIAL_NO = 'BEC4037604' AND CIRCLE_CODE = 56;
UPDATE CAF_ADMIN.BCD SET FRC_FLOW_STATUS = 'RQ', FRC_REQID = 2977829 WHERE GSMNUMBER = '6375250315' AND CAF_SERIAL_NO = 'BEC4022792' AND CIRCLE_CODE = 59;
UPDATE CAF_ADMIN.BCD SET FRC_FLOW_STATUS = 'RQ', FRC_REQID = 2980470 WHERE GSMNUMBER = '7317297365' AND CAF_SERIAL_NO = 'BEC4015231' AND CIRCLE_CODE = 60;
UPDATE CAF_ADMIN.BCD SET FRC_FLOW_STATUS = 'RQ', FRC_REQID = 2864874 WHERE GSMNUMBER = '8278247741' AND CAF_SERIAL_NO = 'BEC4017338' AND CIRCLE_CODE = 61;
UPDATE CAF_ADMIN.BCD SET FRC_FLOW_STATUS = 'RQ', FRC_REQID = 2978646 WHERE GSMNUMBER = '7248011700' AND CAF_SERIAL_NO = 'BEC4023891' AND CIRCLE_CODE = 62;
UPDATE CAF_ADMIN.BCD SET FRC_FLOW_STATUS = 'RQ', FRC_REQID = 2845429 WHERE GSMNUMBER = '7579017985' AND CAF_SERIAL_NO = 'BEC3988493' AND CIRCLE_CODE = 64;
UPDATE CAF_ADMIN.BCD SET FRC_FLOW_STATUS = 'RQ', FRC_REQID = 2869729 WHERE GSMNUMBER = '9419276386' AND CAF_SERIAL_NO = 'BEC4022891' AND CIRCLE_CODE = 65;
COMMIT;

-- 2. RESTORE POSTGRESQL cos_bcd
UPDATE public.cos_bcd SET gsmnumber = '', circle_code = NULL, de_csccode = '9188767335', frc_plan_name = '', frc_plan_code = '', frc_category_code = '', frc_ctopup_number = '', frc_ctopup_number_mpin = '' WHERE caf_serial_no = 'E5486350929';
UPDATE public.cos_bcd SET gsmnumber = '00000000', circle_code = '50', de_csccode = 'TESTDSA145361', frc_plan_name = '', frc_plan_code = '', frc_category_code = '', frc_ctopup_number = '', frc_ctopup_number_mpin = '' WHERE caf_serial_no = 'BEC0465779';
UPDATE public.cos_bcd SET gsmnumber = '00000000', circle_code = '50', de_csccode = 'TESTDSA145361', frc_plan_name = '', frc_plan_code = '', frc_category_code = '', frc_ctopup_number = '', frc_ctopup_number_mpin = '' WHERE caf_serial_no = 'BEC0466026';
UPDATE public.cos_bcd SET gsmnumber = '0000000000', circle_code = '50', de_csccode = 'TESTDSA145361', frc_plan_name = '', frc_plan_code = '', frc_category_code = '', frc_ctopup_number = '', frc_ctopup_number_mpin = '' WHERE caf_serial_no = 'BEC0448253';
UPDATE public.cos_bcd SET gsmnumber = '0000000000', circle_code = '50', de_csccode = 'TESTDSA145361', frc_plan_name = 'PREPAID-FRC-1', frc_plan_code = '1002000', frc_category_code = '1001', frc_ctopup_number = '9447666700', frc_ctopup_number_mpin = '123456' WHERE caf_serial_no = 'BEC0401942';

-- 3. RESTORE POSTGRESQL cos_bcd_dkyc
UPDATE public.cos_bcd_dkyc SET gsmnumber = '9495950452', caf_serial_no = 'BDC0000090', circle_code = '50', de_csccode = 'KETVMCTOCSC', tariff_plan = 'FRC-1', parent_ctopup_number = '9497802714', mpin = '123456' WHERE id = 48;
UPDATE public.cos_bcd_dkyc SET gsmnumber = '9447470864', caf_serial_no = 'BDC0000091', circle_code = '50', de_csccode = 'KECLTKPTCSR', tariff_plan = 'FRC-1', parent_ctopup_number = '9495656279', mpin = '123456' WHERE id = 49;
UPDATE public.cos_bcd_dkyc SET gsmnumber = '9482102662', caf_serial_no = 'BDC0000092', circle_code = '53', de_csccode = 'KT01115', tariff_plan = 'FRC-249', parent_ctopup_number = '9448622220', mpin = '123456' WHERE id = 50;
UPDATE public.cos_bcd_dkyc SET gsmnumber = '8300289728', caf_serial_no = 'BDC0000093', circle_code = '54', de_csccode = 'TN10106', tariff_plan = 'FRC-1', parent_ctopup_number = '7598058579', mpin = '123456' WHERE id = 51;
UPDATE public.cos_bcd_dkyc SET gsmnumber = '8985729425', caf_serial_no = 'BDC0000094', circle_code = '41', de_csccode = 'AP0748', tariff_plan = 'FRC-249', parent_ctopup_number = '9491238422', mpin = '123456' WHERE id = 52;

-- 4. RESTORE POSTGRESQL frc_plan_table
DELETE FROM public.frc_plan_table WHERE plan_code = '1002000' AND circle_code = '2';
UPDATE public.frc_plan_table SET end_date = '2026-09-12' WHERE plan_code = '1002000' AND circle_code IN ('55', '56', '59', '60', '61', '62', '64', '65');
UPDATE public.frc_plan_table SET end_date = '2026-01-31' WHERE plan_code = '1002000' AND circle_code = '9999';
```

---

## 8. Acceptance Criteria Checklist (Section 19)

| # | Acceptance Criterion | Verification Status |
|---|:---|:---:|
| 1 | 10 local Oracle BCD identities exist | **PASSED** |
| 2 | All 10 are eligible for Q019 (`ACTIVATION_STATUS='C'`, `HLR_FINAL_ACT_DATE IS NOT NULL`, `FRC_FLOW_STATUS='NP'`, `FRC_REQID IS NULL`) | **PASSED** |
| 3 | All 10 belong to NZ (`2, 55, 56, 59, 60, 61, 62, 64, 65`) | **PASSED** |
| 4 | Oracle GSM/CAF/Circle match PostgreSQL exactly | **PASSED** |
| 5 | Q022 returns valid enrichment (5 EKYC + 5 DKYC) | **PASSED** |
| 6 | `frcamt = 1` across all 10 records | **PASSED** |
| 7 | CTOP joins succeed with unique dealer accounts | **PASSED** |
| 8 | MPIN prerequisites are valid (6 digits, encrypted) | **PASSED** |
| 9 | `frc_pyro_request_data` has 0 rows | **PASSED** |
| 10 | No production database was modified | **PASSED** |
| 11 | No Pyro transaction was executed | **PASSED** |

---

## 9. Final Decision

All local database preparations, identity alignments, cross-database verifications, query dry-runs, and snapshot backups have completed with 100% compliance:

```text
PHASE 18A TEST DATA PREPARATION — READY FOR PILOT
```

*(Note: In accordance with Section 20, the Phase 18 pilot execution has NOT been triggered automatically and awaits separate operator execution.)*
