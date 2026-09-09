# Phase 16: Static SQL Safety Review & Production Invariant Audit

## 1. Executive Summary & Objective

This document fulfills the requirements of **Phase 16 (Static SQL Safety Review)** of the `pyro_auto_frc` Zonewise Staged Migration Plan (`pyro_auto_frc_agent_implementation_plan.md`).

Prior to production deployment across staged zones (`NZ` → `NZ,WZ` → `ALL`), every SQL statement modified or introduced across the application must undergo a rigorous static safety review. This audit systematically inspects all database interactions across seven critical safety dimensions:

1. **Parameterization:** Zero raw string concatenation of runtime variables or user input; full native bind parameter usage.
2. **Dynamic Placeholders:** Deterministic, injection-proof construction of dynamic tokens (`:c_i`, `ANY(%s)`).
3. **Type Compatibility:** Exact match between application bind types and schema column types; elimination of performance-degrading column casts.
4. **Transaction Scope:** Clean transactional boundaries, explicit commit/rollback contracts, and dedicated checkout lifecycles.
5. **Row-Count Validation:** Exact single-row assertion on claims (`rc == 1`), zero-row failure handling, and multi-row data-integrity alarms.
6. **Zone Predicate:** Strict enforcement of zone circle subsets in `FILTERED` mode; clean removal of predicates in `ALL` mode; defensive early return on empty sets.
7. **State Predicate:** Comprehensive flow-state guards preventing out-of-order execution, race conditions, or unconfirmed dispatch.

---

## 2. Inventory of SQL Statements Reviewed

| Query Identifier | Module & Function | Database | Operation | Target Table(s) | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Q019** | `app.db.oracle.fetch_eligible_bcd_records` | Oracle | `SELECT` | `CAF_ADMIN.BCD` | **PASSED** |
| **Q020** | `app.db.oracle.batch_writeback_bcd_rq` | Oracle | `UPDATE` | `CAF_ADMIN.BCD` | **PASSED** |
| **ORA-UPD** | `app.db.oracle.update_bcd_status` | Oracle | `UPDATE` | `CAF_ADMIN.BCD` | **PASSED** |
| **ORA-REC** | `app.db.oracle.fetch_bcd_claim_statuses` | Oracle | `SELECT` | `CAF_ADMIN.BCD` | **FIXED & PASSED** |
| **PG-LOCK** | `app.db.postgres.population_advisory_lock` | PostgreSQL | `SELECT` | Advisory Lock Functions | **PASSED** |
| **Q022** | `app.db.postgres.fetch_cos_bcd_for_gsms` | PostgreSQL | `SELECT` (UNION) | `cos_bcd`, `cos_bcd_dkyc`, `ctop_master`, `frc_plan_table` | **PASSED** |
| **Q023** | `app.db.postgres.bulk_insert_frc_requests` | PostgreSQL | `INSERT ... RETURNING` | `public.frc_pyro_request_data` | **PASSED** |
| **PG-DISP** | `app.db.postgres.mark_requests_dispatchable` | PostgreSQL | `UPDATE` | `public.frc_pyro_request_data` | **PASSED** |
| **PG-FAIL** | `app.db.postgres.mark_requests_staging_failed` | PostgreSQL | `UPDATE` | `public.frc_pyro_request_data` | **PASSED** |
| **PG-STAGED** | `app.db.postgres.fetch_staged_unconfirmed_requests`| PostgreSQL | `SELECT` | `public.frc_pyro_request_data` | **PASSED** |
| **Q024** | `app.db.postgres.fetch_pending_rows` | PostgreSQL | `UPDATE ... RETURNING` | `public.frc_pyro_request_data` | **PASSED** |
| **PG-REL** | `app.db.postgres.release_unprocessed_claims` | PostgreSQL | `UPDATE` | `public.frc_pyro_request_data` | **PASSED** |
| **PG-PUSH** | `app.db.postgres.mark_as_pushed` | PostgreSQL | `UPDATE` | `public.frc_pyro_request_data` | **PASSED** |
| **PG-SUCC** | `app.db.postgres.mark_as_success` | PostgreSQL | `UPDATE` | `public.frc_pyro_request_data` | **PASSED** |
| **PG-ERR** | `app.db.postgres.mark_as_failed` | PostgreSQL | `UPDATE` | `public.frc_pyro_request_data` | **PASSED** |
| **PG-POLL** | `app.db.postgres.fetch_pushed_rows_for_status_check`| PostgreSQL | `SELECT` | `public.frc_pyro_request_data` | **PASSED** |
| **PG-ATT** | `app.db.postgres.update_status_check_attempt` | PostgreSQL | `UPDATE` | `public.frc_pyro_request_data` | **PASSED** |
| **PG-FIND** | `app.db.postgres.find_row_by_pyro_trans_id` | PostgreSQL | `SELECT` | `public.frc_pyro_request_data` | **PASSED** |
| **PG-TXN** | `app.db.postgres.insert_txn_log` | PostgreSQL | `INSERT` | `public.frc_txn_log` | **PASSED** |

---

## 3. Comprehensive Analysis by Safety Dimension

### 3.1 Parameterization & Injection Immunity

> **Rule:** No query should concatenate user-provided or external values directly into SQL strings.

- **Oracle Bind Architecture (`app/db/oracle.py`):**
  - Uses Oracle native named parameter binding (`:param_name`).
  - In `fetch_eligible_bcd_records` (Q019), `fetch_size` and `status_np` are bound as `:fetch_size` and `:status_np`. Dynamic circle codes are mapped to synthetic named binds `:c_0`, `:c_1`, etc. Zero string literals or user strings are embedded in the SQL text.
  - In `batch_writeback_bcd_rq` (Q020), every candidate update is fully bound with `:status`, `:reqid`, `:gsmnumber`, `:caf_serial_no`, `:circle_code`, and `:status_np`.
  - In `update_bcd_status`, all values (`:status`, `:remarks`, `:caf_serial_no`, `:reqid`) are parameterized; `remarks` is truncated to 2000 chars to avoid buffer/datatype violations.
  - In `fetch_bcd_claim_statuses`, candidate identity fields are passed via `:gsmnumber`, `:caf_serial_no`, and `:circle_code`.
- **PostgreSQL Bind Architecture (`app/db/postgres.py`):**
  - Uses `psycopg2` parameter binding (positional `%s` and pyformat `%(name)s`).
  - In `fetch_cos_bcd_for_gsms` (Q022), the GSM list and circle list are passed through `params = {"gsms": list(gsm_numbers), "allowed_circles": formatted_circles}` using `ANY(%(gsms)s)` and `ANY(%(allowed_circles)s)`.
  - In `bulk_insert_frc_requests` (Q023), rows are passed as structured dictionaries into `%(col)s` placeholders.
  - In `mark_requests_dispatchable` and `mark_requests_staging_failed`, list parameters use `ANY(%(reqids)s)`.
  - In `fetch_pending_rows` (Q024), batch limits and circle lists are passed via `params = ([int(c) for c in circle_codes], batch_size)`.
- **Static AST Audit Result:**
  - Automated Python Abstract Syntax Tree (AST) scanning confirmed zero occurrences of Python `%` formatting (`sql % vars`), `.format()` formatting, or direct f-string injection of unvalidated data into SQL strings.

---

### 3.2 Dynamic Placeholders Construction

> **Rule:** Dynamic query fragments must only contain static syntactic templates or purely index-generated placeholders.

- **Q019 (Oracle Discovery):**
  ```python
  unique_circles = sorted(set(int(c) for c in circle_codes))
  placeholders = [f":c_{i}" for i in range(len(unique_circles))]
  for i, c in enumerate(unique_circles):
      binds[f"c_{i}"] = c
  circle_predicate = f"AND CIRCLE_CODE IN ({', '.join(placeholders)})"
  ```
  - The placeholder tokens `:c_0, :c_1, ...` are constructed exclusively from `range(len(...))` integers.
  - The data values are cleanly separated and assigned to the `binds` dictionary.
  - An empty `circle_codes` argument triggers an early return of `[]` before query generation.
- **Q022 (PostgreSQL Enrichment):**
  ```python
  params["allowed_circles"] = formatted_circles
  circle_predicate = "AND cb.circle_code = ANY(%(allowed_circles)s)"
  ```
  - The dynamic fragment is a hardcoded literal template.
  - It utilizes PostgreSQL native array handling (`ANY(...)`), requiring only a single named bind placeholder regardless of the number of circles.
- **Q024 (PostgreSQL Dispatch Claim):**
  ```python
  if circle_codes is not None:
      circle_clause = "AND circle_code = ANY(%s)"
      params = ([int(c) for c in circle_codes], batch_size)
  else:
      circle_clause = ""
      params = (batch_size,)
  ```
  - The dynamic clause is either the empty string or the fixed literal `"AND circle_code = ANY(%s)"`. No dynamic token construction is involved.

---

### 3.3 Type Compatibility & Sargability

> **Rule:** Column data types must strictly match parameter types without unnecessary column-side casting.

- **PostgreSQL Circle Code Datatypes:**
  - `public.cos_bcd.circle_code`: Character type (`VARCHAR(10)`).
  - `public.cos_bcd_dkyc.circle_code`: Character type (`VARCHAR(20)`).
  - `public.frc_pyro_request_data.circle_code`: Numeric type (`SMALLINT`).
  - **Audit Evaluation:**
    - In Q022, `allowed_circles` is bound as a list of strings (`['2', '55', ...]`), matching `cos_bcd.circle_code` and `cos_bcd_dkyc.circle_code`. No column-side cast (`cb.circle_code::TEXT` or `::INT`) is applied, preserving B-Tree index sargability.
    - In Q024, `circle_codes` is converted via `int(c)` to a list of Python integers (`[2, 55, ...]`), matching `frc_pyro_request_data.circle_code` (`SMALLINT`).
- **Identity Types:**
  - `reqid`: Evaluated as `BIGSERIAL` / `BIGINT`. Converted using `int(r)` across all call sites (`mark_requests_dispatchable`, `release_unprocessed_claims`).
  - `GSMNUMBER` / `gsmno`: Handled as string (10 digits).
  - `caf_serial_no`: Handled as string (up to 30 chars).
- **String Truncation Guards:**
  - Column length constraints are strictly enforced in code:
    - `reason[:200]` in `mark_requests_staging_failed` (column `push_remarks VARCHAR(200)`).
    - `remarks[:2000]` in `update_bcd_status` (column `FRC_FLOW_REMARKS VARCHAR2(2000)`).
    - `remarks[:200]` and `remarks[:500]` in `mark_as_failed` (columns `push_remarks` and `last_error_msg`).

---

### 3.4 Transaction Scope & Connection Lifecycle

> **Rule:** Transactions must have explicit commit/rollback boundaries and safe connection checkout lifecycles.

- **Oracle Q020 Atomic Claim Transaction:**
  - Executed inside `with get_oracle_conn() as conn:`.
  - Loops over all validated candidate rows executing the single-row claim statement.
  - If any row updates `0` rows (already claimed/missing) or `>1` rows (alarm), or if the successful claim count does not match the batch size:
    - Explicit `conn.rollback()` is executed.
    - An exception (`OracleClaimMismatchError` or `OracleClaimDataIntegrityError`) is raised.
    - The transaction is never partially committed.
  - Explicit `conn.commit()` occurs only when 100% of candidate claims succeed.
- **PostgreSQL Advisory Lock (Phase 10):**
  - Session-level advisory lock `pg_try_advisory_lock(POPULATION_ADVISORY_LOCK_KEY)`.
  - Executed on a dedicated connection that remains checked out for the entire population lifecycle.
  - Unlocked in a guaranteed `finally:` block via `pg_advisory_unlock(...)` before returning the connection to the pool.
- **PostgreSQL Staging & Claim Transitions:**
  - `get_pg_conn()` context manager automatically commits (`conn.commit()`) on exit and issues `conn.rollback()` if an unhandled exception occurs.
  - Stale connection detection discards broken connections automatically.

---

### 3.5 Row-Count Validation & Integrity Invariants

> **Rule:** Claim operations must verify affected row counts and abort immediately on discrepancy.

- **Oracle Q020 Verification Matrix:**
  - `rc == 1`: Expected behavior. Candidate successfully claimed.
  - `rc == 0`: Claim failure. Indicates state conflict (concurrent claim, modified record, or missing row). Added to `failed_claims`.
  - `rc > 1`: Data integrity violation (composite key non-uniqueness). Added to `alarm_claims`.
  - Batch check: `len(successful_reqids) != len(caf_reqid_pairs)` forces immediate rollback.
- **PostgreSQL Q023 Staging Validation:**
  - Evaluates `cur.fetchone()` following `INSERT ... ON CONFLICT (batch_date, caf_serial_no) DO NOTHING RETURNING ...`.
  - Only rows that actually returned a `reqid` (newly inserted) are added to `inserted_pairs`. Conflicts are safely skipped without generating duplicate staging records.
- **PostgreSQL Q024 Atomic Worker Claim:**
  - Uses `UPDATE ... WHERE reqid IN (SELECT ... FOR UPDATE SKIP LOCKED LIMIT %s) RETURNING ...`.
  - The rows returned by `RETURNING` are the exact rows atomically owned by that worker thread. No race condition can assign the same row to concurrent workers.

---

### 3.6 Zone Predicate Verification

> **Rule:** When running in FILTERED mode, circle predicates must be applied across all pipeline stages; when running in ALL mode, circle predicates must be omitted.

| Pipeline Stage | Function | FILTERED Mode SQL Predicate | ALL Mode SQL Predicate |
| :--- | :--- | :--- | :--- |
| **Discovery** | `fetch_eligible_bcd_records` | `AND CIRCLE_CODE IN (:c_0, :c_1, ...)` | *(predicate omitted)* |
| **Enrichment** | `fetch_cos_bcd_for_gsms` | `AND cb.circle_code = ANY(%(allowed_circles)s)` | *(predicate omitted)* |
| **Staging** | `bulk_insert_frc_requests` | Inserts `circle_code` from verified candidate | Inserts `circle_code` from verified candidate |
| **Oracle Claim** | `batch_writeback_bcd_rq` | `AND CIRCLE_CODE = :circle_code` | `AND CIRCLE_CODE = :circle_code` |
| **Dispatch Claim** | `fetch_pending_rows` | `AND circle_code = ANY(%s)` | *(predicate omitted)* |

- **Empty Circle Guard:**
  - Both `fetch_eligible_bcd_records` and `fetch_cos_bcd_for_gsms` include defensive guards: if `circle_codes` is provided as an empty collection (`[]`), the functions immediately log and return `[]` without hitting the database.

---

### 3.7 State Predicate & Dispatch Safety Invariants

> **Rule:** An unconfirmed staged request must never become dispatchable.

- **State Invariants:**
  1. **Q023 Staging State:** Newly inserted rows in `frc_pyro_request_data` are hardcoded to `in_status = 'S'` (Staged), `pyro_status = 'N'`, `push_flag = 'N'`.
  2. **Q024 Dispatch Predicate:**
     ```sql
     WHERE in_status   = 'C'
       AND push_flag   IN ('N', 'E')
       AND retry_count <= max_retries
     ```
     Because Q024 strictly requires `in_status = 'C'`, staged rows (`'S'`) are structurally invisible to the recharge dispatch engine.
  3. **Claim Confirmation Gate:**
     Rows only transition from `'S'` to `'C'` in `mark_requests_dispatchable` after `batch_writeback_bcd_rq` has committed successfully in Oracle.
  4. **Oracle State Guard:**
     `batch_writeback_bcd_rq` enforces `WHERE FRC_FLOW_STATUS = 'NP' AND FRC_REQID IS NULL`, guaranteeing that already-processed or claimed records cannot be hijacked.
  5. **Dispatch Release Guard:**
     `release_unprocessed_claims` only resets claims where `push_flag = 'P' AND pyro_trans_id IS NULL AND push_date IS NULL`, guaranteeing that requests already in flight with Pyro are never released for duplicate execution.

---

## 4. Audit Findings & Resolution

### Finding 1: Non-Existent Table Name in Oracle Reconciliation Query (CRITICAL)

- **Location:** `app/db/oracle.py`, function `fetch_bcd_claim_statuses` (lines 335–346).
- **Issue:**
  The query was written as:
  ```sql
  SELECT
      GSMNUMBER,
      CAF_SERIAL_NO,
      CIRCLE_CODE,
      FRC_FLOW_STATUS,
      FRC_REQID
  FROM BCD_RECORD_INFO
  WHERE GSMNUMBER     = :gsmnumber
    AND CAF_SERIAL_NO = :caf_serial_no
    AND CIRCLE_CODE   = :circle_code
  ```
  `BCD_RECORD_INFO` does not exist in the Oracle production schema. All other Oracle BCD operations target `CAF_ADMIN.BCD`. In production, calling `fetch_bcd_claim_statuses` during Phase 8 reconciliation would fail with Oracle error `ORA-00942: table or view does not exist`.
- **Resolution:**
  Changed `FROM BCD_RECORD_INFO` to `FROM CAF_ADMIN.BCD`.
- **Verification:**
  Unit tests and recovery simulations verified that `fetch_bcd_claim_statuses` correctly queries `CAF_ADMIN.BCD` using the composite primary key `(GSMNUMBER, CAF_SERIAL_NO)` plus `CIRCLE_CODE`.

---

## 5. Automated Verification Test Suite

To ensure that SQL safety invariants remain permanently enforced and cannot regress, an automated test suite has been implemented at:

```text
tests/database/test_static_sql_safety_phase16.py
```

### Coverage:
1. **AST / Source Code Inspection:**
   - Verifies zero string-formatting injection patterns across `app/db/oracle.py` and `app/db/postgres.py`.
   - Asserts all dynamic SQL strings are composed strictly of validated templates and identifier placeholders.
2. **Oracle Q019 & Q020 Safety:**
   - Parameterization of dynamic placeholders `:c_i`.
   - Prevention of SQL injection via malicious circle inputs.
   - Exact rowcount verification (`rc == 1`, `rc == 0`, `rc > 1` alarm).
   - Batch-level transaction rollback on mismatch.
3. **PostgreSQL Q022 & Q024 Safety:**
   - Sizing and typing of `cb.circle_code = ANY(%(allowed_circles)s)` using string arrays.
   - Absence of column-side `::TEXT` casts.
   - Typing of `circle_code = ANY(%s)` in Q024 using integer arrays.
   - Concurrency isolation via `FOR UPDATE SKIP LOCKED`.
4. **State Machine Integrity:**
   - Staged requests (`in_status='S'`) remain invisible to Q024.
   - Only confirmed requests (`in_status='C'`) can be picked up for dispatch.

---

## 6. Sign-off & Handover Readiness

All SQL statements in `pyro_auto_frc` have been audited and verified against production database metadata. With Finding 1 resolved, the codebase satisfies all static SQL safety requirements of Phase 16 and is ready for Phase 17 (ALL Regression) and Phase 18 (NZ Pilot).
