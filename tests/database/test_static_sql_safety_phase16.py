"""Phase 16 — Static SQL Safety Review & Production Invariant Audit Test Suite.

Automated verification of the 7 mandatory SQL safety dimensions:
1. Parameterization: No raw string concatenation of runtime variables into SQL strings.
2. Dynamic Placeholders: Deterministic, injection-proof construction of placeholders.
3. Type Compatibility: Correct typing for circle codes, reqids, advisory lock keys, and remarks truncation.
4. Transaction Scope: Strict transaction boundaries, rollback on failure, advisory lock connection scoping.
5. Row-Count Validation: Single-row assertions (rc==1), zero-row failure handling, multi-row alarms.
6. Zone Predicate: Complete circle filtering in FILTERED mode, clean removal in ALL mode, defensive empty list guards.
7. State Predicate: Unclaimed records cannot become dispatchable; staged requests remain invisible to Q024 pickup.
"""

import ast
import inspect
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

import app.db.oracle as oracle_module
import app.db.postgres as postgres_module
from app.db.oracle import (
    BCD_STATUS_NP,
    BCD_STATUS_RQ,
    OracleClaimDataIntegrityError,
    OracleClaimMismatchError,
    batch_writeback_bcd_rq,
    fetch_bcd_claim_statuses,
    fetch_eligible_bcd_records,
    update_bcd_status,
)
from app.db.postgres import (
    FLAG_FAILED,
    FLAG_PENDING,
    FLAG_PUSHED,
    FLAG_RETRY,
    FLAG_SUCCESS,
    IN_STATUS_CONFIRMED,
    IN_STATUS_FAILED,
    IN_STATUS_STAGED,
    POPULATION_ADVISORY_LOCK_KEY,
    bulk_insert_frc_requests,
    fetch_cos_bcd_for_gsms,
    fetch_pending_rows,
    fetch_staged_unconfirmed_requests,
    mark_as_failed,
    mark_as_pushed,
    mark_as_success,
    mark_requests_dispatchable,
    mark_requests_staging_failed,
    population_advisory_lock,
    release_unprocessed_claims,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

@contextmanager
def _mock_oracle_conn(cursor, conn=None):
    if conn is None:
        conn = MagicMock()
    conn.cursor.return_value = cursor
    yield conn


@contextmanager
def _mock_pg_conn(cursor, conn=None):
    if conn is None:
        conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = None
    yield conn


# ── Dimension 1 & 2: AST Static Analysis (Parameterization & Placeholders) ─────

class TestASTSQLSafetyAudit:
    """Static AST inspection across all database access modules."""

    @pytest.mark.parametrize("module_path", [
        Path("app/db/oracle.py"),
        Path("app/db/postgres.py"),
    ])
    def test_ast_no_sql_string_formatting_or_percent_interpolation(self, module_path):
        """Verify no cursor.execute calls use '%' or '.format()' interpolation on SQL text."""
        source_code = module_path.read_text(encoding="utf-8")
        tree = ast.parse(source_code)

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                # Look for cur.execute(...) or cursor.execute(...)
                if isinstance(func, ast.Attribute) and func.attr in ("execute", "executemany"):
                    assert len(node.args) >= 1, f"Empty execute call in {module_path}"
                    sql_arg = node.args[0]

                    # 1. Reject % formatting: cur.execute("SELECT ... %s" % var)
                    if isinstance(sql_arg, ast.BinOp) and isinstance(sql_arg.op, ast.Mod):
                        pytest.fail(
                            f"Unsafe SQL % string formatting detected in {module_path} at line {node.lineno}"
                        )

                    # 2. Reject .format() on SQL text: cur.execute("SELECT ... {}".format(var))
                    if isinstance(sql_arg, ast.Call):
                        if isinstance(sql_arg.func, ast.Attribute) and sql_arg.func.attr == "format":
                            pytest.fail(
                                f"Unsafe SQL .format() detected in {module_path} at line {node.lineno}"
                            )

    @pytest.mark.parametrize("module_path", [
        Path("app/db/oracle.py"),
        Path("app/db/postgres.py"),
    ])
    def test_ast_no_direct_string_concatenation_with_variables(self, module_path):
        """Verify SQL variables are not constructed by concatenating raw runtime variables with '+'."""
        source_code = module_path.read_text(encoding="utf-8")
        tree = ast.parse(source_code)

        for node in ast.walk(tree):
            # Check variable assignments: sql = "..." + variable
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and "sql" in target.id.lower():
                        if isinstance(node.value, ast.BinOp) and isinstance(node.value.op, ast.Add):
                            # Allow literal + literal (string constant splitting), reject variable additions
                            if not (isinstance(node.value.left, ast.Constant) and isinstance(node.value.right, ast.Constant)):
                                pytest.fail(
                                    f"Potential dynamic string concatenation in SQL variable '{target.id}' "
                                    f"at line {node.lineno} in {module_path}"
                                )


# ── Dimension 3: Table & Column Type Compatibility ─────────────────────────────

class TestTypeCompatibilityAndSchemaInvariants:
    """Verify type casting, data lengths, and schema table correctness."""

    def test_oracle_fetch_bcd_claim_statuses_uses_canonical_table(self):
        """Verify fetch_bcd_claim_statuses queries CAF_ADMIN.BCD (Audit Finding 1)."""
        source = inspect.getsource(fetch_bcd_claim_statuses)
        assert "FROM CAF_ADMIN.BCD" in source
        assert "FROM BCD_RECORD_INFO" not in source

    def test_oracle_q019_and_q020_use_canonical_table(self):
        """Verify Q019 and Q020 target CAF_ADMIN.BCD."""
        q019_src = inspect.getsource(fetch_eligible_bcd_records)
        q020_src = inspect.getsource(batch_writeback_bcd_rq)
        assert "FROM CAF_ADMIN.BCD" in q019_src
        assert "UPDATE CAF_ADMIN.BCD" in q020_src

    def test_postgres_q022_no_column_side_text_casts(self):
        """Verify Q022 does not use cb.circle_code::TEXT which invalidates index sargability."""
        source = inspect.getsource(fetch_cos_bcd_for_gsms)
        assert "cb.circle_code::TEXT" not in source
        assert "cb.circle_code::INT" not in source
        assert "cb.circle_code = ANY(%(allowed_circles)s)" in source

    def test_advisory_lock_key_within_signed_bigint_range(self):
        """Verify PostgreSQL advisory lock key fits in signed 64-bit bigint."""
        min_bigint = -(2 ** 63)
        max_bigint = 2 ** 63 - 1
        assert isinstance(POPULATION_ADVISORY_LOCK_KEY, int)
        assert min_bigint <= POPULATION_ADVISORY_LOCK_KEY <= max_bigint

    @patch("app.db.postgres.get_pg_conn")
    def test_mark_requests_staging_failed_truncates_reason(self, mock_get_conn):
        """Verify reason string is defensively sliced to <= 200 chars for VARCHAR(200)."""
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        long_reason = "E" * 500
        mark_requests_staging_failed([101], reason=long_reason)

        mock_cursor.execute.assert_called_once()
        _, params = mock_cursor.execute.call_args[0]
        assert len(params["reason"]) <= 200
        assert params["reason"] == "E" * 200

    @patch("app.db.oracle.get_oracle_conn")
    def test_update_bcd_status_truncates_remarks(self, mock_get_conn):
        """Verify Oracle remarks are defensively sliced to <= 2000 chars for VARCHAR2(2000)."""
        mock_cursor = MagicMock()
        mock_get_conn.side_effect = lambda: _mock_oracle_conn(mock_cursor)

        long_remarks = "R" * 5000
        update_bcd_status("CAF01", 101, "F", long_remarks)

        mock_cursor.execute.assert_called_once()
        _, params = mock_cursor.execute.call_args[0]
        assert len(params["remarks"]) <= 2000
        assert params["remarks"] == "R" * 2000


# ── Dimension 4: Transaction Scope & Connection Lifecycle ──────────────────────

class TestTransactionScope:
    """Verify explicit commit/rollback contracts and connection scoping."""

    @patch("app.db.oracle.get_oracle_conn")
    def test_oracle_q020_commits_only_on_full_success(self, mock_get_conn):
        """Q020 issues commit only when all candidate claims update exactly 1 row."""
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        candidates = [
            {"reqid": 1, "caf_serial_no": "CAF1", "gsmnumber": "9412345671", "circle_code": 2},
            {"reqid": 2, "caf_serial_no": "CAF2", "gsmnumber": "9412345672", "circle_code": 2},
        ]
        claimed = batch_writeback_bcd_rq(candidates)

        assert claimed == 2
        mock_conn.commit.assert_called_once()
        mock_conn.rollback.assert_not_called()

    @patch("app.db.oracle.get_oracle_conn")
    def test_oracle_q020_rolls_back_on_claim_failure(self, mock_get_conn):
        """Q020 immediately rolls back if any row update count is 0."""
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0  # Simulation: record already claimed or modified
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        candidates = [
            {"reqid": 1, "caf_serial_no": "CAF1", "gsmnumber": "9412345671", "circle_code": 2},
        ]
        with pytest.raises(OracleClaimMismatchError):
            batch_writeback_bcd_rq(candidates)

        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()

    @patch("app.db.postgres.get_pg_conn")
    def test_population_advisory_lock_unlocks_in_finally_block(self, mock_get_conn):
        """Advisory lock must execute unlock even when an exception occurs inside the block."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (True,)  # Lock acquired
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        with patch("app.db.postgres._pool", MagicMock()):
            with pytest.raises(RuntimeError, match="Processing aborted"):
                with population_advisory_lock() as acquired:
                    assert acquired is True
                    raise RuntimeError("Processing aborted")

        # Verify pg_advisory_unlock was invoked
        calls = [c[0][0] for c in mock_cursor.execute.call_args_list]
        assert any("pg_advisory_unlock" in sql for sql in calls)


# ── Dimension 5: Row-Count Validation ──────────────────────────────────────────

class TestRowCountValidation:
    """Verify strict rowcount validation and data-integrity alarm handling."""

    @patch("app.db.oracle.get_oracle_conn")
    def test_oracle_q020_raises_data_integrity_alarm_on_multirow_update(self, mock_get_conn):
        """Q020 raises OracleClaimDataIntegrityError and rolls back if rc > 1."""
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 2  # Critical integrity failure: >1 rows updated for single key
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        candidates = [
            {"reqid": 1, "caf_serial_no": "CAF1", "gsmnumber": "9412345671", "circle_code": 2},
        ]
        with pytest.raises(OracleClaimDataIntegrityError) as exc_info:
            batch_writeback_bcd_rq(candidates)

        assert "alarms (>1 rows)" in str(exc_info.value)
        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()

    @patch("app.db.postgres.get_pg_conn")
    def test_bulk_insert_handles_conflicts_via_on_conflict_returning(self, mock_get_conn):
        """Q023 inserts ignore duplicate conflicts and returns only actually inserted pairs."""
        mock_cursor = MagicMock()
        # Row 1 inserts successfully, Row 2 is a conflict (returns None)
        mock_cursor.fetchone.side_effect = [
            (101, "CAF01", "9412345671", 2),
            None,
        ]
        mock_conn = _mock_pg_conn(mock_cursor)
        mock_get_conn.side_effect = lambda: mock_conn

        rows = [
            {"caf_serial_no": "CAF01", "gsmno": "9412345671", "circle_code": 2, "csccode": "C1",
             "edate": "2026-09-01", "reqdate": "2026-09-01", "frc_plan_name": "P1", "frc_plan_code": "PC1",
             "frc_category_code": "CC1", "frcamt": 299, "ctopup_number": "9412300000", "vendormsisdn": "9412300000",
             "vendorid": "V1", "mpin": "M1", "mpin_length": 4, "max_retries": 3, "kyc_mode": "EKYC"},
            {"caf_serial_no": "CAF02", "gsmno": "9412345672", "circle_code": 2, "csccode": "C1",
             "edate": "2026-09-01", "reqdate": "2026-09-01", "frc_plan_name": "P1", "frc_plan_code": "PC1",
             "frc_category_code": "CC1", "frcamt": 299, "ctopup_number": "9412300000", "vendormsisdn": "9412300000",
             "vendorid": "V1", "mpin": "M1", "mpin_length": 4, "max_retries": 3, "kyc_mode": "EKYC"},
        ]
        inserted = bulk_insert_frc_requests(rows)

        assert len(inserted) == 1
        assert inserted[0]["reqid"] == 101
        assert inserted[0]["caf_serial_no"] == "CAF01"


# ── Dimension 6: Zone Predicates & Defensive Guards ────────────────────────────

class TestZonePredicatesAndDefensiveGuards:
    """Verify zone circle filters and defensive empty collection handling."""

    def test_q019_empty_circles_returns_empty_immediately(self):
        """Q019 with circle_codes=[] returns [] immediately without DB call."""
        with patch("app.db.oracle.get_oracle_conn") as mock_conn:
            result = fetch_eligible_bcd_records(circle_codes=[])
            assert result == []
            mock_conn.assert_not_called()

    def test_q022_empty_circles_returns_empty_immediately(self):
        """Q022 with circle_codes=[] returns [] immediately without DB call."""
        with patch("app.db.postgres.get_pg_conn") as mock_conn:
            result = fetch_cos_bcd_for_gsms(["9412345678"], circle_codes=[])
            assert result == []
            mock_conn.assert_not_called()

    @patch("app.db.oracle.get_oracle_conn")
    def test_q019_filtered_vs_all_mode_predicates(self, mock_get_conn):
        """Verify Q019 includes CIRCLE_CODE IN (:c_0, ...) for FILTERED and omits for ALL."""
        mock_cursor = MagicMock()
        mock_cursor.description = []
        mock_cursor.fetchall.return_value = []
        mock_get_conn.side_effect = lambda: _mock_oracle_conn(mock_cursor)

        # 1. FILTERED mode
        fetch_eligible_bcd_records(circle_codes=[2, 55])
        sql_filtered, binds_filtered = mock_cursor.execute.call_args[0]
        assert "AND CIRCLE_CODE IN (:c_0, :c_1)" in sql_filtered
        assert binds_filtered["c_0"] == 2
        assert binds_filtered["c_1"] == 55

        mock_cursor.reset_mock()

        # 2. ALL mode
        fetch_eligible_bcd_records(circle_codes=None)
        sql_all, binds_all = mock_cursor.execute.call_args[0]
        assert "CIRCLE_CODE IN" not in sql_all
        assert "c_0" not in binds_all

    @patch("app.db.postgres.get_pg_conn")
    def test_q024_filtered_vs_all_mode_predicates(self, mock_get_conn):
        """Verify Q024 includes circle_code = ANY(%s) for FILTERED and omits for ALL."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        # 1. FILTERED mode
        fetch_pending_rows(batch_size=50, circle_codes=[2, 55])
        sql_filtered, params_filtered = mock_cursor.execute.call_args[0]
        assert "AND circle_code = ANY(%s)" in sql_filtered
        assert params_filtered == ([2, 55], 50)

        mock_cursor.reset_mock()

        # 2. ALL mode
        fetch_pending_rows(batch_size=50, circle_codes=None)
        sql_all, params_all = mock_cursor.execute.call_args[0]
        assert "circle_code = ANY(%s)" not in sql_all
        assert params_all == (50,)


# ── Dimension 7: State Machine Invariants & Dispatch Safety ────────────────────

class TestStateMachineInvariantsAndDispatchSafety:
    """Verify dispatch isolation and state transition predicates."""

    def test_q023_initial_state_is_strictly_non_dispatchable(self):
        """Verify Q023 stages rows with in_status='S' (non-dispatchable) and push_flag='N'."""
        source = inspect.getsource(bulk_insert_frc_requests)
        # Verify values hardcoded in the INSERT VALUES clause
        assert "'S', 'N', 'N'" in source

    def test_q024_dispatch_requires_confirmed_in_status_c(self):
        """Verify Q024 strictly requires in_status='C', preventing pickup of staged 'S' rows."""
        source = inspect.getsource(fetch_pending_rows)
        assert "WHERE in_status   = 'C'" in source
        assert "push_flag   IN ('N', 'E')" in source
        assert "FOR UPDATE SKIP LOCKED" in source

    @patch("app.db.postgres.get_pg_conn")
    def test_mark_requests_dispatchable_targets_only_staged_status_s(self, mock_get_conn):
        """mark_requests_dispatchable only transitions rows that are in in_status='S'."""
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        mark_requests_dispatchable([101, 102])
        sql, params = mock_cursor.execute.call_args[0]

        assert "SET\n            in_status  = 'C'" in sql or "in_status  = 'C'" in sql
        assert "AND in_status = 'S'" in sql
        assert set(params["reqids"]) == {101, 102}

    @patch("app.db.postgres.get_pg_conn")
    def test_release_unprocessed_claims_safety_predicate(self, mock_get_conn):
        """release_unprocessed_claims requires push_flag='P' and pyro_trans_id IS NULL."""
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        release_unprocessed_claims([101])
        sql, params = mock_cursor.execute.call_args[0]

        assert "AND push_flag = 'P'" in sql
        assert "AND pyro_trans_id IS NULL" in sql
        assert "AND push_date IS NULL" in sql
        assert params == ([101],)
