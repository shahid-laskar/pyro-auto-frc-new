"""Unit tests for Phase 14: Database Index Review & Query Plan Validation.

Verifies:
1. Q022 uses native string comparison on circle_code without column-side casting.
2. Q024 query aligns with the partial composite index design (circle_code, created_at) WHERE push_flag IN ('N', 'E').
3. DDL in sql/postgres_tables.sql includes idx_frc_pyro_pickup_zonewise with IF NOT EXISTS.
4. Oracle Q020 query utilizes exact composite primary key without requiring secondary indexes.
"""

import os
from unittest.mock import MagicMock, patch
import pytest

from app.db.postgres import fetch_cos_bcd_for_gsms, fetch_pending_rows
from app.db.oracle import batch_writeback_bcd_rq


class TestQ022IndexCompliance:
    @patch("app.db.postgres.get_pg_read_conn")
    def test_q022_uses_native_string_comparison_without_casting(self, mock_get_pg_conn):
        """Q022 must use native string comparison (cb.circle_code = ANY) without column casts."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_get_pg_conn.return_value.__enter__.return_value = mock_conn
        mock_cur.fetchall.return_value = []

        fetch_cos_bcd_for_gsms(
            gsm_numbers=["9412300001"],
            circle_codes=[2, 55],
        )

        assert mock_cur.execute.called
        query_sql = mock_cur.execute.call_args[0][0]
        params = mock_cur.execute.call_args[0][1]

        # 1. Native comparison predicate
        assert "cb.circle_code = ANY(%(allowed_circles)s)" in query_sql
        # 2. No column-side casting that invalidates index scans
        assert "cb.circle_code::TEXT" not in query_sql
        assert "cb.circle_code::int" not in query_sql
        assert "cb.circle_code::INTEGER" not in query_sql
        # 3. Formatted parameters are strings
        assert "allowed_circles" in params
        assert params["allowed_circles"] == ["2", "55"]


class TestQ024IndexCompliance:
    @patch("app.db.postgres.get_pg_conn")
    def test_q024_query_structure_aligns_with_partial_index(self, mock_get_pg_conn):
        """Q024 query structure must match partial index predicates and order."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_get_pg_conn.return_value.__enter__.return_value = mock_conn
        mock_cur.fetchall.return_value = []

        fetch_pending_rows(batch_size=100, circle_codes=[2, 55])

        assert mock_cur.execute.called
        query_sql = mock_cur.execute.call_args[0][0]

        # 1. Partial filter conditions
        assert "in_status   = 'C'" in query_sql
        assert "push_flag   IN ('N', 'E')" in query_sql
        assert "circle_code = ANY(%s)" in query_sql
        # 2. Ordering matches index order
        assert "ORDER BY created_at ASC" in query_sql
        # 3. Concurrency control
        assert "FOR UPDATE SKIP LOCKED" in query_sql


class TestPostgresDDLDefinitions:
    def test_idx_frc_pyro_pickup_zonewise_present_in_ddl(self):
        """sql/postgres_tables.sql must define idx_frc_pyro_pickup_zonewise with IF NOT EXISTS."""
        ddl_path = os.path.join(os.path.dirname(__file__), "..", "..", "sql", "postgres_tables.sql")
        ddl_path = os.path.abspath(ddl_path)
        assert os.path.exists(ddl_path), f"DDL file not found at {ddl_path}"

        with open(ddl_path, "r", encoding="utf-8") as f:
            ddl_content = f.read()

        assert "CREATE INDEX IF NOT EXISTS idx_frc_pyro_pickup_zonewise" in ddl_content
        assert "(circle_code, created_at)" in ddl_content
        assert "WHERE push_flag IN ('N', 'E')" in ddl_content


class TestOracleQ020IndexCompliance:
    @patch("app.db.oracle.get_oracle_conn")
    def test_q020_uses_composite_pk_predicate(self, mock_get_oracle_conn):
        """Oracle Q020 claim writeback must use exact composite PK (GSMNUMBER, CAF_SERIAL_NO)."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_get_oracle_conn.return_value.__enter__.return_value = mock_conn
        mock_cur.rowcount = 1

        batch_writeback_bcd_rq([
            {"reqid": 501, "caf_serial_no": "CAF001", "gsmno": "9412300001", "circle_code": 2}
        ])

        assert mock_cur.execute.called
        query_sql = mock_cur.execute.call_args[0][0]

        # Verified PK predicates
        assert "GSMNUMBER" in query_sql and ":gsmnumber" in query_sql
        assert "CAF_SERIAL_NO" in query_sql and ":caf_serial_no" in query_sql
        assert "CIRCLE_CODE" in query_sql and ":circle_code" in query_sql
        assert "FRC_FLOW_STATUS" in query_sql
        assert "FRC_REQID IS NULL" in query_sql
