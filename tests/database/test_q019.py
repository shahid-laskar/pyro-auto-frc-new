"""Unit tests for Q019 Oracle BCD candidate discovery (app/db/oracle.py:fetch_eligible_bcd_records)."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from app.context import ExecutionContext
from app.db.oracle import BCD_STATUS_NP, fetch_eligible_bcd_records
from app.zones import NZ, WZ


def _make_mock_cursor(sample_rows=None):
    """Helper to construct a mock cursor with column description and rows."""
    cursor = MagicMock()
    cursor.description = [
        ("GSMNUMBER",),
        ("CAF_SERIAL_NO",),
        ("DE_CSCCODE",),
        ("CIRCLE_CODE",),
        ("HLR_FINAL_ACT_DATE",),
    ]
    if sample_rows is not None:
        cursor.fetchall.return_value = sample_rows
    else:
        cursor.fetchall.return_value = [
            ("9412345678", "CAF001", "CSC01", 2, "2026-09-01 10:00:00"),
            ("9412345679", "CAF002", "CSC02", 55, "2026-09-01 11:00:00"),
        ]
    return cursor


@contextmanager
def _mock_oracle_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    yield conn


class TestQ019OracleDiscovery:
    """Test suite for Q019 query construction, parameterization, and result mapping."""

    @patch("app.db.oracle.get_oracle_conn")
    def test_q019_all_mode_no_circle_filter(self, mock_get_conn):
        """When circle_codes=None (ALL mode), CIRCLE_CODE predicate must be completely omitted."""
        mock_cursor = _make_mock_cursor()
        mock_get_conn.side_effect = lambda: _mock_oracle_conn(mock_cursor)

        rows = fetch_eligible_bcd_records(fetch_size=500, circle_codes=None)

        assert len(rows) == 2
        assert rows[0]["GSMNUMBER"] == "9412345678"
        assert rows[0]["CAF_SERIAL_NO"] == "CAF001"
        assert rows[0]["CIRCLE_CODE"] == 2

        mock_cursor.execute.assert_called_once()
        executed_sql, executed_binds = mock_cursor.execute.call_args[0]

        # Verify SQL structure
        assert "ACTIVATION_STATUS  = 'C'" in executed_sql
        assert "HLR_FINAL_ACT_DATE IS NOT NULL" in executed_sql
        assert "FRC_FLOW_STATUS    = :status_np" in executed_sql
        assert "FRC_REQID          IS NULL" in executed_sql
        assert "ORDER BY HLR_FINAL_ACT_DATE ASC" in executed_sql
        assert "ROWNUM <= :fetch_size" in executed_sql
        assert "CIRCLE_CODE IN" not in executed_sql

        # Verify binds
        assert executed_binds == {
            "status_np": BCD_STATUS_NP,
            "fetch_size": 500,
        }

    @patch("app.db.oracle.get_oracle_conn")
    def test_q019_filtered_mode_single_circle(self, mock_get_conn):
        """When a single circle code is supplied, dynamic :c_0 placeholder is bound."""
        mock_cursor = _make_mock_cursor()
        mock_get_conn.side_effect = lambda: _mock_oracle_conn(mock_cursor)

        fetch_eligible_bcd_records(fetch_size=100, circle_codes=[2])

        mock_cursor.execute.assert_called_once()
        executed_sql, executed_binds = mock_cursor.execute.call_args[0]

        assert "AND CIRCLE_CODE IN (:c_0)" in executed_sql
        assert executed_binds == {
            "status_np": BCD_STATUS_NP,
            "fetch_size": 100,
            "c_0": 2,
        }

    @patch("app.db.oracle.get_oracle_conn")
    def test_q019_filtered_mode_nz_circles(self, mock_get_conn):
        """When NZ circles (9 circles) are supplied, :c_0 through :c_8 placeholders are generated."""
        mock_cursor = _make_mock_cursor()
        mock_get_conn.side_effect = lambda: _mock_oracle_conn(mock_cursor)

        fetch_eligible_bcd_records(fetch_size=500, circle_codes=NZ)

        mock_cursor.execute.assert_called_once()
        executed_sql, executed_binds = mock_cursor.execute.call_args[0]

        # Verify 9 placeholders
        expected_placeholders = ", ".join(f":c_{i}" for i in range(9))
        assert f"AND CIRCLE_CODE IN ({expected_placeholders})" in executed_sql

        # Verify binds
        assert executed_binds["status_np"] == BCD_STATUS_NP
        assert executed_binds["fetch_size"] == 500
        for i, c in enumerate(sorted(NZ)):
            assert executed_binds[f"c_{i}"] == c

    @patch("app.db.oracle.get_oracle_conn")
    def test_q019_filtered_mode_tuple_input(self, mock_get_conn):
        """Accepts tuple circle_codes (as provided by ExecutionContext.circle_codes)."""
        mock_cursor = _make_mock_cursor()
        mock_get_conn.side_effect = lambda: _mock_oracle_conn(mock_cursor)

        circle_tuple = (1, 3, 4, 10, 12)
        fetch_eligible_bcd_records(fetch_size=250, circle_codes=circle_tuple)

        mock_cursor.execute.assert_called_once()
        executed_sql, executed_binds = mock_cursor.execute.call_args[0]

        assert "AND CIRCLE_CODE IN (:c_0, :c_1, :c_2, :c_3, :c_4)" in executed_sql
        for i, c in enumerate(circle_tuple):
            assert executed_binds[f"c_{i}"] == c

    @patch("app.db.oracle.get_oracle_conn")
    def test_q019_deduplication_and_sorting(self, mock_get_conn):
        """Duplicates in circle_codes are deduplicated and sorted before binding."""
        mock_cursor = _make_mock_cursor()
        mock_get_conn.side_effect = lambda: _mock_oracle_conn(mock_cursor)

        fetch_eligible_bcd_records(fetch_size=500, circle_codes=[55, 2, 55, 2])

        mock_cursor.execute.assert_called_once()
        executed_sql, executed_binds = mock_cursor.execute.call_args[0]

        assert "AND CIRCLE_CODE IN (:c_0, :c_1)" in executed_sql
        assert executed_binds["c_0"] == 2
        assert executed_binds["c_1"] == 55

    @patch("app.db.oracle.get_oracle_conn")
    def test_q019_empty_circle_codes_returns_empty_list_immediately(self, mock_get_conn):
        """If an empty sequence is provided, returns [] without hitting the database."""
        mock_cursor = _make_mock_cursor()
        mock_get_conn.side_effect = lambda: _mock_oracle_conn(mock_cursor)

        rows = fetch_eligible_bcd_records(fetch_size=500, circle_codes=[])

        assert rows == []
        mock_cursor.execute.assert_not_called()
        mock_get_conn.assert_not_called()

    @patch("app.db.oracle.get_oracle_conn")
    def test_q019_context_integration_scheduled(self, mock_get_conn):
        """Verify seamless execution using ExecutionContext for scheduled runs."""
        mock_cursor = _make_mock_cursor()
        mock_get_conn.side_effect = lambda: _mock_oracle_conn(mock_cursor)

        ctx = ExecutionContext.for_manual(zones_str="NZ")
        rows = fetch_eligible_bcd_records(
            fetch_size=500,
            circle_codes=ctx.circle_codes,
        )
        assert len(rows) == 2
        mock_cursor.execute.assert_called_once()
        executed_sql, executed_binds = mock_cursor.execute.call_args[0]
        assert "AND CIRCLE_CODE IN" in executed_sql
        assert len([k for k in executed_binds if k.startswith("c_")]) == 9

    @patch("app.db.oracle.get_oracle_conn")
    def test_q019_context_integration_all(self, mock_get_conn):
        """Verify execution using ExecutionContext in ALL mode."""
        mock_cursor = _make_mock_cursor()
        mock_get_conn.side_effect = lambda: _mock_oracle_conn(mock_cursor)

        ctx = ExecutionContext.for_manual(zones_str="ALL")
        fetch_eligible_bcd_records(
            fetch_size=500,
            circle_codes=ctx.circle_codes,
        )
        mock_cursor.execute.assert_called_once()
        executed_sql, executed_binds = mock_cursor.execute.call_args[0]
        assert "CIRCLE_CODE IN" not in executed_sql
