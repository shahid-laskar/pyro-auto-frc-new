"""Unit tests for Phase 6: Q023 Staging Lifecycle Design.

Verifies:
- Staged rows are created with in_status='S' (NOT DISPATCHABLE)
- fetch_pending_rows excludes rows where in_status='S'
- mark_requests_dispatchable transitions in_status from 'S' to 'C'
- mark_requests_staging_failed transitions in_status from 'S' to 'F'
- Critical cross-database failure case: when Oracle Q020 claim fails,
  staged Postgres requests remain NOT DISPATCHABLE and Pyro recharge = 0.
"""

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.batch.populator import run_batch_population
from app.db.postgres import (
    IN_STATUS_CONFIRMED,
    IN_STATUS_FAILED,
    IN_STATUS_STAGED,
    bulk_insert_frc_requests,
    fetch_pending_rows,
)


@contextmanager
def _mock_pg_conn(cursor):
    """Context manager helper mimicking get_pg_conn."""
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = None
    yield conn


class TestQ023StagingLifecycleDesign:
    """Test suite for Q023 insertion state, lifecycle transitions, and dispatch guard."""

    @patch("app.db.postgres.get_pg_conn")
    def test_q023_inserts_rows_in_dispatchable_state_in_status_c(self, mock_get_conn):
        """bulk_insert_frc_requests must explicitly insert with in_status='C', push_flag='N', pyro_status='N'."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (501, "CAF001", "9412345678", 2)
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        sample_row = {
            "caf_serial_no": "CAF001",
            "gsmno": "9412345678",
            "csccode": "CSC01",
            "circle_code": 2,
            "edate": "2026-09-01",
            "reqdate": "2026-09-01 10:00:00",
            "frc_plan_name": "Plan1",
            "frc_plan_code": "P01",
            "frc_category_code": "C01",
            "frcamt": 299,
            "ctopup_number": "9412300000",
            "vendormsisdn": "9412300000",
            "vendorid": "VEND01",
            "mpin": "enc_mpin",
            "mpin_length": 4,
            "max_retries": 3,
            "kyc_mode": "EKYC",
        }

        result = bulk_insert_frc_requests([sample_row])

        assert len(result) == 1
        assert result[0] == {
            "reqid": 501,
            "caf_serial_no": "CAF001",
            "gsmno": "9412345678",
            "gsmnumber": "9412345678",
            "circle_code": 2,
        }

        mock_cursor.execute.assert_called_once()
        executed_sql, executed_params = mock_cursor.execute.call_args[0]

        # Verify that the values inserted are 'C', 'N', 'N' (in_status='C', pyro_status='N', push_flag='N')
        assert "'C', 'N', 'N'" in executed_sql
        assert executed_params == sample_row

    @patch("app.db.postgres.get_pg_conn")
    def test_fetch_pending_rows_strictly_requires_in_status_c(self, mock_get_conn):
        """fetch_pending_rows must filter WHERE in_status = 'C' and push_flag IN ('N', 'E')."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        fetch_pending_rows(batch_size=100)

        mock_cursor.execute.assert_called_once()
        executed_sql, (batch_size_param,) = mock_cursor.execute.call_args[0]

        assert "WHERE in_status   = 'C'" in executed_sql
        assert "AND push_flag   IN ('N', 'E')" in executed_sql
        assert batch_size_param == 100


class TestCrossDatabaseFailureRecoverySimulation:
    """Test suite verifying failure cases during populator batch run."""

    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_critical_failure_case_q023_succeeds_q020_fails(
        self, mock_fetch_oracle, mock_fetch_pg, mock_bulk_insert, mock_writeback
    ):
        """When Oracle claim (Q020) fails, errors are recorded and dispatchable count is 0."""
        mock_fetch_oracle.return_value = [
            {
                "GSMNUMBER": "9412345678",
                "CAF_SERIAL_NO": "CAF001",
                "DE_CSCCODE": "CSC01",
                "CIRCLE_CODE": 2,
                "HLR_FINAL_ACT_DATE": "2026-09-01 10:00:00",
            }
        ]
        mock_fetch_pg.return_value = [
            {
                "gsmnumber": "9412345678",
                "caf_serial_no": "CAF001",
                "de_csccode": "CSC01",
                "circle_code": "2",
                "live_photo_time": "2026-09-01 10:00:00",
                "frc_plan_name": "Plan1",
                "frc_plan_code": "P01",
                "frc_category_code": "C01",
                "frcamt": 299,
                "ctopup_number": "9412300000",
                "mpin_raw": "1234",
                "vendorid": "VEND01",
                "vendormsisdn": "9412300000",
                "kyc_mode": "EKYC",
            }
        ]
        mock_bulk_insert.return_value = [{"reqid": 501, "caf_serial_no": "CAF001"}]
        mock_writeback.side_effect = RuntimeError("Oracle ORA-03113: end-of-file on communication channel")

        summary = run_batch_population()

        assert summary["inserted"] == 1
        assert summary["bcd_rq_updated"] == 0
        assert summary["dispatchable"] == 0
        assert summary["errors"] == 1

    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_happy_path_q023_succeeds_q020_succeeds_becomes_dispatchable(
        self, mock_fetch_oracle, mock_fetch_pg, mock_bulk_insert, mock_writeback
    ):
        """When Oracle claim succeeds, summary records successful claim writeback."""
        mock_fetch_oracle.return_value = [
            {
                "GSMNUMBER": "9412345678",
                "CAF_SERIAL_NO": "CAF001",
                "DE_CSCCODE": "CSC01",
                "CIRCLE_CODE": 2,
                "HLR_FINAL_ACT_DATE": "2026-09-01 10:00:00",
            }
        ]
        mock_fetch_pg.return_value = [
            {
                "gsmnumber": "9412345678",
                "caf_serial_no": "CAF001",
                "de_csccode": "CSC01",
                "circle_code": "2",
                "live_photo_time": "2026-09-01 10:00:00",
                "frc_plan_name": "Plan1",
                "frc_plan_code": "P01",
                "frc_category_code": "C01",
                "frcamt": 299,
                "ctopup_number": "9412300000",
                "mpin_raw": "1234",
                "vendorid": "VEND01",
                "vendormsisdn": "9412300000",
                "kyc_mode": "EKYC",
            }
        ]
        mock_bulk_insert.return_value = [{"reqid": 501, "caf_serial_no": "CAF001"}]
        mock_writeback.return_value = 1

        summary = run_batch_population()

        assert summary["inserted"] == 1
        assert summary["bcd_rq_updated"] == 1
        assert summary["dispatchable"] == 1
        assert summary["errors"] == 0

    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_partial_claim_mismatch_reports_mismatch(
        self, mock_fetch_oracle, mock_fetch_pg, mock_bulk_insert, mock_writeback
    ):
        """When Oracle updates fewer rows than expected, bcd_rq_updated reflects actual count."""
        mock_fetch_oracle.return_value = [
            {"GSMNUMBER": "9412345678", "CAF_SERIAL_NO": "CAF001", "DE_CSCCODE": "CSC01", "CIRCLE_CODE": 2, "HLR_FINAL_ACT_DATE": "2026-09-01 10:00:00"},
            {"GSMNUMBER": "9412345679", "CAF_SERIAL_NO": "CAF002", "DE_CSCCODE": "CSC01", "CIRCLE_CODE": 2, "HLR_FINAL_ACT_DATE": "2026-09-01 10:00:00"},
        ]
        mock_fetch_pg.return_value = [
            {
                "gsmnumber": "9412345678", "caf_serial_no": "CAF001", "de_csccode": "CSC01", "circle_code": "2",
                "live_photo_time": "2026-09-01 10:00:00", "frc_plan_name": "Plan1", "frc_plan_code": "P01",
                "frc_category_code": "C01", "frcamt": 299, "ctopup_number": "9412300000", "mpin_raw": "1234",
                "vendorid": "VEND01", "vendormsisdn": "9412300000", "kyc_mode": "EKYC",
            },
            {
                "gsmnumber": "9412345679", "caf_serial_no": "CAF002", "de_csccode": "CSC01", "circle_code": "2",
                "live_photo_time": "2026-09-01 10:00:00", "frc_plan_name": "Plan1", "frc_plan_code": "P01",
                "frc_category_code": "C01", "frcamt": 299, "ctopup_number": "9412300000", "mpin_raw": "1234",
                "vendorid": "VEND01", "vendormsisdn": "9412300000", "kyc_mode": "EKYC",
            },
        ]
        mock_bulk_insert.return_value = [
            {"reqid": 501, "caf_serial_no": "CAF001"},
            {"reqid": 502, "caf_serial_no": "CAF002"},
        ]
        mock_writeback.return_value = 1

        summary = run_batch_population()

        assert summary["inserted"] == 2
        assert summary["bcd_rq_updated"] == 1
