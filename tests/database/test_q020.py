"""Unit tests for Phase 7: Q020 Oracle BCD Claim Writeback Hardening.

Verifies:
- Exact Oracle identity predicate in SQL:
    WHERE GSMNUMBER       = :gsmnumber
      AND CAF_SERIAL_NO   = :caf_serial_no
      AND CIRCLE_CODE     = :circle_code
      AND FRC_FLOW_STATUS = 'NP'
      AND FRC_REQID IS NULL
- Input validation: raises ValueError if any identity field is missing
- Rowcount verification:
    - rc == 1: success, candidate claimed
    - rc == 0: claim failure -> conn.rollback() -> OracleClaimMismatchError
    - rc > 1: data-integrity alarm -> conn.rollback() -> OracleClaimDataIntegrityError
- Batch verification: expected claims == actual claims
- Populator integration: when claim fails or raises mismatch,
  Postgres staged rows remain in_status='S' (not dispatchable).
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from app.batch.populator import run_batch_population
from app.db.oracle import (
    BCD_STATUS_NP,
    BCD_STATUS_RQ,
    OracleClaimDataIntegrityError,
    OracleClaimMismatchError,
    batch_writeback_bcd_rq,
)


@contextmanager
def _mock_oracle_conn(cursor):
    """Context manager helper mimicking get_oracle_conn."""
    conn = MagicMock()
    conn.cursor.return_value = cursor
    yield conn


class TestQ020ExactIdentityPredicateAndValidation:
    """Test suite for Q020 SQL structure and identity parameter validation."""

    def test_empty_candidates_returns_zero_immediately(self):
        """Empty input must return 0 without opening a database connection."""
        with patch("app.db.oracle.get_oracle_conn") as mock_conn:
            result = batch_writeback_bcd_rq([])
            assert result == 0
            mock_conn.assert_not_called()

    @patch("app.db.oracle.get_oracle_conn")
    def test_q020_sql_contains_exact_identity_predicates(self, mock_get_conn):
        """SQL statement must enforce exact composite identity and state guards."""
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        candidates = [
            {
                "reqid": 101,
                "caf_serial_no": "CAF001",
                "gsmnumber": "9412345678",
                "circle_code": 2,
            }
        ]

        result = batch_writeback_bcd_rq(candidates)

        assert result == 1
        mock_conn.commit.assert_called_once()
        mock_conn.rollback.assert_not_called()

        mock_cursor.execute.assert_called_once()
        executed_sql, binds = mock_cursor.execute.call_args[0]

        # Verify SQL structure
        assert "FRC_FLOW_STATUS        = :status" in executed_sql
        assert "FRC_REQID              = :reqid" in executed_sql
        assert "WHERE GSMNUMBER       = :gsmnumber" in executed_sql
        assert "AND CAF_SERIAL_NO   = :caf_serial_no" in executed_sql
        assert "AND CIRCLE_CODE     = :circle_code" in executed_sql
        assert "AND FRC_FLOW_STATUS = :status_np" in executed_sql
        assert "AND FRC_REQID IS NULL" in executed_sql

        # Verify binds
        assert binds["status"] == BCD_STATUS_RQ
        assert binds["status_np"] == BCD_STATUS_NP
        assert binds["reqid"] == 101
        assert binds["gsmnumber"] == "9412345678"
        assert binds["caf_serial_no"] == "CAF001"
        assert binds["circle_code"] == 2

    @patch("app.db.oracle.get_oracle_conn")
    def test_q020_field_key_compatibility(self, mock_get_conn):
        """Accepts gsmno / gsmnumber / GSMNUMBER, CAF_SERIAL_NO, and CIRCLE_CODE."""
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        candidates = [
            {
                "reqid": "202",
                "CAF_SERIAL_NO": "CAF_UPPER",
                "gsmno": "9412345670",
                "CIRCLE_CODE": "55",
            }
        ]

        result = batch_writeback_bcd_rq(candidates)

        assert result == 1
        _, binds = mock_cursor.execute.call_args[0]
        assert binds["reqid"] == 202
        assert binds["caf_serial_no"] == "CAF_UPPER"
        assert binds["gsmnumber"] == "9412345670"
        assert binds["circle_code"] == 55

    @pytest.mark.parametrize(
        "invalid_candidate, expected_error_fragment",
        [
            (
                {"reqid": 1, "caf_serial_no": "", "gsmnumber": "9412345678", "circle_code": 2},
                "complete identity",
            ),
            (
                {"reqid": 1, "caf_serial_no": "CAF01", "gsmnumber": "", "circle_code": 2},
                "complete identity",
            ),
            (
                {"reqid": 1, "caf_serial_no": "CAF01", "gsmnumber": "9412345678", "circle_code": None},
                "complete identity",
            ),
            (
                {"reqid": 1, "circle_code": 2},
                "complete identity",
            ),
        ],
    )
    def test_missing_identity_fields_raises_value_error(self, invalid_candidate, expected_error_fragment):
        """Omitting CAF, GSM, or circle_code raises ValueError before DB updates."""
        with patch("app.db.oracle.get_oracle_conn") as mock_conn:
            with pytest.raises(ValueError) as exc_info:
                batch_writeback_bcd_rq([invalid_candidate])
            assert expected_error_fragment in str(exc_info.value)
            mock_conn.assert_not_called()


class TestQ020ClaimVerificationAndIntegrity:
    """Test suite for rowcount verification: 1=success, 0=mismatch, >1=data integrity alarm."""

    @patch("app.db.oracle.get_oracle_conn")
    def test_all_successful_claims(self, mock_get_conn):
        """When all candidates update exactly 1 row, transaction commits and returns count."""
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        candidates = [
            {"reqid": 101, "caf_serial_no": "CAF001", "gsmno": "9412345671", "circle_code": 2},
            {"reqid": 102, "caf_serial_no": "CAF002", "gsmno": "9412345672", "circle_code": 3},
            {"reqid": 103, "caf_serial_no": "CAF003", "gsmno": "9412345673", "circle_code": 55},
        ]

        claimed = batch_writeback_bcd_rq(candidates)

        assert claimed == 3
        assert mock_cursor.execute.call_count == 3
        mock_conn.commit.assert_called_once()
        mock_conn.rollback.assert_not_called()

    @patch("app.db.oracle.get_oracle_conn")
    def test_zero_row_claim_failure_triggers_rollback_and_mismatch_error(self, mock_get_conn):
        """When a candidate updates 0 rows (e.g. already claimed), transaction is rolled back."""
        mock_cursor = MagicMock()
        rowcounts = [1, 0]

        def _execute_side_effect(*args, **kwargs):
            mock_cursor.rowcount = rowcounts.pop(0)

        mock_cursor.execute.side_effect = _execute_side_effect

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        candidates = [
            {"reqid": 101, "caf_serial_no": "CAF001", "gsmno": "9412345671", "circle_code": 2},
            {"reqid": 102, "caf_serial_no": "CAF002", "gsmno": "9412345672", "circle_code": 2},
        ]

        with pytest.raises(OracleClaimMismatchError) as exc_info:
            batch_writeback_bcd_rq(candidates)

        assert "expected 2 claims" in str(exc_info.value)
        assert "1 successes" in str(exc_info.value)
        assert "1 failures (0 rows)" in str(exc_info.value)

        # Transaction MUST be rolled back, NEVER committed!
        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()

    @patch("app.db.oracle.get_oracle_conn")
    def test_multi_row_update_triggers_data_integrity_alarm(self, mock_get_conn):
        """When a candidate updates >1 row (duplicate identity in Oracle), triggers integrity alarm."""
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 2  # Updated 2 rows for a single candidate!
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        candidates = [
            {"reqid": 101, "caf_serial_no": "CAF_DUP", "gsmno": "9412345671", "circle_code": 2},
        ]

        with pytest.raises(OracleClaimDataIntegrityError) as exc_info:
            batch_writeback_bcd_rq(candidates)

        assert "alarms (>1 rows)" in str(exc_info.value)
        assert "Transaction rolled back" in str(exc_info.value)

        # Rollback must be called
        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()


class TestQ020PopulatorCrossDatabaseIntegration:
    """Test suite verifying populator behavior with hardened Q020 claim writeback."""

    @patch("app.batch.populator.mark_requests_dispatchable")
    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_populator_passes_complete_identity_to_writeback(
        self, mock_fetch_oracle, mock_fetch_pg, mock_bulk_insert, mock_writeback, mock_dispatchable
    ):
        """bulk_insert_frc_requests returns full identity and passes it to batch_writeback_bcd_rq."""
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
        # bulk_insert_frc_requests returns full 4-field identity dicts
        mock_bulk_insert.return_value = [
            {
                "reqid": 501,
                "caf_serial_no": "CAF001",
                "gsmno": "9412345678",
                "gsmnumber": "9412345678",
                "circle_code": 2,
            }
        ]
        mock_writeback.return_value = 1
        mock_dispatchable.return_value = 1

        summary = run_batch_population()

        assert summary["inserted"] == 1
        assert summary["bcd_rq_updated"] == 1
        assert summary["dispatchable"] == 1

        # batch_writeback_bcd_rq received the pairs with complete identity
        mock_writeback.assert_called_once_with(mock_bulk_insert.return_value)
        mock_dispatchable.assert_called_once_with([501])

    @patch("app.batch.populator.mark_requests_dispatchable")
    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_populator_claim_mismatch_keeps_staged_requests_undispatchable(
        self, mock_fetch_oracle, mock_fetch_pg, mock_bulk_insert, mock_writeback, mock_dispatchable
    ):
        """When batch_writeback_bcd_rq raises OracleClaimMismatchError, requests remain in_status='S'."""
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
        mock_bulk_insert.return_value = [
            {
                "reqid": 501,
                "caf_serial_no": "CAF001",
                "gsmno": "9412345678",
                "gsmnumber": "9412345678",
                "circle_code": 2,
            }
        ]
        mock_writeback.side_effect = OracleClaimMismatchError(
            "Oracle Q020 claim verification mismatch: expected 1 claims, got 0 successes"
        )

        summary = run_batch_population()

        assert summary["inserted"] == 1
        assert summary["bcd_rq_updated"] == 0
        assert summary["dispatchable"] == 0
        assert summary["errors"] == 1

        mock_dispatchable.assert_not_called()

    @patch("app.batch.populator.mark_requests_dispatchable")
    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_populator_data_integrity_alarm_keeps_staged_requests_undispatchable(
        self, mock_fetch_oracle, mock_fetch_pg, mock_bulk_insert, mock_writeback, mock_dispatchable
    ):
        """When batch_writeback_bcd_rq raises OracleClaimDataIntegrityError, requests remain in_status='S'."""
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
        mock_bulk_insert.return_value = [
            {
                "reqid": 501,
                "caf_serial_no": "CAF001",
                "gsmno": "9412345678",
                "gsmnumber": "9412345678",
                "circle_code": 2,
            }
        ]
        mock_writeback.side_effect = OracleClaimDataIntegrityError(
            "Oracle Q020 claim verification mismatch: 1 alarms (>1 rows)"
        )

        summary = run_batch_population()

        assert summary["inserted"] == 1
        assert summary["bcd_rq_updated"] == 0
        assert summary["dispatchable"] == 0
        assert summary["errors"] == 1

        mock_dispatchable.assert_not_called()
