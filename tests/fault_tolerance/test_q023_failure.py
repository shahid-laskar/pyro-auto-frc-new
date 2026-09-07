"""Fault tolerance test suite for Q023 PostgreSQL staging failure (Phase 15).

Verifies staging boundary isolation and transaction rollback guards:
1. PostgreSQL bulk insert failure:
   - Oracle claim writeback (Q020) is never executed when staging fails.
   - Summary reports errors=1, staged=0, claim_expected=0.
2. Identity mismatch discard:
   - Mismatched CAF or circle_code between Oracle and PostgreSQL is dropped before staging.
   - Oracle is never updated for mismatched candidates.
3. MPIN encryption failure:
   - If MPIN encryption fails, row is dropped before staging.
4. Staging transaction contract:
   - Verifies bulk_insert_frc_requests stages with in_status='S' and push_flag='N'.
"""

from unittest.mock import MagicMock, patch
import pytest

from app.batch.populator import run_batch_population
from app.context import ExecutionContext
from app.db.postgres import bulk_insert_frc_requests


from contextlib import contextmanager


@contextmanager
def _mock_pg_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = None
    yield conn


class TestQ023FailureFaultTolerance:
    """Test suite verifying PostgreSQL staging failure isolation and transaction safety."""

    @patch("app.batch.populator.mark_requests_dispatchable")
    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_q023_insert_failure_isolates_oracle_from_claim_writeback(
        self,
        mock_oracle_fetch,
        mock_pg_kyc,
        mock_bulk_insert,
        mock_writeback,
        mock_mark_dispatchable,
    ):
        """When bulk_insert_frc_requests fails, Oracle writeback is strictly prevented."""
        ctx = ExecutionContext.create(source="SCHEDULED", zones_str="NZ")

        mock_oracle_fetch.return_value = [
            {
                "GSMNUMBER": "9412300001",
                "CAF_SERIAL_NO": "CAF_001",
                "CIRCLE_CODE": 2,
                "DE_CSCCODE": "CSC1",
                "HLR_FINAL_ACT_DATE": "2026-09-01",
            }
        ]
        mock_pg_kyc.return_value = [
            {
                "gsmnumber": "9412300001",
                "caf_serial_no": "CAF_001",
                "circle_code": 2,
                "vendorid": "V1",
                "vendormsisdn": "9412300000",
                "frcamt": 299,
                "mpin_raw": "1234",
                "kyc_mode": "EKYC",
            }
        ]
        # PostgreSQL staging raises DB error
        mock_bulk_insert.side_effect = RuntimeError("PostgreSQL deadlock or connection lost")

        summary = run_batch_population(context=ctx)

        # Oracle claim must NOT be attempted
        mock_writeback.assert_not_called()
        mock_mark_dispatchable.assert_not_called()

        assert summary["errors"] == 1
        assert summary["staged"] == 0
        assert summary["claim_expected"] == 0
        assert summary["claim_success"] == 0
        assert summary["held"] == 0

    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_q023_caf_mismatch_dropped_before_staging(
        self,
        mock_oracle_fetch,
        mock_pg_kyc,
        mock_writeback,
        mock_bulk_insert,
    ):
        """Mismatched CAF serial number between Oracle and Postgres is discarded prior to staging."""
        ctx = ExecutionContext.create(source="SCHEDULED", zones_str="NZ")

        mock_oracle_fetch.return_value = [
            {
                "GSMNUMBER": "9412300001",
                "CAF_SERIAL_NO": "CAF_ORACLE_A",
                "CIRCLE_CODE": 2,
                "DE_CSCCODE": "CSC1",
                "HLR_FINAL_ACT_DATE": "2026-09-01",
            }
        ]
        mock_pg_kyc.return_value = [
            {
                "gsmnumber": "9412300001",
                "caf_serial_no": "CAF_POSTGRES_B",  # Mismatch!
                "circle_code": 2,
                "vendorid": "V1",
                "vendormsisdn": "9412300000",
                "frcamt": 299,
                "mpin_raw": "1234",
                "kyc_mode": "EKYC",
            }
        ]

        summary = run_batch_population(context=ctx)

        assert summary["skipped_identity_mismatch"] == 1
        assert summary["staged"] == 0
        mock_bulk_insert.assert_not_called()
        mock_writeback.assert_not_called()

    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator._encrypt_mpin")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_q023_mpin_encryption_failure_dropped_before_staging(
        self,
        mock_oracle_fetch,
        mock_pg_kyc,
        mock_encrypt,
        mock_writeback,
        mock_bulk_insert,
    ):
        """When MPIN encryption fails, row is skipped and never staged or claimed."""
        ctx = ExecutionContext.create(source="SCHEDULED", zones_str="NZ")

        mock_oracle_fetch.return_value = [
            {
                "GSMNUMBER": "9412300001",
                "CAF_SERIAL_NO": "CAF_001",
                "CIRCLE_CODE": 2,
                "DE_CSCCODE": "CSC1",
                "HLR_FINAL_ACT_DATE": "2026-09-01",
            }
        ]
        mock_pg_kyc.return_value = [
            {
                "gsmnumber": "9412300001",
                "caf_serial_no": "CAF_001",
                "circle_code": 2,
                "vendorid": "V1",
                "vendormsisdn": "9412300000",
                "frcamt": 299,
                "mpin_raw": "1234",
                "kyc_mode": "EKYC",
            }
        ]
        mock_encrypt.side_effect = ValueError("Corrupt secret key")

        summary = run_batch_population(context=ctx)

        assert summary["skipped_mpin_err"] == 1
        assert summary["staged"] == 0
        mock_bulk_insert.assert_not_called()
        mock_writeback.assert_not_called()

    @patch("app.db.postgres.get_pg_conn")
    def test_bulk_insert_frc_requests_stages_with_status_s_and_flag_n(self, mock_get_conn):
        """bulk_insert_frc_requests SQL inserts in_status='S' and push_flag='N'."""
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (901, "CAF001", "9412300001", 2)
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cur)

        rows = [
            {
                "caf_serial_no": "CAF001",
                "gsmno": "9412300001",
                "csccode": "CSC1",
                "circle_code": 2,
                "edate": "2026-09-01",
                "reqdate": "2026-09-01 10:00:00",
                "frc_plan_name": "P1",
                "frc_plan_code": "PC1",
                "frc_category_code": "CC1",
                "frcamt": 299,
                "ctopup_number": "9412300000",
                "vendormsisdn": "9412300000",
                "vendorid": "V1",
                "mpin": "enc_mpin",
                "mpin_length": 4,
                "max_retries": 3,
                "kyc_mode": "EKYC",
            }
        ]

        pairs = bulk_insert_frc_requests(rows)

        assert len(pairs) == 1
        assert pairs[0]["reqid"] == 901

        executed_sql = mock_cur.execute.call_args[0][0]
        assert "in_status" in executed_sql
        assert "'S'" in executed_sql
        assert "push_flag" in executed_sql
        assert "'N'" in executed_sql
        assert "RETURNING reqid, caf_serial_no" in executed_sql
