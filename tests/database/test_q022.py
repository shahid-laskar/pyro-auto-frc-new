"""Unit tests for Q022 PostgreSQL enrichment and Populator identity consistency (Phase 5).

Covers:
- app/db/postgres.py:fetch_cos_bcd_for_gsms (parameterization, circle filtering, text cast removal)
- app/batch/populator.py:run_batch_population (composite identity validation, circle consistency, Risk 5)
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, call, patch

import pytest

from app.context import ExecutionContext
from app.db.postgres import fetch_cos_bcd_for_gsms
from app.batch.populator import run_batch_population
from app.zones import NZ, WZ


def _make_mock_pg_cursor(rows=None):
    """Helper to construct a mock cursor for psycopg2 RealDictCursor."""
    cursor = MagicMock()
    cursor.fetchall.return_value = rows if rows is not None else []
    return cursor


@contextmanager
def _mock_pg_conn(cursor):
    """Context manager helper mimicking get_pg_conn context manager."""
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = None
    yield conn


class TestQ022PostgresEnrichment:
    """Test suite for Q022 query construction, parameterization, and text cast removal."""

    def test_q022_empty_gsm_numbers_returns_empty_list_without_db_call(self):
        """When gsm_numbers is empty, return [] immediately without acquiring DB connection."""
        with patch("app.db.postgres.get_pg_conn") as mock_get_conn:
            result = fetch_cos_bcd_for_gsms([])
            assert result == []
            mock_get_conn.assert_not_called()

    def test_q022_empty_circle_codes_returns_empty_list_without_db_call(self):
        """When circle_codes is explicitly empty sequence [], return [] without querying."""
        with patch("app.db.postgres.get_pg_conn") as mock_get_conn:
            result = fetch_cos_bcd_for_gsms(["9412345678"], circle_codes=[])
            assert result == []
            mock_get_conn.assert_not_called()

    @patch("app.db.postgres.get_pg_conn")
    def test_q022_all_mode_no_circle_predicate_and_no_text_cast(self, mock_get_conn):
        """When circle_codes=None (ALL mode), circle predicate is omitted and ::TEXT cast is absent."""
        mock_cursor = _make_mock_pg_cursor(rows=[
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
        ])
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        result = fetch_cos_bcd_for_gsms(["9412345678"], circle_codes=None)

        assert len(result) == 1
        assert result[0]["gsmnumber"] == "9412345678"
        assert result[0]["circle_code"] == "2"

        mock_cursor.execute.assert_called_once()
        executed_sql, executed_params = mock_cursor.execute.call_args[0]

        # Verify SQL: no circle predicate
        assert "cb.circle_code = ANY(" not in executed_sql
        # Verify SQL: no unnecessary ::TEXT column-side casts
        assert "cb.circle_code::TEXT" not in executed_sql
        assert "cb.circle_code                      AS circle_code" in executed_sql
        assert "(fp.circle_code = cb.circle_code OR fp.circle_code = '9999')" in executed_sql
        assert "fp.circle_code = cb.circle_code" in executed_sql

        # Verify params
        assert executed_params == {"gsms": ["9412345678"]}

    @patch("app.db.postgres.get_pg_conn")
    def test_q022_filtered_mode_single_circle(self, mock_get_conn):
        """When circle_codes=[2], ANY(%(allowed_circles)s) is bound with string list ['2']."""
        mock_cursor = _make_mock_pg_cursor(rows=[])
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        fetch_cos_bcd_for_gsms(["9412345678"], circle_codes=[2])

        mock_cursor.execute.assert_called_once()
        executed_sql, executed_params = mock_cursor.execute.call_args[0]

        assert "AND cb.circle_code = ANY(%(allowed_circles)s)" in executed_sql
        assert "cb.circle_code::TEXT" not in executed_sql
        assert executed_params == {
            "gsms": ["9412345678"],
            "allowed_circles": ["2"],
        }

    @patch("app.db.postgres.get_pg_conn")
    def test_q022_filtered_mode_nz_circles(self, mock_get_conn):
        """When NZ circles are passed, allowed_circles contains sorted strings for all 9 NZ circles."""
        mock_cursor = _make_mock_pg_cursor(rows=[])
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        fetch_cos_bcd_for_gsms(["9412345678", "9412345679"], circle_codes=NZ)

        mock_cursor.execute.assert_called_once()
        executed_sql, executed_params = mock_cursor.execute.call_args[0]

        assert "AND cb.circle_code = ANY(%(allowed_circles)s)" in executed_sql
        assert executed_params["gsms"] == ["9412345678", "9412345679"]
        assert executed_params["allowed_circles"] == ["2", "55", "56", "59", "60", "61", "62", "64", "65"]

    @patch("app.db.postgres.get_pg_conn")
    def test_q022_string_circle_codes_input_normalized(self, mock_get_conn):
        """String circle inputs like '02', '55' are normalized to integer strings and deduplicated."""
        mock_cursor = _make_mock_pg_cursor(rows=[])
        mock_get_conn.side_effect = lambda: _mock_pg_conn(mock_cursor)

        fetch_cos_bcd_for_gsms(["9412345678"], circle_codes=["02", "55", 2, "55"])

        mock_cursor.execute.assert_called_once()
        _, executed_params = mock_cursor.execute.call_args[0]

        assert executed_params["allowed_circles"] == ["2", "55"]


class TestPopulatorIdentityConsistency:
    """Test suite for populator identity consistency validation and candidate matching."""

    @patch("app.batch.populator.mark_requests_dispatchable")
    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_matching_identity_successfully_staged_and_claimed(
        self, mock_fetch_oracle, mock_fetch_pg, mock_bulk_insert, mock_writeback, mock_dispatchable
    ):
        mock_dispatchable.return_value = 1
        """When Oracle candidate and Postgres enriched record match on (GSM, CAF, CIRCLE), stage and claim."""
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
        mock_bulk_insert.return_value = [(101, "CAF001")]
        mock_writeback.return_value = 1

        summary = run_batch_population()

        assert summary["oracle_fetched"] == 1
        assert summary["ekyc_matched"] == 1
        assert summary["skipped_identity_mismatch"] == 0
        assert summary["inserted"] == 1
        assert summary["bcd_rq_updated"] == 1
        assert summary["errors"] == 0

        mock_bulk_insert.assert_called_once()
        inserted_row = mock_bulk_insert.call_args[0][0][0]
        assert inserted_row["caf_serial_no"] == "CAF001"
        assert inserted_row["gsmno"] == "9412345678"
        assert inserted_row["circle_code"] == 2

        mock_writeback.assert_called_once_with([(101, "CAF001")])

    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_caf_serial_mismatch_skips_staging_and_claiming(
        self, mock_fetch_oracle, mock_fetch_pg, mock_bulk_insert, mock_writeback
    ):
        """When Postgres returns a different CAF_SERIAL_NO for candidate GSM, skip staging and claiming."""
        mock_fetch_oracle.return_value = [
            {
                "GSMNUMBER": "9412345678",
                "CAF_SERIAL_NO": "CAF_ORACLE_001",
                "DE_CSCCODE": "CSC01",
                "CIRCLE_CODE": 2,
                "HLR_FINAL_ACT_DATE": "2026-09-01 10:00:00",
            }
        ]
        # Postgres returns CAF_PG_999 for the same GSM
        mock_fetch_pg.return_value = [
            {
                "gsmnumber": "9412345678",
                "caf_serial_no": "CAF_PG_999",
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

        summary = run_batch_population()

        assert summary["oracle_fetched"] == 1
        assert summary["skipped_identity_mismatch"] == 1
        assert summary["inserted"] == 0
        assert summary["bcd_rq_updated"] == 0

        mock_bulk_insert.assert_not_called()
        mock_writeback.assert_not_called()

    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_circle_code_mismatch_skips_staging_and_claiming(
        self, mock_fetch_oracle, mock_fetch_pg, mock_bulk_insert, mock_writeback
    ):
        """When Oracle circle (2) differs from Postgres circle (55), skip staging and claiming."""
        mock_fetch_oracle.return_value = [
            {
                "GSMNUMBER": "9412345678",
                "CAF_SERIAL_NO": "CAF001",
                "DE_CSCCODE": "CSC01",
                "CIRCLE_CODE": 2,  # Delhi
                "HLR_FINAL_ACT_DATE": "2026-09-01 10:00:00",
            }
        ]
        mock_fetch_pg.return_value = [
            {
                "gsmnumber": "9412345678",
                "caf_serial_no": "CAF001",
                "de_csccode": "CSC01",
                "circle_code": "55",  # Himachal Pradesh
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

        summary = run_batch_population()

        assert summary["oracle_fetched"] == 1
        assert summary["skipped_identity_mismatch"] == 1
        assert summary["inserted"] == 0
        assert summary["bcd_rq_updated"] == 0

        mock_bulk_insert.assert_not_called()
        mock_writeback.assert_not_called()

    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_populator_threads_context_into_oracle_and_postgres(
        self, mock_fetch_oracle, mock_fetch_pg, mock_bulk_insert, mock_writeback
    ):
        """ExecutionContext with NZ is passed to both fetch_eligible_bcd_records and fetch_cos_bcd_for_gsms."""
        ctx = ExecutionContext.for_manual(zones_str="NZ")
        mock_fetch_oracle.return_value = []

        summary = run_batch_population(context=ctx)

        assert summary["oracle_fetched"] == 0
        mock_fetch_oracle.assert_called_once_with(
            fetch_size=mock_fetch_oracle.call_args[1]["fetch_size"],
            circle_codes=ctx.circle_codes,
        )
        assert mock_fetch_oracle.call_args[1]["circle_codes"] == tuple(NZ)
        mock_fetch_pg.assert_not_called()

    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_populator_all_mode_threads_none_circle_codes(
        self, mock_fetch_oracle, mock_fetch_pg, mock_bulk_insert, mock_writeback
    ):
        """ExecutionContext with ALL passes circle_codes=None to both discovery and enrichment."""
        ctx = ExecutionContext.for_manual(zones_str="ALL")
        mock_fetch_oracle.return_value = [
            {
                "GSMNUMBER": "9412345678",
                "CAF_SERIAL_NO": "CAF001",
                "DE_CSCCODE": "CSC01",
                "CIRCLE_CODE": 2,
                "HLR_FINAL_ACT_DATE": "2026-09-01 10:00:00",
            }
        ]
        mock_fetch_pg.return_value = []

        run_batch_population(context=ctx)

        mock_fetch_oracle.assert_called_once_with(
            fetch_size=mock_fetch_oracle.call_args[1]["fetch_size"],
            circle_codes=None,
        )
        mock_fetch_pg.assert_called_once_with(
            gsm_numbers=["9412345678"],
            circle_codes=None,
        )

    @patch("app.batch.populator.mark_requests_dispatchable")
    @patch("app.batch.populator.batch_writeback_bcd_rq")
    @patch("app.batch.populator.bulk_insert_frc_requests")
    @patch("app.batch.populator.fetch_cos_bcd_for_gsms")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_risk_5_multiple_oracle_candidates_same_gsm_preserved(
        self, mock_fetch_oracle, mock_fetch_pg, mock_bulk_insert, mock_writeback, mock_dispatchable
    ):
        mock_dispatchable.return_value = 2
        """Verify Risk 5: two Oracle candidates with identical GSM but different CAFs are both preserved."""
        mock_fetch_oracle.return_value = [
            {
                "GSMNUMBER": "9412345678",
                "CAF_SERIAL_NO": "CAF_A",
                "DE_CSCCODE": "CSC01",
                "CIRCLE_CODE": 2,
                "HLR_FINAL_ACT_DATE": "2026-09-01 10:00:00",
            },
            {
                "GSMNUMBER": "9412345678",
                "CAF_SERIAL_NO": "CAF_B",
                "DE_CSCCODE": "CSC01",
                "CIRCLE_CODE": 2,
                "HLR_FINAL_ACT_DATE": "2026-09-01 10:30:00",
            },
        ]
        mock_fetch_pg.return_value = [
            {
                "gsmnumber": "9412345678",
                "caf_serial_no": "CAF_A",
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
            },
            {
                "gsmnumber": "9412345678",
                "caf_serial_no": "CAF_B",
                "de_csccode": "CSC01",
                "circle_code": "2",
                "live_photo_time": "2026-09-01 10:30:00",
                "frc_plan_name": "Plan2",
                "frc_plan_code": "P02",
                "frc_category_code": "C01",
                "frcamt": 499,
                "ctopup_number": "9412300000",
                "mpin_raw": "1234",
                "vendorid": "VEND01",
                "vendormsisdn": "9412300000",
                "kyc_mode": "DKYC",
            },
        ]
        mock_bulk_insert.return_value = [(101, "CAF_A"), (102, "CAF_B")]
        mock_writeback.return_value = 2

        summary = run_batch_population()

        assert summary["oracle_fetched"] == 2
        assert summary["ekyc_matched"] == 1
        assert summary["dkyc_matched"] == 1
        assert summary["skipped_identity_mismatch"] == 0
        assert summary["inserted"] == 2
        assert summary["bcd_rq_updated"] == 2

        mock_bulk_insert.assert_called_once()
        inserted_rows = mock_bulk_insert.call_args[0][0]
        assert len(inserted_rows) == 2
        assert inserted_rows[0]["caf_serial_no"] == "CAF_A"
        assert inserted_rows[1]["caf_serial_no"] == "CAF_B"
