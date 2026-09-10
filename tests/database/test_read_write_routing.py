"""Tests for PostgreSQL read/write separation, connection routing, and diagnostics."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import psycopg2
import pytest
from pydantic import ValidationError

from app.config import Settings
from app.db.postgres import (
    bulk_insert_frc_requests,
    fetch_cos_bcd_for_gsms,
    fetch_pending_rows,
    fetch_pushed_rows_for_status_check,
    fetch_staged_unconfirmed_requests,
    find_row_by_pyro_trans_id,
    get_pg_read_conn,
    get_pg_write_conn,
    insert_txn_log,
    mark_as_failed,
    mark_as_pushed,
    mark_as_success,
    mark_requests_dispatchable,
    mark_requests_staging_failed,
    population_advisory_lock,
    postgres_health,
    release_unprocessed_claims,
    update_status_check_attempt,
)

BASE_CONFIG = {
    "pyro_base_url": "https://dummy-pyro.example.com",
    "pyro_api_key": "dummy_key",
    "pyro_login_id": "dummy_login",
    "pyro_password": "dummy_password",
    "pyro_secret_key": "dummy_secret_key_24b",
    "oracle_user": "dummy_user",
    "oracle_password": "dummy_password",
    "oracle_dsn": "dummy_host:1521/dummy_service",
    "callback_base_url": "https://callback.example.com",
}


@contextmanager
def _mock_conn_cm(mock_cursor=None):
    conn = MagicMock()
    if mock_cursor is None:
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = None
        mock_cursor.rowcount = 1
    conn.cursor.return_value.__enter__.return_value = mock_cursor
    conn.cursor.return_value.__exit__.return_value = None
    yield conn


class TestPostgresConfigurationRouting:
    """Verify Settings accepts canonical read/write variables and legacy aliases."""

    def test_canonical_read_write_settings(self):
        s = Settings(
            **BASE_CONFIG,
            pg_write_host="10.201.222.77",
            pg_write_port=5432,
            pg_write_database="postgres",
            pg_write_user="write_user",
            pg_write_password="write_password",
            pg_read_host="10.201.222.77",
            pg_read_port=5433,
            pg_read_database="postgres",
            pg_read_user="read_user",
            pg_read_password="read_password",
        )
        assert s.pg_write_host == "10.201.222.77"
        assert s.pg_write_port == 5432
        assert s.pg_read_host == "10.201.222.77"
        assert s.pg_read_port == 5433
        assert s.pg_read_user == "read_user"

    def test_legacy_write_variables_mapped_to_write_settings(self, monkeypatch):
        monkeypatch.setenv("PG_HOST", "legacy_write_host")
        monkeypatch.setenv("PG_PORT", "5432")
        monkeypatch.setenv("PG_DATABASE", "legacy_db")
        monkeypatch.setenv("PG_USER", "legacy_user")
        monkeypatch.setenv("PG_PASSWORD", "legacy_pass")
        monkeypatch.setenv("PG_READ_HOST", "replica_host")
        monkeypatch.setenv("PG_READ_PORT", "5433")
        monkeypatch.setenv("PG_READ_DATABASE", "replica_db")
        monkeypatch.setenv("PG_READ_USER", "replica_user")
        monkeypatch.setenv("PG_READ_PASSWORD", "replica_pass")

        s = Settings(_env_file=None, **BASE_CONFIG)
        assert s.pg_write_host == "legacy_write_host"
        assert s.pg_write_port == 5432
        assert s.pg_write_database == "legacy_db"
        assert s.pg_write_user == "legacy_user"
        assert s.pg_write_password == "legacy_pass"
        assert s.pg_read_host == "replica_host"
        assert s.pg_read_port == 5433

    def test_missing_read_host_fails_closed(self, monkeypatch):
        monkeypatch.delenv("PG_READ_HOST", raising=False)
        with pytest.raises(ValidationError):
            Settings(
                _env_file=None,
                **BASE_CONFIG,
                pg_write_host="10.201.222.77",
                pg_write_database="postgres",
                pg_write_user="write_user",
                pg_write_password="write_password",
                # pg_read_host omitted
                pg_read_database="postgres",
                pg_read_user="read_user",
                pg_read_password="read_password",
            )


class TestConnectionRouting:
    """Verify Q022 routes exclusively to read pool and all other DB operations route to write pool."""

    @patch("app.db.postgres.get_pg_write_conn")
    @patch("app.db.postgres.get_pg_read_conn")
    def test_q022_routes_to_read_pool_only(self, mock_read_conn, mock_write_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_read_conn.side_effect = lambda: _mock_conn_cm(mock_cursor)

        result = fetch_cos_bcd_for_gsms(["9412345678"], circle_codes=["02"])
        assert result == []
        mock_read_conn.assert_called_once()
        mock_write_conn.assert_not_called()

    @patch("app.db.postgres.get_pg_write_conn")
    @patch("app.db.postgres.get_pg_read_conn")
    def test_q023_bulk_insert_routes_to_write_pool_only(self, mock_read_conn, mock_write_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1, "CAF01", "9412345678", "02")
        mock_write_conn.side_effect = lambda: _mock_conn_cm(mock_cursor)

        rows = [{
            "caf_serial_no": "CAF01",
            "gsmno": "9412345678",
            "batch_date": "2026-09-10",
            "client_txn_id": "TXN01",
            "circle_code": "02",
            "vendorcode": "V01",
            "vendormsisdn": "9412345670",
            "ctopup_number": "9412345671",
            "frcamt": 100,
            "mpin": "1234",
            "mpin_length": 4,
            "initial_statuscode": 0,
            "initial_remarks": "INIT",
            "in_status": "S",
        }]
        bulk_insert_frc_requests(rows)
        mock_write_conn.assert_called_once()
        mock_read_conn.assert_not_called()

    @patch("app.db.postgres.get_pg_write_conn")
    @patch("app.db.postgres.get_pg_read_conn")
    def test_q024_fetch_pending_rows_routes_to_write_pool_only(self, mock_read_conn, mock_write_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_write_conn.side_effect = lambda: _mock_conn_cm(mock_cursor)

        fetch_pending_rows(batch_size=10, circle_codes=["02"])
        mock_write_conn.assert_called_once()
        mock_read_conn.assert_not_called()

    @patch("app.db.postgres.get_pg_write_conn")
    @patch("app.db.postgres.get_pg_read_conn")
    def test_advisory_lock_routes_to_write_pool_only(self, mock_read_conn, mock_write_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (True,)
        mock_write_conn.side_effect = lambda: _mock_conn_cm(mock_cursor)

        with population_advisory_lock() as acquired:
            assert acquired is True

        mock_write_conn.assert_called_once()
        mock_read_conn.assert_not_called()

    @patch("app.db.postgres.get_pg_write_conn")
    @patch("app.db.postgres.get_pg_read_conn")
    def test_mark_requests_dispatchable_routes_to_write_pool_only(self, mock_read_conn, mock_write_conn):
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_write_conn.side_effect = lambda: _mock_conn_cm(mock_cursor)

        mark_requests_dispatchable([1, 2])
        mock_write_conn.assert_called_once()
        mock_read_conn.assert_not_called()

    @patch("app.db.postgres.get_pg_write_conn")
    @patch("app.db.postgres.get_pg_read_conn")
    def test_mark_requests_staging_failed_routes_to_write_pool_only(self, mock_read_conn, mock_write_conn):
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_write_conn.side_effect = lambda: _mock_conn_cm(mock_cursor)

        mark_requests_staging_failed([1], "reason")
        mock_write_conn.assert_called_once()
        mock_read_conn.assert_not_called()

    @patch("app.db.postgres.get_pg_write_conn")
    @patch("app.db.postgres.get_pg_read_conn")
    def test_fetch_staged_unconfirmed_requests_routes_to_write_pool_only(self, mock_read_conn, mock_write_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_write_conn.side_effect = lambda: _mock_conn_cm(mock_cursor)

        fetch_staged_unconfirmed_requests(limit=10)
        mock_write_conn.assert_called_once()
        mock_read_conn.assert_not_called()

    @patch("app.db.postgres.get_pg_write_conn")
    @patch("app.db.postgres.get_pg_read_conn")
    def test_release_unprocessed_claims_routes_to_write_pool_only(self, mock_read_conn, mock_write_conn):
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_write_conn.side_effect = lambda: _mock_conn_cm(mock_cursor)

        release_unprocessed_claims([1])
        mock_write_conn.assert_called_once()
        mock_read_conn.assert_not_called()

    @patch("app.db.postgres.get_pg_write_conn")
    @patch("app.db.postgres.get_pg_read_conn")
    def test_mark_as_pushed_routes_to_write_pool_only(self, mock_read_conn, mock_write_conn):
        mock_cursor = MagicMock()
        mock_write_conn.side_effect = lambda: _mock_conn_cm(mock_cursor)

        mark_as_pushed(1, 100, "resp", "msg", 0)
        mock_write_conn.assert_called_once()
        mock_read_conn.assert_not_called()

    @patch("app.db.postgres.get_pg_write_conn")
    @patch("app.db.postgres.get_pg_read_conn")
    def test_mark_as_success_routes_to_write_pool_only(self, mock_read_conn, mock_write_conn):
        mock_cursor = MagicMock()
        mock_write_conn.side_effect = lambda: _mock_conn_cm(mock_cursor)

        mark_as_success(1, "resp", 10.0, 20.0, 0)
        mock_write_conn.assert_called_once()
        mock_read_conn.assert_not_called()

    @patch("app.db.postgres.get_pg_write_conn")
    @patch("app.db.postgres.get_pg_read_conn")
    def test_mark_as_failed_routes_to_write_pool_only(self, mock_read_conn, mock_write_conn):
        mock_cursor = MagicMock()
        mock_write_conn.side_effect = lambda: _mock_conn_cm(mock_cursor)

        mark_as_failed(1, "F", "err", "resp", 500)
        mock_write_conn.assert_called_once()
        mock_read_conn.assert_not_called()

    @patch("app.db.postgres.get_pg_write_conn")
    @patch("app.db.postgres.get_pg_read_conn")
    def test_fetch_pushed_rows_for_status_check_routes_to_write_pool_only(self, mock_read_conn, mock_write_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_write_conn.side_effect = lambda: _mock_conn_cm(mock_cursor)

        fetch_pushed_rows_for_status_check()
        mock_write_conn.assert_called_once()
        mock_read_conn.assert_not_called()

    @patch("app.db.postgres.get_pg_write_conn")
    @patch("app.db.postgres.get_pg_read_conn")
    def test_update_status_check_attempt_routes_to_write_pool_only(self, mock_read_conn, mock_write_conn):
        mock_cursor = MagicMock()
        mock_write_conn.side_effect = lambda: _mock_conn_cm(mock_cursor)

        update_status_check_attempt(1)
        mock_write_conn.assert_called_once()
        mock_read_conn.assert_not_called()

    @patch("app.db.postgres.get_pg_write_conn")
    @patch("app.db.postgres.get_pg_read_conn")
    def test_find_row_by_pyro_trans_id_routes_to_write_pool_only(self, mock_read_conn, mock_write_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_write_conn.side_effect = lambda: _mock_conn_cm(mock_cursor)

        find_row_by_pyro_trans_id(12345)
        mock_write_conn.assert_called_once()
        mock_read_conn.assert_not_called()

    @patch("app.db.postgres.get_pg_write_conn")
    @patch("app.db.postgres.get_pg_read_conn")
    def test_insert_txn_log_routes_to_write_pool_only(self, mock_read_conn, mock_write_conn):
        mock_cursor = MagicMock()
        mock_write_conn.side_effect = lambda: _mock_conn_cm(mock_cursor)

        insert_txn_log(
            frc_reqid=1, caf_serial_no="C1", gsmno="9412345678",
            batch_date="2026-09-10", client_txn_id="TX1",
            api_stage="RECHARGE", api_endpoint="/recharge", http_method="POST",
            attempt_no=1, request_headers="{}", request_body="{}",
            response_http_code=200, response_body="{}",
            pyro_status_code=0, pyro_status_text="OK", pyro_txn_id=111,
            call_started_at=None, call_ended_at=None, duration_ms=100,
            is_success="Y",
        )
        mock_write_conn.assert_called_once()
        mock_read_conn.assert_not_called()


class TestRetryIsolationAndNoCrossFallback:
    """Verify that read pool retry never falls back to the write pool."""

    @patch("app.db.postgres.get_pg_write_conn")
    @patch("app.db.postgres.get_pg_read_conn")
    def test_q022_operational_error_retries_on_read_pool_only(self, mock_read_conn, mock_write_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []

        # First call raises OperationalError, second succeeds
        mock_read_conn.side_effect = [
            psycopg2.OperationalError("Stale connection"),
            _mock_conn_cm(mock_cursor),
        ]

        result = fetch_cos_bcd_for_gsms(["9412345678"], circle_codes=["02"])
        assert result == []
        assert mock_read_conn.call_count == 2
        mock_write_conn.assert_not_called()


class TestPostgresHealthDiagnostics:
    """Verify database health probes both pools non-mutatingly."""

    @patch("app.db.postgres._read_pool")
    @patch("app.db.postgres._write_pool")
    def test_health_reports_both_endpoints(self, mock_write_pool, mock_read_pool):
        read_conn = MagicMock()
        read_cur = MagicMock()
        read_cur.fetchone.return_value = (5433, True)
        read_conn.cursor.return_value.__enter__.return_value = read_cur
        mock_read_pool.getconn.return_value = read_conn

        write_conn = MagicMock()
        write_cur = MagicMock()
        write_cur.fetchone.return_value = (5432, False)
        write_conn.cursor.return_value.__enter__.return_value = write_cur
        mock_write_pool.getconn.return_value = write_conn

        health = postgres_health()

        assert health["read"]["status"] == "ok"
        assert health["read"]["server_port"] == 5433
        assert health["read"]["in_recovery"] is True

        assert health["write"]["status"] == "ok"
        assert health["write"]["server_port"] == 5432
        assert health["write"]["in_recovery"] is False
