"""Unit tests for Phase 10: PostgreSQL Advisory Lock Concurrency Guard.

Verifies:
1. Advisory lock key is stable and valid 64-bit integer.
2. Advisory lock acquisition, operation execution, and release on completion.
3. Advisory lock release even when protected operation raises an exception.
4. Advisory lock contention (try_lock returns False) yields False and skips unlock.
5. run_batch_population skips execution and marks skipped_lock_busy=True when lock is held.
6. FastAPI endpoint /admin/trigger-batch-population returns HTTP 409 when lock is busy.
7. Scheduler job logs warning when skipped_lock_busy=True.
"""

from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from fastapi.testclient import TestClient

from app.config import settings
from app.db.postgres import (
    POPULATION_ADVISORY_LOCK_KEY,
    population_advisory_lock,
)
from app.batch.populator import run_batch_population
from main import app


class TestPopulationAdvisoryLockKey:
    def test_stable_64bit_lock_key(self):
        """Lock key must be a stable integer within signed 64-bit range."""
        assert isinstance(POPULATION_ADVISORY_LOCK_KEY, int)
        assert POPULATION_ADVISORY_LOCK_KEY == 8292837261947261
        # Signed 64-bit integer boundaries
        assert -9223372036854775808 <= POPULATION_ADVISORY_LOCK_KEY <= 9223372036854775807


class TestPopulationAdvisoryLockContextManager:
    @patch("app.db.postgres.get_pg_conn")
    def test_lock_acquisition_and_release_on_success(self, mock_get_pg_conn):
        """Lock is acquired via pg_try_advisory_lock and released via pg_advisory_unlock on exit."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_get_pg_conn.return_value.__enter__.return_value = mock_conn

        # Mock try_lock returns True
        mock_cur.fetchone.return_value = (True,)

        operation_executed = False
        with population_advisory_lock() as acquired:
            assert acquired is True
            operation_executed = True

        assert operation_executed is True

        # Verify cursor executed try_lock then unlock
        calls = mock_cur.execute.call_args_list
        assert len(calls) == 2
        assert "SELECT pg_try_advisory_lock(%s);" in calls[0][0][0]
        assert calls[0][0][1] == (POPULATION_ADVISORY_LOCK_KEY,)

        assert "SELECT pg_advisory_unlock(%s);" in calls[1][0][0]
        assert calls[1][0][1] == (POPULATION_ADVISORY_LOCK_KEY,)

    @patch("app.db.postgres.get_pg_conn")
    def test_lock_released_on_exception(self, mock_get_pg_conn):
        """Lock is released even if protected operation raises an exception."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_get_pg_conn.return_value.__enter__.return_value = mock_conn

        mock_cur.fetchone.return_value = (True,)

        with pytest.raises(RuntimeError, match="Simulated population crash"):
            with population_advisory_lock() as acquired:
                assert acquired is True
                raise RuntimeError("Simulated population crash")

        # Verify unlock was still called in finally block
        calls = mock_cur.execute.call_args_list
        assert len(calls) == 2
        assert "SELECT pg_advisory_unlock(%s);" in calls[1][0][0]
        assert calls[1][0][1] == (POPULATION_ADVISORY_LOCK_KEY,)

    @patch("app.db.postgres.get_pg_conn")
    def test_lock_contention_yields_false_without_unlock(self, mock_get_pg_conn):
        """When lock is busy (try_lock returns False), yields False and does not unlock."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_get_pg_conn.return_value.__enter__.return_value = mock_conn

        # Mock try_lock returns False (already held by another worker)
        mock_cur.fetchone.return_value = (False,)

        with population_advisory_lock() as acquired:
            assert acquired is False

        # Only try_lock executed; unlock must NOT be called
        calls = mock_cur.execute.call_args_list
        assert len(calls) == 1
        assert "SELECT pg_try_advisory_lock(%s);" in calls[0][0][0]


class TestPopulatorLockIntegration:
    @patch("app.batch.populator.population_advisory_lock")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_run_batch_population_skips_when_lock_busy(
        self, mock_fetch_oracle, mock_lock
    ):
        """When advisory lock is busy, run_batch_population skips without fetching Oracle records."""
        # Setup context manager to yield acquired = False
        lock_ctx = MagicMock()
        lock_ctx.__enter__.return_value = False
        mock_lock.return_value = lock_ctx

        summary = run_batch_population()

        assert summary["skipped_lock_busy"] is True
        assert summary["status"] == "SKIPPED_LOCK_BUSY"
        assert summary["oracle_fetched"] == 0
        assert summary["inserted"] == 0
        mock_fetch_oracle.assert_not_called()

    @patch("app.batch.populator.population_advisory_lock")
    @patch("app.batch.populator.fetch_eligible_bcd_records")
    def test_run_batch_population_proceeds_when_lock_acquired(
        self, mock_fetch_oracle, mock_lock
    ):
        """When advisory lock is acquired, run_batch_population proceeds normally."""
        lock_ctx = MagicMock()
        lock_ctx.__enter__.return_value = True
        mock_lock.return_value = lock_ctx

        mock_fetch_oracle.return_value = []

        summary = run_batch_population()

        assert summary["skipped_lock_busy"] is False
        assert summary.get("status") != "SKIPPED_LOCK_BUSY"
        mock_fetch_oracle.assert_called_once()


class TestAdminEndpointLockContention:
    def setup_method(self):
        self.client = TestClient(app)
        self.headers = {"X-Admin-Api-Key": settings.admin_api_key}

    @patch("app.batch.populator.run_batch_population")
    def test_admin_trigger_returns_409_when_lock_busy(self, mock_populator):
        """POST /admin/trigger-batch-population returns HTTP 409 Conflict if lock is busy."""
        mock_populator.return_value = {
            "batch_date": "2026-09-07",
            "status": "SKIPPED_LOCK_BUSY",
            "skipped_lock_busy": True,
            "oracle_fetched": 0,
            "inserted": 0,
        }

        response = self.client.post(
            "/admin/trigger-batch-population",
            headers=self.headers,
        )

        assert response.status_code == 409
        body = response.json()
        assert body["triggered"] is False
        assert "already in progress" in body["reason"]
        assert body["summary"]["skipped_lock_busy"] is True

    @patch("app.batch.populator.run_batch_population")
    def test_admin_trigger_returns_200_when_successful(self, mock_populator):
        """POST /admin/trigger-batch-population returns HTTP 200 when successful."""
        mock_populator.return_value = {
            "batch_date": "2026-09-07",
            "skipped_lock_busy": False,
            "oracle_fetched": 5,
            "inserted": 5,
            "dispatchable": 5,
        }

        response = self.client.post(
            "/admin/trigger-batch-population",
            headers=self.headers,
        )

        assert response.status_code == 200
        body = response.json()
        assert body["triggered"] is True
        assert body["summary"]["inserted"] == 5


class TestSchedulerLockWarning:
    @pytest.mark.asyncio
    @patch("app.batch.populator.run_batch_population")
    @patch("app.scheduler.logger.warning")
    async def test_scheduler_logs_warning_on_lock_busy(
        self, mock_warn, mock_populator
    ):
        """Scheduler job logs warning when skipped_lock_busy is True."""
        from app.scheduler import _batch_population_job

        mock_populator.return_value = {
            "status": "SKIPPED_LOCK_BUSY",
            "skipped_lock_busy": True,
        }

        await _batch_population_job()

        mock_warn.assert_called_once()
        assert "SKIPPED due to active advisory lock" in mock_warn.call_args[0][0]
