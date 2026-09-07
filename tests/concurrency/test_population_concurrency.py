"""Concurrency test suite for batch population (Phase 15).

Verifies concurrency isolation and mutual exclusion:
1. Scheduler vs Manual API trigger contention on PostgreSQL advisory lock.
2. Two concurrent population workers: exactly one proceeds, the other cleanly skips.
3. Lock is released immediately upon completion or unexpected failure, allowing subsequent runs.
4. HTTP 409 Conflict returned to API callers when lock contention occurs.
"""

import threading
import time
from unittest.mock import MagicMock, patch
import pytest
from fastapi.testclient import TestClient

from app.batch.populator import run_batch_population
from app.context import ExecutionContext
from app.db.postgres import POPULATION_ADVISORY_LOCK_KEY, population_advisory_lock
from main import app


class TestPopulationConcurrency:
    """Test suite for population concurrency guards and advisory lock contention."""

    def test_scheduler_and_manual_api_contention(self):
        """When scheduler job holds advisory lock, manual admin API trigger returns 409 Conflict."""
        from app.config import settings
        client = TestClient(app)
        headers = {"X-Admin-Api-Key": settings.admin_api_key}

        # Simulate lock held by scheduler (acquired=False for subsequent attempt)
        with patch("app.batch.populator.population_advisory_lock") as mock_lock:
            # Yield False to simulate contention
            mock_lock.return_value.__enter__.return_value = False
            mock_lock.return_value.__exit__.return_value = None

            response = client.post("/admin/trigger-batch-population?zones=NZ", headers=headers)

            assert response.status_code == 409
            data = response.json()
            assert data.get("triggered") is False
            assert "advisory lock busy" in data.get("reason", "").lower()

    def test_two_concurrent_population_workers_mutual_exclusion(self):
        """Two concurrent worker threads calling run_batch_population: exactly one proceeds."""
        # A thread-safe mock simulating PostgreSQL advisory lock behavior
        lock_holder = []
        lock_mutex = threading.Lock()

        results = []

        def mock_advisory_lock():
            class MockLockContext:
                def __enter__(self):
                    with lock_mutex:
                        if len(lock_holder) == 0:
                            lock_holder.append("HELD")
                            self.acquired = True
                        else:
                            self.acquired = False
                    return self.acquired

                def __exit__(self, exc_type, exc_val, exc_tb):
                    if self.acquired:
                        with lock_mutex:
                            if lock_holder:
                                lock_holder.pop()

            return MockLockContext()

        ctx1 = ExecutionContext.create(source="SCHEDULED", zones_str="NZ", execution_id="worker_1")
        ctx2 = ExecutionContext.create(source="MANUAL_API", zones_str="NZ", execution_id="worker_2")

        with patch("app.batch.populator.population_advisory_lock", side_effect=mock_advisory_lock), \
             patch("app.batch.populator.fetch_eligible_bcd_records") as mock_oracle, \
             patch("app.batch.populator.fetch_cos_bcd_for_gsms") as mock_pg, \
             patch("app.batch.populator.bulk_insert_frc_requests") as mock_insert, \
             patch("app.batch.populator.batch_writeback_bcd_rq") as mock_wb, \
             patch("app.batch.populator.mark_requests_dispatchable") as mock_disp:

            def slow_oracle(*args, **kwargs):
                time.sleep(0.05)  # hold the lock briefly
                return [
                    {
                        "GSMNUMBER": "9412300001",
                        "CAF_SERIAL_NO": "CAF01",
                        "CIRCLE_CODE": 2,
                        "DE_CSCCODE": "CSC1",
                        "HLR_FINAL_ACT_DATE": "2026-09-01",
                    }
                ]

            mock_oracle.side_effect = slow_oracle
            mock_pg.return_value = [
                {
                    "gsmnumber": "9412300001",
                    "caf_serial_no": "CAF01",
                    "circle_code": 2,
                    "vendorid": "V1",
                    "vendormsisdn": "9412300000",
                    "frcamt": 299,
                    "mpin_raw": "1234",
                    "kyc_mode": "EKYC",
                }
            ]
            mock_insert.return_value = [{"reqid": 1001, "caf_serial_no": "CAF01"}]
            mock_wb.return_value = 1
            mock_disp.return_value = 1

            def run_worker(ctx):
                res = run_batch_population(context=ctx)
                results.append(res)

            t1 = threading.Thread(target=run_worker, args=(ctx1,))
            t2 = threading.Thread(target=run_worker, args=(ctx2,))

            t1.start()
            # Give t1 slight headstart so it enters the lock
            time.sleep(0.01)
            t2.start()

            t1.join()
            t2.join()

        assert len(results) == 2
        success_runs = [r for r in results if not r.get("skipped_lock_busy")]
        skipped_runs = [r for r in results if r.get("skipped_lock_busy")]

        assert len(success_runs) == 1
        assert len(skipped_runs) == 1

        assert success_runs[0]["staged"] == 1
        assert success_runs[0]["claim_success"] == 1
        assert skipped_runs[0]["status"] == "SKIPPED_LOCK_BUSY"
        assert skipped_runs[0]["staged"] == 0

    def test_subsequent_run_acquires_after_first_releases(self):
        """After first worker finishes and releases lock, a second worker successfully acquires it."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (True,)

        ctx1 = ExecutionContext.create(source="SCHEDULED", zones_str="NZ")
        ctx2 = ExecutionContext.create(source="MANUAL_API", zones_str="WZ")

        with patch("app.db.postgres.get_pg_conn") as mock_get_conn, \
             patch("app.batch.populator.fetch_eligible_bcd_records", return_value=[]):
            mock_get_conn.return_value.__enter__.return_value = mock_conn

            res1 = run_batch_population(context=ctx1)
            res2 = run_batch_population(context=ctx2)

            assert res1.get("skipped_lock_busy") is False
            assert res2.get("skipped_lock_busy") is False

            # Cursor executed try_lock and unlock for both runs (4 total executions)
            assert mock_cur.execute.call_count == 4

    def test_advisory_lock_release_on_unexpected_crash(self):
        """When an exception occurs inside protected block, lock is still unlocked."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        # try_lock returns True
        mock_cur.fetchone.return_value = (True,)

        with patch("app.db.postgres.get_pg_conn") as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_conn

            with pytest.raises(ZeroDivisionError):
                with population_advisory_lock() as acquired:
                    assert acquired is True
                    _ = 1 / 0

            # Unlock must have been called in finally block
            calls = mock_cur.execute.call_args_list
            assert len(calls) == 2
            assert "SELECT pg_try_advisory_lock" in calls[0][0][0]
            assert "SELECT pg_advisory_unlock" in calls[1][0][0]
