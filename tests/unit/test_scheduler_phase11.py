"""Unit tests for Phase 11: Scheduler ExecutionContext integration.

Verifies:
1. _batch_population_job builds ExecutionContext(source="SCHEDULED") using settings.enabled_zones.
2. _batch_population_job logs execution_id, source, zones, circle count, and mode.
3. _batch_population_job passes context to run_batch_population.
4. _batch_population_job handles lock contention logging.
5. _recharge_job builds ExecutionContext(source="SCHEDULED") using settings.enabled_zones.
6. _recharge_job logs execution_id, source, zones, circle count, and mode.
7. _recharge_job passes context to process_pending_recharges.
8. Exception handling in both jobs does not crash the scheduler.
"""

from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from app.config import settings
from app.context import ExecutionContext
from app.scheduler import _batch_population_job, _recharge_job


class TestSchedulerBatchPopulationJob:
    @pytest.mark.asyncio
    @patch("app.scheduler.logger.info")
    @patch("app.batch.populator.run_batch_population")
    async def test_batch_population_job_builds_and_logs_execution_context(
        self, mock_populator, mock_log_info
    ):
        """Job builds ExecutionContext with source=SCHEDULED and logs execution metadata."""
        mock_populator.return_value = {
            "batch_date": "2026-09-07",
            "inserted": 3,
            "dispatchable": 3,
        }

        with patch.object(settings, "enabled_zones", "NZ"):
            await _batch_population_job()

        # 1. Populator was called with ExecutionContext
        mock_populator.assert_called_once()
        passed_ctx = mock_populator.call_args.kwargs.get("context")
        assert passed_ctx is not None
        assert isinstance(passed_ctx, ExecutionContext)
        assert passed_ctx.source == "SCHEDULED"
        assert passed_ctx.zone_codes == ("NZ",)
        assert passed_ctx.mode == "FILTERED"
        assert passed_ctx.circle_count == 9

        # 2. Start log includes required fields: execution_id, source, zones, circle_count, mode
        start_log_calls = [
            c for c in mock_log_info.call_args_list
            if "Scheduler: batch population starting" in c[0][0]
        ]
        assert len(start_log_calls) == 1
        start_log_msg = start_log_calls[0][0][0]
        start_log_args = start_log_calls[0][0][1:]

        assert "execution_id=%s" in start_log_msg
        assert "source=%s" in start_log_msg
        assert "zones=%s" in start_log_msg
        assert "circle_count=%d" in start_log_msg
        assert "mode=%s" in start_log_msg

        assert start_log_args[0] == passed_ctx.execution_id
        assert start_log_args[1] == "SCHEDULED"
        assert start_log_args[2] == ["NZ"]
        assert start_log_args[3] == 9
        assert start_log_args[4] == "FILTERED"

    @pytest.mark.asyncio
    @patch("app.scheduler.logger.info")
    @patch("app.batch.populator.run_batch_population")
    async def test_batch_population_job_inherits_all_mode(
        self, mock_populator, mock_log_info
    ):
        """When settings.enabled_zones is ALL, ExecutionContext is in ALL mode."""
        mock_populator.return_value = {"inserted": 0}

        with patch.object(settings, "enabled_zones", "ALL"):
            await _batch_population_job()

        passed_ctx = mock_populator.call_args.kwargs.get("context")
        assert passed_ctx.source == "SCHEDULED"
        assert passed_ctx.mode == "ALL"
        assert passed_ctx.zone_codes == ("ALL",)
        assert passed_ctx.circle_count == 31

    @pytest.mark.asyncio
    @patch("app.scheduler.logger.warning")
    @patch("app.batch.populator.run_batch_population")
    async def test_batch_population_job_handles_lock_busy(
        self, mock_populator, mock_log_warn
    ):
        """Logs warning with execution_id when advisory lock is busy."""
        mock_populator.return_value = {
            "status": "SKIPPED_LOCK_BUSY",
            "skipped_lock_busy": True,
        }

        await _batch_population_job()

        mock_log_warn.assert_called_once()
        warn_msg = mock_log_warn.call_args[0][0]
        assert "SKIPPED due to active advisory lock" in warn_msg
        assert "execution_id=%s" in warn_msg

    @pytest.mark.asyncio
    @patch("app.scheduler.logger.error")
    @patch("app.batch.populator.run_batch_population")
    async def test_batch_population_job_catches_exceptions(
        self, mock_populator, mock_log_error
    ):
        """Exceptions in batch population are caught and logged as error."""
        mock_populator.side_effect = RuntimeError("Oracle connection lost")

        await _batch_population_job()

        mock_log_error.assert_called_once()
        assert "batch population exception" in mock_log_error.call_args[0][0]


class TestSchedulerRechargeJob:
    @pytest.mark.asyncio
    @patch("app.scheduler.logger.info")
    @patch("app.processor.process_pending_recharges", new_callable=AsyncMock)
    async def test_recharge_job_builds_and_logs_execution_context(
        self, mock_processor, mock_log_info
    ):
        """Recharge job builds ExecutionContext with source=SCHEDULED and logs execution metadata."""
        mock_processor.return_value = {"processed": 2, "registered": 2}

        with patch.object(settings, "enabled_zones", "NZ,WZ"):
            await _recharge_job()

        # 1. Processor was called with context
        mock_processor.assert_called_once()
        passed_ctx = mock_processor.call_args.kwargs.get("context")
        assert passed_ctx is not None
        assert isinstance(passed_ctx, ExecutionContext)
        assert passed_ctx.source == "SCHEDULED"
        assert passed_ctx.zone_codes == ("NZ", "WZ")
        assert passed_ctx.mode == "FILTERED"
        assert passed_ctx.circle_count == 14  # NZ (9) + WZ (5)

        # 2. Start log includes required fields
        start_log_calls = [
            c for c in mock_log_info.call_args_list
            if "Scheduler: recharge batch starting" in c[0][0]
        ]
        assert len(start_log_calls) == 1
        start_log_msg = start_log_calls[0][0][0]
        start_log_args = start_log_calls[0][0][1:]

        assert "execution_id=%s" in start_log_msg
        assert "source=%s" in start_log_msg
        assert "zones=%s" in start_log_msg
        assert "circle_count=%d" in start_log_msg
        assert "mode=%s" in start_log_msg

        assert start_log_args[0] == passed_ctx.execution_id
        assert start_log_args[1] == "SCHEDULED"
        assert start_log_args[2] == ["NZ", "WZ"]
        assert start_log_args[3] == 14
        assert start_log_args[4] == "FILTERED"

    @pytest.mark.asyncio
    @patch("app.scheduler.logger.error")
    @patch("app.processor.process_pending_recharges", new_callable=AsyncMock)
    async def test_recharge_job_catches_exceptions(
        self, mock_processor, mock_log_error
    ):
        """Exceptions in recharge job are caught and logged as error."""
        mock_processor.side_effect = RuntimeError("Pyro API timeout")

        await _recharge_job()

        mock_log_error.assert_called_once()
        assert "recharge exception" in mock_log_error.call_args[0][0]
