import asyncio
import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.auth.token_manager import token_manager
from app.config import settings

logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler()


async def _daily_auth_job():
    logger.info("Scheduler: daily re-auth")
    try:
        ok = await token_manager.authenticate()
        logger.info("Scheduler: daily re-auth %s", "OK" if ok else "FAILED")
    except Exception as exc:
        logger.error("Scheduler: daily re-auth exception: %s", exc)


async def _batch_population_job():
    logger.info("Scheduler: batch population starting")
    try:
        from app.batch.populator import run_batch_population
        summary = await asyncio.to_thread(run_batch_population)
        logger.info("Scheduler: batch population done -- %s", summary)
    except Exception as exc:
        logger.error("Scheduler: batch population exception: %s", exc)


async def _recharge_job():
    logger.info("Scheduler: recharge batch starting")
    try:
        from app.processor import process_pending_recharges
        summary = await process_pending_recharges(
            batch_size=settings.recharge_batch_size
        )
        logger.info("Scheduler: recharge done -- %s", summary)
    except Exception as exc:
        logger.error("Scheduler: recharge exception: %s", exc, exc_info=True)


async def _status_check_job():
    try:
        from app.status_checker import run_status_checks
        summary = await run_status_checks()
        logger.info("Scheduler: status check done -- %s", summary)
    except Exception as exc:
        logger.error("Scheduler: status check exception: %s", exc)


def start_scheduler():
    now = datetime.now(timezone.utc)

    scheduler.add_job(
        _daily_auth_job,
        CronTrigger(hour=settings.scheduler_auth_hour,
                    minute=settings.scheduler_auth_minute),
        id="daily_auth",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        _batch_population_job,
        IntervalTrigger(minutes=settings.scheduler_batch_population_interval_minutes),
        id="batch_population",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=settings.scheduler_batch_population_grace_seconds,
        next_run_time=now if settings.run_batch_on_startup else None,
    )

    scheduler.add_job(
        _recharge_job,
        IntervalTrigger(minutes=settings.scheduler_recharge_interval_minutes),
        id="recharge_batch",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=settings.scheduler_recharge_grace_seconds,
        next_run_time=now if settings.run_recharge_on_startup else None,
    )

    scheduler.add_job(
        _status_check_job,
        IntervalTrigger(minutes=settings.scheduler_status_check_interval_minutes),
        id="status_check",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=settings.scheduler_status_check_grace_seconds,       
    )

    scheduler.start()
    logger.info(
        "Scheduler started -- "
        "batch_pop: every %dmin | recharge: every %dmin | "
        "status_check: every %dmin | daily_auth: %02d:%02d",
        settings.scheduler_batch_population_interval_minutes,
        settings.scheduler_recharge_interval_minutes,
        settings.scheduler_status_check_interval_minutes,
        settings.scheduler_auth_hour,
        settings.scheduler_auth_minute,
    )

def stop_scheduler():
    scheduler.shutdown()
    logger.info("Scheduler stopped")