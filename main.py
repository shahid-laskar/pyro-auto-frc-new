
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from app.auth.token_manager import token_manager
from app.callback import router as callback_router
from app.config import settings
from app.db.oracle import close_oracle_pool, init_oracle_pool
from app.db.postgres import close_pg_pool, init_pg_pool, postgres_health
from app.scheduler import start_scheduler, stop_scheduler
from app.security import require_admin_api_key

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler_started = False
    logger.info("FRC Pyro Recharge Service -- starting up")
    logger.info("  Pyro URL      : %s", settings.pyro_base_url)
    logger.info("  Callback URL  : %s/callback/recharge", settings.callback_base_url)    
    logger.info("  Scheduler     : %s", "enabled" if settings.enable_scheduler else "disabled")
    init_pg_pool()
    init_oracle_pool()
    try:
        ok = await token_manager.authenticate()
        if not ok:
            logger.warning("Pyro auth failed on startup -- service will start anyway; ")
    except Exception as exc:
        logger.warning("Pyro auth exception on startup (%s: %s) -- service will start anyway",
                       type(exc).__name__, exc)
    if settings.enable_scheduler:
        start_scheduler()
        scheduler_started = True
    else:
        logger.info("Scheduler disabled by ENABLE_SCHEDULER=false")
    logger.info("FRC Pyro Recharge Service -- ready")
    yield
    logger.info("FRC Pyro Recharge Service -- shutting down")
    if scheduler_started:
        stop_scheduler()
    close_oracle_pool()
    close_pg_pool()
    logger.info("Shutdown complete")


app = FastAPI(
    title="FRC Pyro Recharge Service",
    version="1.0.0",
    root_path="/api",
    lifespan=lifespan,
)

app.include_router(callback_router)


@app.get("/health", tags=["Ops"])
async def health():
    return {"status": "ok"}


@app.get("/health/database", tags=["Ops"])
async def database_health():
    """Non-mutating read/write PostgreSQL endpoint diagnostics."""
    import asyncio
    return await asyncio.to_thread(postgres_health)


@app.get("/token-status", tags=["Ops"])
async def token_status():
    from datetime import datetime, timezone
    tm  = token_manager
    exp = tm._access_token_exp
    now = datetime.now(timezone.utc).timestamp()
    return {
        "session_token_present": tm.session_token is not None,
        "access_token_present":  tm.access_token  is not None,
        "access_expires_in_s":   max(0, round(exp - now)) if exp else None,
    }


admin_dependencies = [Depends(require_admin_api_key)]


@app.get("/admin/zones", tags=["Admin"], dependencies=admin_dependencies)
async def get_zones_metadata():
    """Inspect active zone configuration, resolved circles, and supported zones."""
    from app.zones import CIRCLE_METADATA, VALID_ZONES, resolve_zones
    selection = resolve_zones(settings.enabled_zones)
    active_circles = (
        list(selection.circle_codes)
        if selection.circle_codes is not None
        else sorted(CIRCLE_METADATA.keys())
    )
    return {
        "status": "HEALTHY",
        "configured_zones": settings.enabled_zones,
        "resolved_mode": selection.mode,
        "active_zone_codes": list(selection.zone_codes),
        "active_circle_count": len(active_circles),
        "active_circles": active_circles,
        "all_available_zones": sorted(list(VALID_ZONES)),
    }


@app.post("/admin/trigger-batch-population", tags=["Admin"], dependencies=admin_dependencies)
async def trigger_batch_population(
    zones: Optional[str] = Query(default=None, description="Optional comma-separated zone codes (e.g. NZ,WZ) or ALL"),
):
    """Manually run Oracle BCD -> Postgres population with optional zone filter."""
    import asyncio
    from app.context import ExecutionContext
    from app.zones import InvalidZoneError
    from app.batch.populator import run_batch_population

    try:
        ctx = ExecutionContext.for_manual(zones_str=zones)
    except (InvalidZoneError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    summary = await asyncio.to_thread(run_batch_population, context=ctx)
    if summary.get("skipped_lock_busy"):
        return JSONResponse(
            status_code=409,
            content={
                "triggered": False,
                "reason": "Population job already in progress (advisory lock busy)",
                "execution_id": ctx.execution_id,
                "execution_source": ctx.source,
                "effective_zones": list(ctx.zone_codes),
                "mode": ctx.mode,
                "circles_count": ctx.circle_count,
                "summary": summary,
            },
        )
    return {
        "triggered": True,
        "execution_id": ctx.execution_id,
        "execution_source": ctx.source,
        "effective_zones": list(ctx.zone_codes),
        "mode": ctx.mode,
        "circles_count": ctx.circle_count,
        "summary": summary,
    }


@app.post("/admin/trigger-recharge", tags=["Admin"], dependencies=admin_dependencies)
async def trigger_recharge(
    zones: Optional[str] = Query(default=None, description="Optional comma-separated zone codes (e.g. NZ,WZ) or ALL"),
):
    """Manually trigger recharge dispatch with optional zone filter."""
    from app.context import ExecutionContext
    from app.zones import InvalidZoneError
    from app.processor import process_pending_recharges

    try:
        ctx = ExecutionContext.for_manual(zones_str=zones)
    except (InvalidZoneError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    summary = await process_pending_recharges(
        batch_size=settings.recharge_batch_size,
        context=ctx,
    )
    return {
        "triggered": True,
        "execution_id": ctx.execution_id,
        "execution_source": ctx.source,
        "effective_zones": list(ctx.zone_codes),
        "mode": ctx.mode,
        "circles_count": ctx.circle_count,
        "summary": summary,
    }


@app.post("/admin/trigger-status-check", tags=["Admin"], dependencies=admin_dependencies)
async def trigger_status_check():
    """Manually trigger status check fallback."""
    from app.status_checker import run_status_checks
    summary = await run_status_checks()
    return {"triggered": True, "summary": summary}


@app.post("/admin/trigger-reconciliation", tags=["Admin"], dependencies=admin_dependencies)
async def trigger_reconciliation():
    """Manually trigger cross-database reconciliation for staged requests."""
    import asyncio
    from app.batch.reconciler import reconcile_staged_requests
    summary = await asyncio.to_thread(reconcile_staged_requests)
    return {"triggered": True, "summary": summary}
