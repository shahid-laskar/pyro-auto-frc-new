from secrets import compare_digest

from fastapi import Header, HTTPException, status

from app.config import settings


async def require_admin_api_key(
    x_admin_api_key: str | None = Header(default=None, alias="X-Admin-API-Key"),
) -> None:
    """Require the configured admin API key for operational endpoints."""
    if not settings.admin_api_key:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Admin API key is not configured",
        )

    if not x_admin_api_key or not compare_digest(x_admin_api_key, settings.admin_api_key):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid admin API key",
        )



