"""H1 (audit_report.md): a token outage must not use up status-check attempts.

- get_access_token() falls back to authenticate() when refresh fails, and returns
  None (never a stale token) when that fails too. Concurrent callers refresh once.
- check_transaction_status() does not call Pyro without a token (-1, not counted).
- A token-type 5000 / 506 reply in the status checker is not counted as an attempt,
  never parks or fails the row, invalidates the token and stops the run.
- A business 5000 is still counted as before.
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from app.auth.token_manager import PyroAuthService
from app.pyro_client import check_transaction_status
from app.pyro_outcome import is_token_failure
from app.status_checker import run_status_checks

from tests.unit.test_p1_state_integrity import sc  # noqa: F401  (fixture)


def _future_exp():
    return datetime.now(timezone.utc).timestamp() + 900


# ── token manager ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
class TestGetAccessTokenFallback:

    async def test_valid_token_is_returned_without_refresh(self):
        svc = PyroAuthService()
        svc.access_token, svc._access_token_exp = "tok", _future_exp()
        with patch.object(svc, "refresh_access_token", new_callable=AsyncMock) as refresh:
            assert await svc.get_access_token() == "tok"
        refresh.assert_not_called()

    async def test_refresh_failure_falls_back_to_authenticate(self):
        svc = PyroAuthService()
        svc.access_token = "stale"

        async def _login():
            svc.access_token, svc._access_token_exp = "fresh", _future_exp()
            return True

        with patch.object(svc, "refresh_access_token", new_callable=AsyncMock, return_value=False), \
             patch.object(svc, "authenticate", side_effect=_login) as auth:
            assert await svc.get_access_token() == "fresh"
        auth.assert_awaited_once()

    async def test_both_fail_returns_none_not_stale_token(self):
        svc = PyroAuthService()
        svc.access_token = "stale"
        with patch.object(svc, "refresh_access_token", new_callable=AsyncMock, return_value=False), \
             patch.object(svc, "authenticate", new_callable=AsyncMock, return_value=False):
            assert await svc.get_access_token() is None

    async def test_concurrent_callers_refresh_once(self):
        svc = PyroAuthService()

        async def _refresh():
            await asyncio.sleep(0.01)
            svc.access_token, svc._access_token_exp = "fresh", _future_exp()
            return True

        with patch.object(svc, "refresh_access_token", side_effect=_refresh) as refresh:
            results = await asyncio.gather(*(svc.get_access_token() for _ in range(5)))
        assert results == ["fresh"] * 5
        assert refresh.await_count == 1

    async def test_invalidate_forces_refresh_but_keeps_session(self):
        svc = PyroAuthService()
        svc.session_token, svc.access_token, svc._access_token_exp = "s", "tok", _future_exp()
        svc.invalidate_access_token()
        assert svc.session_token == "s"
        assert not svc._is_access_token_valid()


# ── status-check client ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_status_check_without_token_does_not_call_pyro():
    with patch("app.pyro_client.token_manager") as tm, \
         patch("app.pyro_client._get_client") as get_client, \
         patch("app.pyro_client._log", new_callable=AsyncMock):
        tm.get_access_token = AsyncMock(return_value=None)
        resp = await check_transaction_status(501, "CAF501", "9489902775", "2026-11-21", "43385")
    assert resp["statusCode"] == -1
    get_client.assert_not_called()


# ── shared token-failure rule ─────────────────────────────────────────────────

@pytest.mark.parametrize("code,msg,expected", [
    (5000, "Invalid access token", True),
    (5000, "Invalid credintials", True),
    (506, None, True),
    (5000, "User blocked", False),
    (5005, "Invalid token", False),
])
def test_is_token_failure(code, msg, expected):
    assert is_token_failure(code, msg) is expected


# ── status checker ────────────────────────────────────────────────────────────

@pytest.fixture
def sc_tm(sc):  # noqa: F811
    with patch("app.status_checker.token_manager") as tm:
        sc["tm"] = tm
        yield sc


@pytest.mark.asyncio
class TestStatusCheckerTokenFailure:

    @pytest.mark.parametrize("resp", [
        {"statusCode": 5000, "status": "FAILED", "message": "Invalid access token"},
        {"statusCode": 506, "status": "FAILED", "message": "Invalid token"},
    ])
    async def test_token_failure_not_counted_on_last_attempt(self, sc_tm, resp):
        # fixture row is at status_check_count=4, so this is the 5th (last) attempt
        sc_tm["check"].return_value = resp
        summary = await run_status_checks()
        sc_tm["attempt"].assert_not_called()
        sc_tm["park"].assert_not_called()
        sc_tm["failed"].assert_not_called()
        sc_tm["tm"].invalidate_access_token.assert_called_once()
        assert summary["token_error"] == 1

    async def test_token_failure_stops_the_run(self, sc_tm):
        row = dict(sc_tm["fetch"].return_value[0])
        sc_tm["fetch"].return_value = [row, {**row, "reqid": 502}]
        sc_tm["check"].return_value = {"statusCode": 5000, "message": "Invalid access token"}
        summary = await run_status_checks()
        sc_tm["check"].assert_awaited_once()
        assert summary["token_error"] == 1 and summary["checked"] == 2

    async def test_business_5000_is_still_counted(self, sc_tm):
        sc_tm["check"].return_value = {"statusCode": 5000, "message": "User blocked"}
        await run_status_checks()
        sc_tm["attempt"].assert_awaited_once()
        sc_tm["tm"].invalidate_access_token.assert_not_called()
