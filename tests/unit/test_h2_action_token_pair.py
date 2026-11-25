"""H2 (audit_report.md): a recharge must send the accessToken issued together with its
actionToken, even when concurrent recharges generate action tokens at the same time.

- get_action_token() returns ActionTokens(session, access, action) from one response.
- recharge() sends exactly that tuple, never the shared token_manager attributes.
- Concurrent generation: every pair stays consistent.
- An older access token never replaces a newer one in the shared cache.
- get_action_token() goes through get_access_token() (H1 lock + re-auth fallback).
"""

import asyncio
import base64
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.auth.token_manager import ActionTokens, PyroAuthService
from app.pyro_client import recharge


def _jwt(exp_offset):
    exp = int(datetime.now(timezone.utc).timestamp() + exp_offset)
    payload = base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode()).decode().rstrip("=")
    return f"h.{payload}.s"


class _Resp:
    def __init__(self, body):
        self._body = body
        self.text = json.dumps(body)

    def json(self):
        return self._body


def _fake_client(responses, delays=None):
    """httpx.AsyncClient stand-in: each GET returns the next response (after an optional delay)."""
    queue = list(responses)
    delays = list(delays or [0] * len(queue))
    sent_headers = []

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url, headers=None):
            idx = len(sent_headers)
            sent_headers.append(headers)
            await asyncio.sleep(delays[idx])
            return _Resp(queue[idx])

    return _Client, sent_headers


def _ok(access, action):
    return {"statusCode": 2000, "status": "SUCCESS", "message": "generate-action-token",
            "data": {"accessToken": access, "actionToken": action}}


def _service():
    svc = PyroAuthService()
    svc.session_token = "sess"
    svc.access_token = _jwt(900)
    svc._access_token_exp = datetime.now(timezone.utc).timestamp() + 900
    return svc


# ── token manager ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
class TestGetActionTokenPair:

    async def test_returns_pair_from_one_response(self):
        svc = _service()
        new_access = _jwt(901)
        client, sent = _fake_client([_ok(new_access, "act-1")])
        with patch("app.auth.token_manager.httpx.AsyncClient", lambda **kw: client()):
            tokens = await svc.get_action_token()
        assert tokens == ActionTokens("sess", new_access, "act-1")
        assert sent[0]["sessionToken"] == "sess"

    async def test_concurrent_pairs_stay_consistent(self):
        svc = _service()
        acc_a, acc_b = _jwt(910), _jwt(920)
        # A is requested first but answers last: the shared attribute ends up "wrong" for A.
        client, _ = _fake_client([_ok(acc_a, "act-A"), _ok(acc_b, "act-B")], delays=[0.05, 0.0])
        with patch("app.auth.token_manager.httpx.AsyncClient", lambda **kw: client()):
            a, b = await asyncio.gather(svc.get_action_token(), svc.get_action_token())
        assert (a.access_token, a.action_token) == (acc_a, "act-A")
        assert (b.access_token, b.action_token) == (acc_b, "act-B")

    async def test_older_access_token_does_not_replace_newer(self):
        svc = _service()
        newer, older = _jwt(1000), _jwt(950)
        svc.access_token = newer
        svc._access_token_exp = svc._parse_jwt_exp(newer)
        client, _ = _fake_client([_ok(older, "act")])
        with patch("app.auth.token_manager.httpx.AsyncClient", lambda **kw: client()):
            tokens = await svc.get_action_token()
        assert tokens.access_token == older      # the pair keeps its own token
        assert svc.access_token == newer         # the cache keeps the newest

    async def test_no_access_token_means_no_call(self):
        svc = _service()
        client, sent = _fake_client([])
        with patch.object(svc, "get_access_token", new_callable=AsyncMock, return_value=None), \
             patch("app.auth.token_manager.httpx.AsyncClient", lambda **kw: client()):
            assert await svc.get_action_token() is None
        assert sent == []

    async def test_uses_get_access_token_for_refresh(self):
        svc = _service()
        svc._access_token_exp = None  # expired
        client, sent = _fake_client([_ok(_jwt(900), "act")])
        with patch.object(svc, "get_access_token", new_callable=AsyncMock,
                          return_value="refreshed") as gat, \
             patch("app.auth.token_manager.httpx.AsyncClient", lambda **kw: client()):
            await svc.get_action_token()
        gat.assert_awaited_once()
        assert sent[0]["accessToken"] == "refreshed"

    async def test_failure_returns_none(self):
        svc = _service()
        client, _ = _fake_client([{"statusCode": 5000, "message": "Invalid access token"}])
        with patch("app.auth.token_manager.httpx.AsyncClient", lambda **kw: client()):
            assert await svc.get_action_token() is None


# ── recharge client ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_recharge_sends_the_pair_not_shared_attributes():
    http = MagicMock()
    http.post = AsyncMock(return_value=_Resp({"statusCode": 2002, "status": "IN_PROCESS",
                                              "data": {"transactionId": 1}}))
    http.post.return_value.status_code = 200
    with patch("app.pyro_client.token_manager") as tm, \
         patch("app.pyro_client._get_client", return_value=http), \
         patch("app.pyro_client._log", new_callable=AsyncMock):
        tm.session_token, tm.access_token = "shared-sess", "shared-access"
        tm.get_action_token = AsyncMock(return_value=ActionTokens("pair-sess", "pair-access", "pair-act"))
        await recharge(501, "CAF501", "9489902775", "2026-11-21",
                       "8903696189", "9489902775", 153, "123456")
    headers = http.post.call_args.kwargs["headers"]
    assert headers["sessionToken"] == "pair-sess"
    assert headers["accessToken"] == "pair-access"
    assert headers["actionToken"] == "pair-act"
