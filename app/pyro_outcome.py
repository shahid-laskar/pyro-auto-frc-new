"""Shared interpretation of Pyro replies (recharge, callback, transaction status).

One rule for every path so that the callback, the status checker and the
synchronous recharge reply can never disagree about the same payload.
"""

from typing import Optional, Tuple

OUTCOME_SUCCESS = "SUCCESS"
OUTCOME_RECONCILE = "RECONCILE"

# Final failure codes in a callback or status-check reply: mark FAILED, stop polling.
# Shared by app.callback and app.status_checker so the two cannot drift apart.
#   5010 only First Recharge accounts/plans allowed   5030 / 5005  IN / network failure
#   5011 invalid denomination                         5012 invalid MPIN
#   902  (undocumented, see audit L5)
#   5027 IN request failed / null IN response (v3.0)  1030 txn_in_failed (v3.0)
TERMINAL_FAILURE_CODES = frozenset({5010, 5030, 5005, 5011, 5012, 902, 5027, 1030})


def _norm(value) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text.upper() or None


def is_token_failure(status_code, message) -> bool:
    """True when Pyro rejected our tokens rather than the transaction.

    506 is always a token error. 5000 is shared by business and auth errors (spec §9);
    only the message tells 'Invalid access token' / 'Invalid token' /
    'Invalid credintials' (v2 spelling) / 'Invalid credentials' apart.
    """
    if status_code == 506:
        return True
    if status_code != 5000 or not message:
        return False
    text = str(message).lower()
    return "token" in text or "credintial" in text or "credential" in text


_DEALER_STATUS_PHRASES = (
    "blocked",                              # User blocked (dealerStatus == Blocked)
    "inactive", "in active",                # Inactive user (v3) / In active user (v2)
    "not available",                        # User not available (dealerStatus == Purged)
    "not authorized", "not authorised",     # You are not authorized to use this service
)


def is_dealer_status_failure(status_code, message) -> bool:
    """True for a 5000 that reports the dealer's status (v3.0 §9), not a token problem.

    Retrying will not help within a batch, so the row is failed at once (audit M2).
    """
    if status_code != 5000 or not message or is_token_failure(status_code, message):
        return False
    text = str(message).lower()
    return any(phrase in text for phrase in _DEALER_STATUS_PHRASES)


def classify_2000(response: dict) -> Tuple[str, Optional[str]]:
    """Classify a reply whose statusCode is 2000.

    statusCode 2000 is the success signal. ``status`` (top level) and ``data.status``
    are optional cross-checks: data.status is not in either spec's parameter table
    (v2.0/v3.0, audit D4), so its absence must not turn a success into a failure.

    Returns (OUTCOME_SUCCESS, None), or (OUTCOME_RECONCILE, reason) when a status
    is present and says something other than SUCCESS. A 2000 is never a failure:
    the dealer may already have been debited.
    """
    data = response.get("data")
    if not isinstance(data, dict):
        data = {}
    top = _norm(response.get("status"))
    inner = _norm(data.get("status"))
    contradicting = [
        f"{name}={raw!r}"
        for name, norm, raw in (("status", top, response.get("status")),
                                ("data.status", inner, data.get("status")))
        if norm is not None and norm != OUTCOME_SUCCESS
    ]
    if contradicting:
        return OUTCOME_RECONCILE, "2000 with non-SUCCESS " + ", ".join(contradicting)
    return OUTCOME_SUCCESS, None
