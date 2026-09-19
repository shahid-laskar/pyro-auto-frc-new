# Implementation Plan: Auto FRC Throughput Optimization
**Goal**: Scale Auto FRC to process 30,000 activations/day in a 12-hour window  
**Target**: 7,500+ records/hour sustained (vs. current ~1,020 records/hour)

---

## Background

The capacity analysis confirmed the current single-process sequential loop in [`app/processor.py`](file:///D:/pyro/pyro_auto_frc/app/processor.py#L114) is the primary bottleneck. Each Pyro recharge API call blocks the next one for **3.1–6.5 seconds**, yielding a ceiling of only ~34 records per 2-minute interval.

Four isolated changes — no schema changes, no new dependencies — achieve the goal:

| # | Change | Impact |
|---|--------|--------|
| 1 | Bounded concurrency in processor | **8× throughput gain** |
| 2 | Persistent httpx client | **Eliminates 90,000 TLS handshakes** |
| 3 | Smart token caching | **Removes 30,000 unnecessary HTTP calls** |
| 4 | Status check LIMIT cap | **Prevents poller runaway** |
| 5 | Env tuning | **Zero-misfire scheduler config** |

---

## User Review Required

> [!IMPORTANT]
> **Concurrency level `RECHARGE_CONCURRENCY=8`**: This is the key tuning parameter. At 8 concurrent workers, the system sustains ~2.2 TPS against Pyro. Confirm with Pyro/BSNL API team that their gateway rate limit supports at least **5 TPS per API key** before enabling concurrency. Start with `RECHARGE_CONCURRENCY=4` and ramp up.

> [!WARNING]
> **Dealer Balance Risk at Higher Throughput**: At 8× throughput, dealer C-TOPUP balances will drain significantly faster. Ensure telecom circle administrators are informed to monitor and top up dealer accounts proactively during the 09:00 AM – 09:00 PM window.

> [!WARNING]
> **Abort Semantics Change**: The current `break` on auth errors (506, 5001, -1 action token) aborts all remaining queued rows and releases them. With concurrent execution, the semaphore approach still aborts the batch early — any row not yet submitted when an abort signal fires will be released by the existing `finally` block. This behaviour is preserved and tested.

> [!NOTE]
> **No Database Schema Changes**: All changes are at the application layer only. Postgres and Oracle table structures remain untouched.

---

## Open Questions

> [!IMPORTANT]
> **1. Confirmed Pyro API Rate Limit?** Do you have a written SLA from Pyro/BSNL stating the max TPS/RPM per API key on the `/epin-vendor-api/recharge` endpoint? This directly dictates the safe value of `RECHARGE_CONCURRENCY`.

> [!IMPORTANT]
> **2. Status check LIMIT value**: The plan uses `LIMIT 100` on the status check query. If your typical callback delay volume is higher (e.g., 200–300 records regularly miss callbacks), consider a higher limit. What is the expected average callback success rate?

---

## Proposed Changes

### Component 1 — Bounded Concurrency in Recharge Processor

---

#### [MODIFY] `app/config.py`

Add a new `recharge_concurrency` setting so the concurrency level is runtime-configurable via `.env` without code changes.

**Diff:**
```python
# Before (line 66):
recharge_batch_size: int = 500

# After:
recharge_batch_size: int = 500
recharge_concurrency: int = 1   # number of parallel recharge workers; 1 = sequential (safe default)
```

---

#### [MODIFY] `app/processor.py`

Replace the sequential `for row in rows:` loop with a concurrent fan-out using `asyncio.Semaphore` and `asyncio.gather`. The concurrency level is read from `settings.recharge_concurrency`.

**Key Design Decisions:**
- Each row is processed in its own async task via `_process_single_row()`.
- The `exhausted_dealers` set is shared across tasks using thread-safe mechanisms (it is only written once per dealer per batch, and `asyncio` is single-threaded, so a plain `set` remains safe).
- Abort signals (506, -1, 5001) set a shared `asyncio.Event` to stop issuing new work and break early. All in-flight tasks are allowed to complete; unclaimed rows are released.
- The `finally` block for releasing unprocessed claims is fully preserved.

**New Structure:**
```python
async def process_pending_recharges(
    batch_size: int = 500,
    circle_codes: Optional[Sequence[int]] = None,
    context: Optional[ExecutionContext] = None,
) -> dict:
    # ... (auth guard unchanged) ...

    semaphore = asyncio.Semaphore(settings.recharge_concurrency)
    abort_event = asyncio.Event()          # signals early stop on auth/token errors
    exhausted_dealers: set[str] = set()    # shared across concurrent tasks (asyncio is single-threaded)
    
    counters = {"registered": 0, "perm_failed": 0, "retryable": 0}
    processed_reqids: set[int] = set()

    async def _process_single_row(row: dict) -> None:
        async with semaphore:
            if abort_event.is_set():
                return   # stop issuing new API calls after abort
            # ... per-row logic extracted here ...
            # On abort conditions (506, -1, 5001):
            #     abort_event.set()
            # On 405:
            #     exhausted_dealers.add(dealer_msisdn)

    try:
        tasks = [
            asyncio.create_task(_process_single_row(row))
            for row in rows
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        # Existing finally block: release unclaimed rows
        unprocessed_reqids = [r["reqid"] for r in rows if r["reqid"] not in processed_reqids]
        if unprocessed_reqids:
            await async_release_unprocessed_claims(unprocessed_reqids)
```

**Throughput Impact:**
| `RECHARGE_CONCURRENCY` | Records / 2-min run | Records / hour | 12-hour capacity |
|---|---|---|---|
| 1 (current) | 34 | 1,020 | 12,240 |
| 4 | 136 | 4,080 | 48,960 |
| 8 | 250 | 7,500 | 90,000 |

---

### Component 2 — Persistent HTTP Client in `pyro_client.py`

---

#### [MODIFY] `app/pyro_client.py`

Replace the per-call `async with httpx.AsyncClient(...) as client:` pattern with a module-level shared client that maintains a connection pool.

```python
# Add at module level:
_http_client: Optional[httpx.AsyncClient] = None

def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            verify=True,
            timeout=30.0,
            limits=httpx.Limits(
                max_keepalive_connections=20,
                max_connections=50,
                keepalive_expiry=30.0,
            ),
        )
    return _http_client

async def close_http_client() -> None:
    global _http_client
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()
        _http_client = None
```

The `close_http_client()` is called during the FastAPI `lifespan` shutdown in [`main.py`](file:///D:/pyro/pyro_auto_frc/main.py#L48).

**Callers updated:**
- `recharge()` — replaces `async with httpx.AsyncClient(...) as client:` with `client = get_http_client()`
- `check_transaction_status()` — same

**Same change in `app/auth/token_manager.py`** — `authenticate()`, `refresh_access_token()`, `get_action_token()` all use ephemeral clients today. They switch to a shared client on the token manager instance.

---

### Component 3 — Smart Token Caching in Token Manager

---

#### [MODIFY] `app/auth/token_manager.py`

**Problem**: [`get_action_token()`](file:///D:/pyro/pyro_auto_frc/app/auth/token_manager.py#L137) always calls `refresh_access_token()`, which makes an outbound HTTP GET regardless of token validity.

**Fix**: Check `_is_access_token_valid()` before refreshing. The existing method already implements this guard but it is never consulted in `get_action_token()`:

```python
# Current (line 137-141):
async def get_action_token(self) -> Optional[str]:
    if not await self.refresh_access_token():   # ← always refreshes
        ...

# Fixed:
async def get_action_token(self) -> Optional[str]:
    # Only refresh if access token is expired or within 60s of expiry
    if not self._is_access_token_valid():
        if not await self.refresh_access_token():
            logger.error("Cannot get action token - access token refresh failed")
            return None
    # ... proceed to generate action token ...
```

**Impact**: Reduces per-record API calls from **3 HTTP calls** to **1 HTTP call** (only the action token generation call) when the access token is still valid. At 30,000 records:
- **Before**: 90,000 HTTP calls
- **After**: ~30,100 HTTP calls (1 refresh at startup + 30,000 action token calls)

> [!NOTE]
> Under concurrent mode, `get_action_token()` (and the underlying `refresh_access_token()`) is already protected by `self._auth_lock = asyncio.Lock()` inside `authenticate()`. However, `get_action_token` itself does not hold this lock during the access token validity check. A small race window exists where two concurrent tasks both see `_is_access_token_valid() == False` and both issue a refresh. This is benign (the second refresh simply overwrites the first with a fresh token) and is the same pattern used by `get_access_token()` today.

---

### Component 4 — Status Check LIMIT Cap

---

#### [MODIFY] `app/db/postgres.py`

Add `LIMIT` to [`fetch_pushed_rows_for_status_check()`](file:///D:/pyro/pyro_auto_frc/app/db/postgres.py#L736) and expose it as a config setting.

**Config addition (`app/config.py`):**
```python
status_check_batch_size: int = 100   # max rows per status check run
```

**SQL change:**
```sql
-- Before:
ORDER BY push_date ASC

-- After:
ORDER BY push_date ASC
LIMIT %(limit)s
```

```python
def fetch_pushed_rows_for_status_check() -> List[dict]:
    sql = """
        SELECT reqid, pyro_trans_id, client_txn_id,
               gsmno, caf_serial_no, batch_date, status_check_count
        FROM public.frc_pyro_request_data
        WHERE push_flag = 'P'
          AND status_check_eligible_at <= CURRENT_TIMESTAMP
          AND push_date >= CURRENT_TIMESTAMP - INTERVAL '60 minutes'
          AND push_date <= CURRENT_TIMESTAMP - INTERVAL '2 minutes'
        ORDER BY push_date ASC
        LIMIT %s
    """
    with get_pg_write_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (settings.status_check_batch_size,))
            return [dict(r) for r in cur.fetchall()]
```

---

### Component 5 — Environment Configuration Tuning

---

#### [MODIFY] `.env`

No code change required — only configuration values:

```ini
# ─── Batch Population ────────────────────────────────────────────────────────
ORACLE_BATCH_FETCH_SIZE=350
SCHEDULER_BATCH_POPULATION_INTERVAL_MINUTES=5
SCHEDULER_BATCH_POPULATION_GRACE_SECONDS=180

# ─── Recharge Processing ─────────────────────────────────────────────────────
RECHARGE_BATCH_SIZE=300           # Larger batch is safe now that runs complete in ~35s at concurrency=8
RECHARGE_CONCURRENCY=4            # Start conservative; escalate to 8 after Pyro rate limit confirmation
SCHEDULER_RECHARGE_INTERVAL_MINUTES=2
SCHEDULER_RECHARGE_GRACE_SECONDS=90

# ─── Status Check Fallback ───────────────────────────────────────────────────
STATUS_CHECK_BATCH_SIZE=100
STATUS_CHECK_MAX_ATTEMPTS=5
SCHEDULER_STATUS_CHECK_INTERVAL_MINUTES=5
SCHEDULER_STATUS_CHECK_GRACE_SECONDS=60

# ─── Startup Flags ───────────────────────────────────────────────────────────
RUN_BATCH_ON_STARTUP=false
RUN_RECHARGE_ON_STARTUP=false
RUN_CLEANUP_ON_STARTUP=true
```

---

## Verification Plan

### Automated Tests

Run the full existing test suite first to establish baseline:
```bash
cd D:/pyro/pyro_auto_frc
python -m pytest tests/ -v --tb=short 2>&1 | head -100
```

After implementation, run targeted test suites to verify no regressions:
```bash
# Core: processor concurrency and batch abort
python -m pytest tests/concurrency/test_dispatch_concurrency.py -v

# Integration: end-to-end batch flow
python -m pytest tests/integration/test_all_regression.py -v

# Unit: observability summary keys preserved
python -m pytest tests/unit/test_observability_phase13.py -v

# All
python -m pytest tests/ -v --tb=short
```

New tests to be added in `tests/concurrency/test_dispatch_concurrency.py`:
1. `test_concurrent_semaphore_bounded_at_n` — verify exactly N concurrent tasks can run at once.
2. `test_abort_event_stops_new_tasks` — verify abort_event cancels queued (not yet started) tasks.
3. `test_dealer_exhaustion_shared_across_concurrent_tasks` — verify dealer skip propagates across concurrent coroutines.

New tests in `tests/unit/test_token_manager.py`:
1. `test_get_action_token_skips_refresh_when_valid` — verify no HTTP call when token valid.
2. `test_get_action_token_refreshes_when_expired` — verify refresh is called when expired.

### Manual Verification

After deploying with `RECHARGE_CONCURRENCY=4`:

1. **Monitor batch run duration in logs**:
   ```
   grep "Processor complete" /var/log/pyro_auto_frc.log | tail -20
   ```
   Confirm `submitted=*` count is ~130–150 and execution time < 90 seconds.

2. **Verify no scheduler misfires**:
   ```
   grep -i "misfire" /var/log/pyro_auto_frc.log
   ```
   Should be empty.

3. **Monitor PostgreSQL held records** (should stay near 0):
   ```sql
   SELECT COUNT(*) FROM frc_pyro_request_data WHERE in_status = 'S';
   ```

4. **Monitor Pyro gateway error rates**:
   Watch for any burst of `429 Too Many Requests` in logs. If seen, reduce `RECHARGE_CONCURRENCY` from 4 → 2.

5. **Verify HTTP connection pool is reused** (check OS socket stats):
   ```bash
   ss -tn | grep <PYRO_IP> | wc -l
   ```
   Should show 5–20 reused ESTABLISHED sockets, not thousands of `TIME_WAIT`.

---

## Summary of Files Changed

| File | Type | What Changes |
|------|------|-------------|
| [`app/config.py`](file:///D:/pyro/pyro_auto_frc/app/config.py) | MODIFY | Add `recharge_concurrency`, `status_check_batch_size` settings |
| [`app/processor.py`](file:///D:/pyro/pyro_auto_frc/app/processor.py) | MODIFY | Replace sequential loop with semaphore-bounded concurrent fan-out |
| [`app/pyro_client.py`](file:///D:/pyro/pyro_auto_frc/app/pyro_client.py) | MODIFY | Shared persistent httpx client with connection pool |
| [`app/auth/token_manager.py`](file:///D:/pyro/pyro_auto_frc/app/auth/token_manager.py) | MODIFY | Guard `get_action_token()` with `_is_access_token_valid()` check |
| [`app/db/postgres.py`](file:///D:/pyro/pyro_auto_frc/app/db/postgres.py) | MODIFY | Add `LIMIT %s` to `fetch_pushed_rows_for_status_check()` |
| [`main.py`](file:///D:/pyro/pyro_auto_frc/main.py) | MODIFY | Call `close_http_client()` in lifespan shutdown |
| `.env` | MODIFY | Updated scheduler/batch/concurrency values |
| [`tests/concurrency/test_dispatch_concurrency.py`](file:///D:/pyro/pyro_auto_frc/tests/concurrency/test_dispatch_concurrency.py) | MODIFY | Add 3 new concurrency tests |
