 # PostgreSQL Read-Replica Routing Implementation Plan

  ## Summary

  The change is small and localized to the PostgreSQL connection layer plus configuration, diagnostics, and routing tests.

  fetch_cos_bcd_for_gsms() is the only existing PostgreSQL operation that should use 5433. All other PostgreSQL operations must remain on the primary at 5432 because they mutate state, use locking,
  participate in state transitions, or require read-after-write consistency.

  The repository has main.py as the FastAPI entrypoint; app/main.py does not exist.

  ## Current PostgreSQL Access Map

   Function/File       Operation                                              Classification                                                         Current mechanism                Target DB
  ━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━
   population_advis    pg_try_advisory_lock, pg_advisory_unlock               Session/stateful lock                                                  Shared ThreadedConnectionPool    5432
   ory_lock() /
   app/db/
   postgres.py
  ──────────────────  ─────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────  ───────────────────────────────  ──────────────────────────
   fetch_cos_bcd_fo    SELECT/UNION ALL over cos_bcd, cos_bcd_dkyc,           Read-only enrichment                                                   get_pg_conn()                    5433
   r_gsms() / app/     ctop_master, frc_plan_table
   db/postgres.py
  ──────────────────  ─────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────  ───────────────────────────────  ──────────────────────────
   bulk_insert_frc_    INSERT ... ON CONFLICT ... RETURNING into              Write; Q023 staging                                                    get_pg_conn()                    5432
   requests()          frc_pyro_request_data
  ──────────────────  ─────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────  ───────────────────────────────  ──────────────────────────
   mark_requests_di    UPDATE in_status='C'                                   Write following Oracle claim                                           get_pg_conn()                    5432
   spatchable()
  ──────────────────  ─────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────  ───────────────────────────────  ──────────────────────────
   mark_requests_st    UPDATE in_status='F'                                   Write/failure recovery                                                 get_pg_conn()                    5432
   aging_failed()
  ──────────────────  ─────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────  ───────────────────────────────  ──────────────────────────
   fetch_staged_unc    SELECT in_status='S'                                   Reconciliation read; immediately followed by state decisions/writes    get_pg_conn()                    5432
   onfirmed_request
   s()
  ──────────────────  ─────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────  ───────────────────────────────  ──────────────────────────
   fetch_pending_ro    UPDATE ... RETURNING with FOR UPDATE SKIP LOCKED       Atomic Q024 claim and write                                            get_pg_conn()                    5432
   ws()
  ──────────────────  ─────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────  ───────────────────────────────  ──────────────────────────
   release_unproces    UPDATE push_flag                                       Claim recovery write                                                   get_pg_conn()                    5432
   sed_claims()
  ──────────────────  ─────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────  ───────────────────────────────  ──────────────────────────
   mark_as_pushed()    UPDATE Pyro submission state                           Write                                                                  get_pg_conn()                    5432
  ──────────────────  ─────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────  ───────────────────────────────  ──────────────────────────
   mark_as_success(    UPDATE terminal success state                          Write                                                                  get_pg_conn()                    5432
   )
  ──────────────────  ─────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────  ───────────────────────────────  ──────────────────────────
   mark_as_failed()    UPDATE failure/retry state                             Write                                                                  get_pg_conn()                    5432
  ──────────────────  ─────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────  ───────────────────────────────  ──────────────────────────
   fetch_pushed_row    SELECT eligible in-flight rows                         Read immediately followed by attempt/state updates                     get_pg_conn()                    5432
   s_for_status_che
   ck()
  ──────────────────  ─────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────  ───────────────────────────────  ──────────────────────────
   update_status_ch    UPDATE polling counter                                 Write                                                                  get_pg_conn()                    5432
   eck_attempt()
  ──────────────────  ─────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────  ───────────────────────────────  ──────────────────────────
   find_row_by_pyro    SELECT callback lookup                                 Read-after-write/idempotency-sensitive                                 get_pg_conn()                    5432
   _trans_id()
  ──────────────────  ─────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────  ───────────────────────────────  ──────────────────────────
   insert_txn_log()    INSERT into frc_txn_log                                Audit write                                                            get_pg_conn()                    5432
  ──────────────────  ─────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────  ───────────────────────────────  ──────────────────────────
   Async PostgreSQL    Delegate to the functions above                        Inherit classification                                                 asyncio.to_thread                Same as wrapped function
   wrappers

  Q024 is already implemented as an atomic UPDATE ... RETURNING with FOR UPDATE SKIP LOCKED; it must remain entirely on 5432.

  ## Target Architecture

  Q022 fetch_cos_bcd_for_gsms()
          │
          ▼
  PostgreSQL READ pool → 10.201.222.77:5433
                           pg_is_in_recovery() = true

  Q023 staging INSERT
  Q024 claim/update and FOR UPDATE SKIP LOCKED
  callback lookups and updates
  status-check reads and updates
  state transitions
  audit/log writes
  advisory locks
          │
          ▼
  PostgreSQL WRITE pool → 10.201.222.77:5432
                           pg_is_in_recovery() = false

  There must be no automatic read-to-write or write-to-read fallback. A failed replica read should fail the Q022 population attempt and be retried against the read pool only.

  ## Files to Modify

  ### app/config.py

  Current responsibility: loads a single PostgreSQL host, port, database, user, password, and pool sizes.

  Required change:

  - Add explicit read and write PostgreSQL settings.
  - Use PG_WRITE_* and PG_READ_* as canonical variables.
  - Preserve compatibility with existing PG_HOST, PG_PORT, PG_DATABASE, PG_USER, and PG_PASSWORD as legacy aliases for the write endpoint during migration.
  - Require an explicit read endpoint or fail configuration validation; do not silently infer the read replica.
  - Keep existing pool-size defaults unless separate read sizing is needed.
  - Add optional read-pool sizing only if required by observed Q022 concurrency; otherwise reuse the existing min/max values.

  ### app/db/postgres.py

  Current responsibility: owns one ThreadedConnectionPool, connection lifecycle, retries, all PostgreSQL functions, and async wrappers.

  Required change:

  - Replace _pool with distinct read and write pools.
  - Add explicit context managers:
      - get_pg_read_conn()
      - get_pg_write_conn()

  - Preserve the existing commit/rollback/discard behavior independently for both pools.
  - Initialize both pools in init_pg_pool().
  - Close both pools in close_pg_pool().
  - Route only fetch_cos_bcd_for_gsms() through get_pg_read_conn().
  - Route every other PostgreSQL function through get_pg_write_conn().
  - Keep _pg_retry pool-local: retry against the same endpoint only.
  - Ensure the advisory-lock context keeps its write-pool connection checked out until unlock completes.
  - Avoid introducing a generic “choose primary if replica fails” abstraction.

  The old get_pg_conn() should either be removed and all repository callers/tests updated, or retained only as a clearly documented deprecated alias to get_pg_write_conn() during compatibility transition. No
  production function should rely on the ambiguous name.

  ### main.py

  Current responsibility: FastAPI lifespan and operational endpoints.

  Required change:

  - Continue initializing and closing PostgreSQL pools through the existing lifespan hooks.
  - Add safe database diagnostics capable of checking both pools independently.
  - Use non-mutating checks such as:
      - SELECT inet_server_port()
      - SELECT pg_is_in_recovery()

  - Do not log connection strings, usernames with secrets, or passwords.
  - Keep the existing lightweight Docker /health endpoint compatible.
  - Add a separate protected or operational database-health endpoint if exposing endpoint details publicly is undesirable.

  ### app/batch/populator.py

  Current responsibility: Oracle candidate discovery, Q022 enrichment, Q023 staging, Oracle Q020 writeback, and dispatchability transition.

  Required change:

  - No business-flow redesign.
  - fetch_cos_bcd_for_gsms() automatically uses the read pool.
  - Keep bulk_insert_frc_requests() on the write pool.
  - Preserve the existing sequence:
      1. Oracle candidate read
      2. Q022 PostgreSQL enrichment read
      3. Q023 PostgreSQL staging insert
      4. Oracle Q020 writeback
      5. PostgreSQL dispatchability update

  - Preserve zone filtering, identity checks, MPIN encryption, and failure handling.

  ### app/batch/reconciler.py

  Current responsibility: recover staged PostgreSQL rows whose Oracle claim confirmation was interrupted.

  Required change:

  - Keep staged-row reads and all subsequent PostgreSQL updates on 5432.
  - Do not move fetch_staged_unconfirmed_requests() to the replica because its results drive reconciliation and immediate state transitions.

  ### app/processor.py

  Current responsibility: Q024 claim, Pyro recharge processing, PostgreSQL state updates, and Oracle status updates.

  Required change:

  - Keep Q024 and all state mutations on 5432.
  - Preserve the existing async wrappers and abort/release behavior.
  - Do not treat Q024 as a replica-safe SELECT; it is an atomic claim/update operation.

  ### app/callback.py

  Current responsibility: callback lookup, validation, audit logging, PostgreSQL terminal updates, and Oracle updates.

  Required change:

  - Keep find_row_by_pyro_trans_id() on 5432 because callback processing depends on current committed state.
  - Keep audit inserts and terminal updates on 5432.

  ### app/status_checker.py

  Current responsibility: identify eligible in-flight rows, increment polling attempts, call Pyro, and update PostgreSQL/Oracle state.

  Required change:

  - Keep fetch_pushed_rows_for_status_check() on 5432.
  - Keep attempt counters and terminal state updates on 5432.
  - Preserve current ordering and retry semantics.

  ### Dockerfile

  No source-code change is required.

  The same image remains deployable across environments. Runtime configuration must supply both PostgreSQL endpoints through environment variables or secrets.

  ### docker-compose.yml and docker-compose-prod.yml

  Required change:

  - Document or inject the separate PG_READ_* and PG_WRITE_* variables.
  - Do not hardcode database addresses into application code.
  - Keep .env/secret-based deployment behavior.
  - Ensure production and non-production Compose files provide both endpoints.
  - Keep the existing healthcheck compatible with the application’s startup behavior.

  ### Tests

  Update existing PostgreSQL tests that patch get_pg_conn() to patch the explicit read or write connection context manager as appropriate.

  Add routing and endpoint-verification tests without using production.

  ## Function-by-Function Routing Changes

  ### Read replica

  fetch_cos_bcd_for_gsms()

  - Current: read-only Q022 query through the single pool.
  - Target: read pool at 5433.
  - Transaction: one read-only session; no mutation.
  - Consistency: replica lag may cause stale or missing enrichment data.
  - Failure: retry only against the read pool; surface failure to batch population. No primary fallback.

  ### Primary-only operations

  The following must use get_pg_write_conn():

  - population_advisory_lock()
  - bulk_insert_frc_requests()
  - mark_requests_dispatchable()
  - mark_requests_staging_failed()
  - fetch_staged_unconfirmed_requests()
  - fetch_pending_rows()
  - release_unprocessed_claims()
  - mark_as_pushed()
  - mark_as_success()
  - mark_as_failed()
  - fetch_pushed_rows_for_status_check()
  - update_status_check_attempt()
  - find_row_by_pyro_trans_id()
  - insert_txn_log()

  Q024 must retain its current atomic transaction semantics. FOR UPDATE SKIP LOCKED cannot execute against the standby.

  ## Configuration

  Current configuration:

  PG_HOST
  PG_PORT=5432
  PG_DATABASE
  PG_USER
  PG_PASSWORD
  PG_MIN_CONN=2
  PG_MAX_CONN=10

  Proposed canonical configuration:

  PG_READ_HOST=10.201.222.77
  PG_READ_PORT=5433
  PG_READ_DATABASE=...
  PG_READ_USER=...
  PG_READ_PASSWORD=...

  PG_WRITE_HOST=10.201.222.77
  PG_WRITE_PORT=5432
  PG_WRITE_DATABASE=...
  PG_WRITE_USER=...
  PG_WRITE_PASSWORD=...

  PG_MIN_CONN=2
  PG_MAX_CONN=10

  Legacy PG_* variables may temporarily populate the write settings only. Read credentials must be independently configurable so the replica can use a restricted role.

  Recommended database prerequisite:

  - The read role should have SELECT privileges only on the Q022 source tables.
  - It should not have INSERT, UPDATE, or DELETE privileges.
  - Production grants must be verified by infrastructure/database administrators, not changed by this application task.

  ## Implementation Order

  1. Add and validate separate read/write settings while preserving legacy write configuration compatibility.
  2. Implement two PostgreSQL pools with independent lifecycle and connection context managers.
  3. Move Q022 to the read connection context.
  4. Move every remaining PostgreSQL operation explicitly to the write context.
  5. Preserve _pg_retry behavior without cross-endpoint fallback.
  6. Add independent read/write diagnostics using inet_server_port() and pg_is_in_recovery().
  7. Update Compose/runtime configuration documentation and secret injection.
  8. Update unit tests for explicit connection routing.
  9. Add non-production integration tests against a primary/standby-like environment.
  10. Verify deployment startup, shutdown, health reporting, and rollback behavior.

  ## Replica-Lag Analysis

  The current Auto FRC flow contains no PostgreSQL write immediately followed by a Q022 read of the same rows:

  Oracle candidate read
      ↓
  Q022 read from cos_bcd/cos_bcd_dkyc
      ↓
  Q023 PostgreSQL staging INSERT

  Therefore Q022 is structurally safe to route to the replica from a transaction-ordering perspective.

  However, replica lag can cause:

  - recently committed cos_bcd or cos_bcd_dkyc data to be absent;
  - recently changed plan/vendor/MPIN data to be stale;
  - fewer Q022 matches and therefore fewer staged requests.

  The safe policy is:

  - do not silently fall back to 5432;
  - record read-pool failure or replica-health failure;
  - fail or defer the affected population run;
  - monitor replica lag externally;
  - optionally verify replica recovery state during diagnostics.

  All flows involving frc_pyro_request_data remain on 5432, so callback, claim, status, and retry state cannot observe stale replica data.

  ## Health and Observability

  Add independent diagnostics that report, without credentials:

  read:
    configured endpoint identity
    inet_server_port()
    pg_is_in_recovery()
    connectivity status

  write:
    configured endpoint identity
    inet_server_port()
    pg_is_in_recovery()
    connectivity status

  Expected validation:

  Q022 connection:
    inet_server_port() = 5433
    pg_is_in_recovery() = true

  Write connection:
    inet_server_port() = 5432
    pg_is_in_recovery() = false

  Diagnostics must not execute writes, advisory locks, or production transactions.

  ## Testing Strategy

  ### Unit tests

  - Settings accept canonical read/write variables.
  - Legacy write variables remain compatible.
  - Missing read configuration fails closed.
  - fetch_cos_bcd_for_gsms() uses the read connection context.
  - Q023 uses the write connection context.
  - Q024 uses the write connection context and retains FOR UPDATE SKIP LOCKED.
  - Advisory lock uses the write pool.
  - Callback lookup, callback updates, status polling, and audit inserts use the write pool.
  - Read-pool operational errors retry on the read pool only.
  - No test permits automatic fallback to the write pool.

  ### Endpoint verification tests

  Use mocked or dedicated non-production PostgreSQL endpoints to execute:

  SELECT inet_server_port();
  SELECT pg_is_in_recovery();

  Assert:

  - Q022 observes port 5433.
  - Q023 observes port 5432.
  - Q024 observes port 5432.
  - All state-changing operations observe port 5432.

  ### Integration tests

  - Q022 returns enrichment data from a standby-like database.
  - Q023 inserts only through the primary.
  - Q024 claims rows atomically through the primary.
  - Callback lookup sees the current primary state.
  - Status-check selection and updates remain consistent.
  - A read-only database role cannot mutate Q022 source data.
  - Replica-unavailable behavior fails the Q022 population attempt without using the primary.
  - Primary-unavailable behavior prevents all state-changing workflows.
  - Replica lag or stale Q022 data is detected and handled as a deferred/failed enrichment run.
  - Existing zone-wise filtering and Auto FRC workflow remain unchanged.

  No production-mutating SQL, Pyro request, or real recharge transaction is required.

  ## Rollback Plan

  1. Restore the previous application image.
  2. Restore the previous single-endpoint PG_* configuration.
  3. Restart the service.
  4. Confirm the original primary connection and health behavior.
  5. Leave database grants unchanged; application rollback does not require production schema or privilege rollback.

  If configuration is rolled back before the application image, ensure the old image does not receive only the new variable names.

  ## Risks

  - Replica lag causes stale or missing Q022 enrichment.
  - Replica outage stops batch population unless the replica is restored.
  - Incorrect routing could send writes to the standby or Q022 reads to the primary.
  - Ambiguous legacy connection APIs could hide routing errors.
  - Session-level advisory locks must remain tied to one primary connection.
  - Q024 cannot run on a standby because it updates rows and locks candidates.
  - Callback and status workflows could become inconsistent if moved to the replica.
  - A read role with write privileges weakens database-level enforcement.
  - Pool shutdown must close both pools cleanly.
  - Independent pool sizing may increase total PostgreSQL connections.

  ## Acceptance Criteria

  - [ ] fetch_cos_bcd_for_gsms() is the only existing PostgreSQL operation routed to 5433.
  - [ ] Q022 executes through the read pool.
  - [ ] Q023 executes through the write pool at 5432.
  - [ ] Q024 claim/update and FOR UPDATE SKIP LOCKED execute through 5432.
  - [ ] Advisory locks execute through 5432.
  - [ ] Callback lookups and updates execute through 5432.
  - [ ] Status-check reads and updates execute through 5432.
  - [ ] Audit/log inserts execute through 5432.
  - [ ] Transaction-dependent reads remain on 5432.
  - [ ] No mutation is intentionally routed to 5433.
  - [ ] No automatic cross-endpoint fallback exists.
  - [ ] Read and write pools initialize and close independently.
  - [ ] Diagnostics verify actual server ports and recovery state.
  - [ ] Read credentials can be restricted independently.
  - [ ] Existing Auto FRC behavior and zone-wise filtering remain unchanged.
  - [ ] Tests verify actual endpoint identity where possible.
  - [ ] No production transaction or Pyro request is required for validation.

  ## Explicit Answers

  1. Is fetch_cos_bcd_for_gsms() the only existing PostgreSQL operation that should use 5433?
     Yes. It is the only read-only, non-locking, non-state-dependent enrichment query.

  2. Which operations must remain on 5432?
     Q023 staging, Q024 claiming, advisory locks, staged-row reconciliation, dispatchability/failure transitions, release operations, Pyro submission state updates, success/failure updates, status-check
     selection and counters, callback lookup, audit logging, and every other PostgreSQL operation currently present.

  3. What is the minimum required code change?
     Introduce explicit read/write connection contexts and change fetch_cos_bcd_for_gsms() to use the read context while all other functions use the write context.

  4. What additional changes are required purely to support separate pools?
     Configuration fields and environment variables, dual pool initialization and shutdown, endpoint-specific retry behavior, explicit routing APIs, Compose secret/configuration updates, health diagnostics,
     and routing tests.

  5. Are there replica-lag/read-after-write risks in the current Auto FRC flow?
     There is no direct PostgreSQL write-before-Q022 dependency. Q022 occurs before Q023 staging. Replica lag can nevertheless produce stale or missing enrichment data, so Q022 must not silently fall back to
     the primary; lag and replica health must be observable and the affected batch must be deferred or failed safely.