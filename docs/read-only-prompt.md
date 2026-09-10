You are acting as a senior Python/FastAPI/PostgreSQL architect.

I need you to CREATE AN IMPLEMENTATION PLAN ONLY for a small infrastructure change in my existing `pyro_auto_frc` codebase.

Do NOT modify code.
Do NOT create commits.
Do NOT execute production mutations.
Do NOT perform real Pyro transactions.

## PostgreSQL topology

The production PostgreSQL topology is:

- `10.201.222.77:5432` = PostgreSQL PRIMARY / WRITE
- `10.201.222.77:5433` = PostgreSQL STANDBY / READ REPLICA

Database verification indicates that 5433 is in recovery and both endpoints share the same PostgreSQL system identifier, consistent with a primary/standby topology.

## Requirement

Inspect the entire existing `pyro_auto_frc` codebase and determine how PostgreSQL is currently accessed.

The intended rule is:

> Identify all PostgreSQL operations, but route `fetch_cos_bcd_for_gsms()` (Q022) to the 5433 read replica; keep all state-changing operations and transaction-dependent reads on 5432 unless code inspection proves they are safely replica-readable.

Do not interpret this as "move every SELECT to 5433."

The objective is specifically to isolate the existing Q022 enrichment read workload onto the PostgreSQL read replica while preserving correctness and existing transaction semantics.

---

# 1. FIRST: INSPECT THE CURRENT CODE

Inspect at minimum:

    app/config.py
    app/main.py
    app/db/postgres.py
    app/db/oracle.py
    app/batch/populator.py
    app/processor.py
    app/callback.py
    app/status_checker.py
    app/scheduler.py
    app/pyro_client.py

Also inspect:

    Dockerfile
    docker-compose*.yml
    environment/configuration files
    tests/

Search repository-wide for:

    get_pg_conn
    asyncpg
    psycopg
    psycopg2
    create_pool
    postgres
    SELECT
    INSERT
    UPDATE
    DELETE
    transaction
    advisory
    frc_pyro_request_data
    fetch_cos_bcd_for_gsms
    fetch_pending_rows

Do not assume the existing architecture from documentation alone. Determine the actual implementation.

---

# 2. CLASSIFY EVERY POSTGRESQL OPERATION

Create a table containing every relevant PostgreSQL operation:

| Function/File | Query/Operation | Read/Write | Transaction-dependent? | Current connection mechanism | Recommended DB |
|---|---|---|---|---|---|

Pay particular attention to:

- Q022 `fetch_cos_bcd_for_gsms()`
- Q023 staging INSERT
- Q024 pending/claim processing
- callback updates
- status-check updates
- audit/log writes
- advisory locks
- any read performed immediately before/after a write transaction

Do not classify purely by whether the SQL starts with SELECT.

A SELECT that participates in a transactional state change, locking operation, or read-after-write dependency should remain on 5432 unless there is strong evidence otherwise.

---

# 3. Q022 ANALYSIS

Inspect `fetch_cos_bcd_for_gsms()` carefully.

Determine:

- exact SQL executed
- all tables read
- whether it performs any mutation
- whether it depends on a transaction created elsewhere
- whether its results are immediately written back to PostgreSQL
- whether any read-after-write consistency assumption exists
- whether the function can safely use a replica
- whether replica lag could affect correctness

The expected conclusion is that Q022 is read-only enrichment and is therefore the primary candidate for 5433.

Do NOT assume this conclusion without confirming it from code.

---

# 4. Q023 / Q024 ANALYSIS

Inspect Q023 and Q024 separately.

### Q023

Determine exactly how the PostgreSQL staging INSERT works.

Confirm whether it must remain on 5432.

### Q024

Determine whether the current implementation is only a SELECT or whether there are associated state changes.

Also compare the current implementation with the existing zone-wise migration plan.

The final implementation plan must NOT accidentally move Q024 to 5433 if it becomes or already is a state-changing/locking operation.

If Q024 still requires the planned atomic:

    FOR UPDATE SKIP LOCKED

claiming behavior, explicitly state that such a claim belongs on 5432.

Do not implement this change now; only incorporate it in the plan if it is part of the already approved architecture.

---

# 5. CONNECTION-POOL DESIGN

Determine the minimum code changes required to support:

    PostgreSQL READ pool → 5433
    PostgreSQL WRITE pool → 5432

Assess the current connection abstraction.

Recommend whether the project should introduce explicit APIs such as:

    get_pg_read_conn()
    get_pg_write_conn()

or another equivalent design.

Do not redesign the application's database layer unnecessarily.

The plan should explain:

- configuration changes
- pool initialization
- pool shutdown
- connection acquisition
- transaction handling
- retry behavior
- failure behavior

The application must never silently fall back from the read replica to the primary or vice versa unless such behavior is explicitly justified and documented.

---

# 6. CONFIGURATION PLAN

Determine the minimum configuration required.

The preferred conceptual configuration is:

    PG_READ_HOST
    PG_READ_PORT=5433
    PG_READ_DATABASE
    PG_READ_USER
    PG_READ_PASSWORD

    PG_WRITE_HOST
    PG_WRITE_PORT=5432
    PG_WRITE_DATABASE
    PG_WRITE_USER
    PG_WRITE_PASSWORD

Do not assume exact variable names already exist.

First inspect the current configuration style and then recommend the smallest compatible change.

---

# 7. READ-REPLICA CONSISTENCY

This is important.

Inspect the code for any sequence like:

    write to 5432
       ↓
    read same data from PostgreSQL
       ↓
    continue workflow

Determine whether any such sequence is sensitive to replica lag.

For each case, classify:

- safe to read from 5433
- must remain on 5432
- needs special handling

Correctness takes precedence over read offloading.

---

# 8. HEALTH / OBSERVABILITY

Determine whether the existing health/startup logic can distinguish:

    PostgreSQL READ
    PostgreSQL WRITE

Recommend the minimum changes required to prove that:

    Q022 → 5433
    write operations → 5432

The plan should include safe verification such as:

    SELECT inet_server_port()
    SELECT pg_is_in_recovery()

where appropriate.

Do not expose credentials in logs.

---

# 9. DOCKER / DEPLOYMENT

Inspect the existing Docker and Compose configuration.

Determine the exact deployment changes required to provide both endpoints.

Do not hardcode database addresses into application source code.

The same image should remain promotable between environments through configuration/secrets.

---

# 10. SECURITY / READ-ONLY ENFORCEMENT

Determine whether the 5433 PostgreSQL role is actually read-only.

The plan should distinguish:

### Application-level separation

Q022 uses the READ connection.

### Database-level separation

The 5433 role should not have INSERT/UPDATE/DELETE privileges.

Do not modify production grants.

Instead, document any infrastructure-side verification or prerequisite.

---

# 11. PERFORMANCE ANALYSIS

Do NOT assume that moving Q022 to 5433 automatically improves performance.

Analyze:

- expected benefit from read/write workload isolation
- PostgreSQL replica behavior
- replica lag
- network latency
- connection-pool sizing
- Q022 workload characteristics
- whether current indexes support Q022 efficiently

Do not propose unrelated performance redesigns.

Do not add indexes simply because of this task.

---

# 12. TEST PLAN

Create an implementation-level test plan that proves:

### Configuration

- both read and write endpoints are configured separately

### Connection routing

- Q022 connects to 5433
- Q023 uses 5432
- Q024 uses 5432 where state-changing/locking
- callback writes use 5432
- status writes use 5432

### Actual endpoint verification

Where possible, tests should verify:

    SELECT inet_server_port()

rather than merely checking configuration variables.

### Read/write isolation

Verify that the read connection cannot perform mutations when the database role is properly restricted.

### Replica consistency

Test any identified read-after-write dependency.

### Failure handling

Test:

- 5433 unavailable
- 5432 unavailable
- replica lag scenario where relevant

Do not perform tests against production unless explicitly designed as non-mutating diagnostics.

---

# 13. SCOPE CONTROL

This task is NOT asking for:

- new business functionality
- new recharge logic
- changes to Pyro API behavior
- Oracle redesign
- schema redesign
- zone-wise architecture redesign
- Redis/Kafka/etc.
- unrelated performance optimization

The existing zone-wise migration architecture must remain intact.

Only recommend additional changes when code inspection proves they are necessary for safe PostgreSQL read/write separation.

---

# 14. REQUIRED OUTPUT: IMPLEMENTATION PLAN

Produce a comprehensive but focused implementation plan containing:

## A. Executive Summary

State whether the requirement can be implemented with a small change or requires broader refactoring.

## B. Current PostgreSQL Access Map

Provide the complete operation classification table.

## C. Target Architecture

Show:

    Q022 → PostgreSQL 5433 READ REPLICA

and:

    Q023
    Q024 claim/update
    callback updates
    status updates
    other writes
        ↓
    PostgreSQL 5432 PRIMARY

## D. Exact Files to Modify

For each file:

- path
- current responsibility
- required modification
- reason

## E. Detailed Step-by-Step Implementation

Specify the implementation order.

Example:

    Step 1: configuration
    Step 2: dual connection pools
    Step 3: Q022 routing
    Step 4: preserve/write-route all mutations
    Step 5: transaction review
    Step 6: health/diagnostics
    Step 7: Docker
    Step 8: tests

Adjust this order based on the actual code.

## F. Function-by-Function Changes

For every affected function specify:

- current behavior
- target behavior
- connection required
- transaction requirements
- compatibility concerns

## G. Configuration Changes

Show old → proposed configuration.

## H. Docker Changes

Show required environment/configuration changes.

## I. Testing Strategy

Provide unit, integration and endpoint-verification tests.

## J. Rollback Plan

Explain how to revert the code/configuration change safely.

## K. Risks

Explicitly identify:

- replica lag
- connection failure
- incorrect pool routing
- read-after-write inconsistency
- transaction/session issues
- accidental writes against the replica

## L. Acceptance Criteria

Provide objective criteria such as:

[ ] Q022 executes against 5433.

[ ] Q023 executes against 5432.

[ ] Q024 state-changing/locking operations execute against 5432.

[ ] No mutation is intentionally routed to 5433.

[ ] Transaction-dependent reads remain on 5432.

[ ] Existing Auto FRC workflow remains unchanged.

[ ] Zone-wise filtering remains unchanged.

[ ] Existing Oracle/Pyro behavior remains unchanged.

[ ] Tests verify actual server ports.

[ ] No production transaction is required to validate the implementation.

---

# 15. IMPORTANT: PLAN ONLY

Do not edit the repository.

Do not create files.

Do not commit changes.

Do not run production-mutating SQL.

Do not trigger Pyro.

Do not fabricate test results.

At the end, give me the implementation plan only, with enough precision that another coding agent can execute it safely.

Before finalizing, explicitly answer:

1. Is `fetch_cos_bcd_for_gsms()` the only existing PostgreSQL operation that should use 5433?
2. Which PostgreSQL operations must remain on 5432?
3. What is the minimum required code change?
4. What additional changes are required purely to support the separate connection pools?
5. Are there any replica-lag/read-after-write risks in the current Auto FRC flow?