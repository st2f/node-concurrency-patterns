# Concurrency Patterns — Practice Plan

## Why

Practicing the alternatives for closing a classic concurrency race: an
invariant like "two callers can't claim the same resource at the same time"
gets violated when a naive `find` → `mutate` → `save` sequence lets two
concurrent callers interleave and both succeed. This repo works through the
options for closing that race — DB-level locking, app-level locking,
distributed locking, optimistic concurrency, and a DB constraint as a
safety net — each proven against a real concurrent-connections test, not an
in-memory fake.

Secondary goal: hands-on practice with raw Postgres (no ORM, no query
builder) and raw Redis (no framework facade around it) — both common
building blocks for this kind of coordination problem in serverless /
container-based backends.

## Domain under test

A minimal `Locker` domain: a `checkout(user, locker)` operation on an
aggregate, backed by Postgres, with the invariant "no two users can hold
the same locker at the same time."

## Setup

- Raw `pg` (node-postgres) client — no ORM, no query builder.
- `docker-compose.yml` with real Postgres **and** Redis (both common in
  serverless/container backends).
- Plain SQL migration files (or `node-pg-migrate`).
- `pg.Pool`, explicit `BEGIN`/`COMMIT`, parameterized queries.

## Step 1 — Prove the race

Must run against real Postgres in a container, with genuinely concurrent
connections/processes — not an in-memory fake.

1a. **Deterministic interleaving (baseline demonstration only).** Drive two
raw `pg` client connections directly from the test, step by step: issue
`find` on connection A, await it; issue `find` on connection B, await
it; _then_ issue `mutate`+`save` on A, await; then on B, await. Both
reads observe "available" before either write happens, so the bug
reproduces on every run — no timing luck involved, no synchronization
hook needed inside application code. This talks to Postgres directly
and bypasses whatever `checkout()` API a strategy wraps around it, so
it can only ever demonstrate the _unprotected_ baseline — a correctly
serialized strategy has nothing to "pass" here, since its guard code
never runs in this test. Scope it to Step 1 only; it is not reused in
Step 2.

1b. **Shared concurrent-load contract.** Fire N concurrent "checkout the
same locker" calls **through each strategy's real `checkout()`
function** and inspect both the returned results and the final database
state. In the single-process suite, every protected strategy must yield
exactly one winner and one active checkout. This exercises the real
guard code, but natural scheduling can accidentally serialize a broken
implementation, so it is a useful shared contract/load test rather
than sufficient proof by itself.

1c. **Deterministic, strategy-specific tests.** Add controlled contention
at the boundary relevant to each mechanism rather than forcing every
implementation through the same interleaving. Examples: hold an
advisory lock on one connection while another waits; keep the first
mutex callback open while a second queues; coordinate Redis holders by
worker-process messages; and make two optimistic writers attempt the
same expected version. These tests prove that the intended mechanism —
not merely a favorable scheduler — produced the result.

1d. **Process-topology tests.** Run the same checkout attempt from two Node
processes. The expected result is part of the lesson: the app-level
mutex must pass within one process but is expected to fail across two,
while the Postgres, Redis-with-storage-validation, optimistic-locking,
and unique-constraint strategies must preserve the invariant across
processes.

## Step 2 — Implement and compare strategies

One module per strategy, each implementing the same `checkout()` shape and
run against the applicable shared, deterministic, and process-topology
tests from Step 1:

1. **Postgres advisory lock** — `pg_advisory_xact_lock(hashtext(lockerKey))`
   inside the transaction wrapping find + checkout + save. Explicit,
   code-level (not implicit like `SELECT ... FOR UPDATE`), scoped to the
   transaction so it auto-releases on commit/rollback.

2. **App-level keyed mutex/queue** — in-memory, per aggregate id (hand-rolled
   promise queue). Demonstrates the limitation directly: only serializes
   within a single process, so it does **not** protect against races across
   multiple serverless invocations / container replicas.

3. **Redis distributed lock** — raw `ioredis` (not a framework facade):
   `SET lock:<key> <token> NX PX <ttl>` to acquire, release only if the
   stored value still matches the token (via a Lua script through `EVAL`,
   to avoid releasing a lock that expired and was re-acquired by someone
   else). Prove it works across **two separate Node processes** hitting the
   same Redis + Postgres — something the in-memory mutex can't do.

   Token-checked release only stops one owner deleting _another_ owner's
   lock — it does **not** stop a stalled/GC-paused owner from resuming and
   writing _after_ its lock already expired and a new owner acquired it
   (the classic Redlock-safety objection: Kleppmann vs. antirez).
   Demonstrate this failure directly: pause a holder past its TTL, let a
   second holder acquire and proceed, then let the first resume its write.

   Then demonstrate storage-layer validation. Prefer a genuine fencing
   token for this strategy: allocate a monotonically increasing token when
   acquiring the lock, persist the last accepted token with the locker,
   and have Postgres reject writes carrying an older token. Contrast this
   with `UPDATE ... WHERE version = $expected`, which also protects the
   invariant but is optimistic compare-and-swap — effectively strategy 4
   combined with the Redis lock, not fencing itself. This distinction is
   one of the most valuable lessons in the repo: a lock lease alone does
   not guarantee mutual exclusion after expiry unless the storage layer
   rejects stale writers.

4. **Optimistic locking with a version column** — both processes read
   freely, conflict is only detected at write time, loser retries.

5. **Unique constraint** — the DB-level safety net, independent of
   application code correctness. A bare `UNIQUE (locker_id)` only works if
   checkouts are never kept as history; with a `checkouts` table that
   retains past rows, the real mechanism is a **partial unique index**,
   e.g. `CREATE UNIQUE INDEX ... ON checkouts (locker_id) WHERE
released_at IS NULL`. Worth calling out explicitly: this is the one
   strategy where the fix isn't a locking _technique_ at all — it's
   expressing the invariant directly as a constraint the database enforces
   unconditionally.

## Step 3 — Write up the comparison

Short doc/table: mechanism, where it lives (DB vs. app vs. cache),
correctness across multiple processes (yes/no), failure modes (lock
expiry mid-operation, deadlocks, retry cost under contention), and when
each is the right default for a serverless/container + Postgres + Redis
stack.

## Test-harness decisions (make with Step 1b)

These are deliberately deferred until the strategy interface and shared
test suite exist, but must be resolved before relying on the comparison:

- **Database isolation:** choose a clean schema/database per strategy or a
  deterministic reset fixture. In particular, applying the partial unique
  index for strategy 5 must not silently change the behavior attributed to
  strategies 1–4. Because Docker volumes live outside Git, changing a
  branch or module does not reset database state.
- **Test-runner concurrency:** either serialize integration-test files or
  give each test worker isolated database state. Cleanup performed inside
  one transaction cannot cover the separate connections/processes used by
  these tests.
- **Client ownership:** make every process that creates a `pg.Pool` or
  Redis connection responsible for closing it (`pool.end()` and
  `redis.quit()`/`disconnect()`). Prefer fixtures or factories when the
  shared suite makes the required lifetime clear.
- **No false-green run:** once the first test is introduced, ensure the
  integration command cannot report success merely because it discovered
  zero tests.

## Deferred / another time

- **PGlite** for fast tests without Docker. Not used here because it's
  single-process / single active backend (queues concurrent calls rather
  than truly running them concurrently, no listener for a second OS process
  to connect to by default) — which undermines the exact thing this repo
  is trying to prove. Worth evaluating separately for TDD-speed unit tests
  once this repo's concurrency work is done.
