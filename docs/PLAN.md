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

1a. **Deterministic interleaving (the real regression test).** Drive two
    raw `pg` client connections directly from the test, step by step: issue
    `find` on connection A, await it; issue `find` on connection B, await
    it; *then* issue `mutate`+`save` on A, await; then on B, await. Both
    reads observe "available" before either write happens, so the bug
    reproduces on every run — no timing luck involved, no synchronization
    hook needed inside application code. This is the test every strategy
    below must pass.

1b. **Stress variant (secondary).** Fire N naturally concurrent "checkout
    the same locker" calls with no protection and assert the bug happens
    (more than one succeeds). Closer to real production load, but on its
    own would be a flaky regression test — keep it as a sanity check
    alongside 1a, not a replacement for it.

## Step 2 — Implement and compare strategies

One branch or module per strategy, each re-run against the Step 1 test:

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
   same Redis + Postgres — something the in-memory mutex can't do. Note as
   a talking point: single-instance `SET NX PX` is fine for most cases;
   the multi-node "Redlock" algorithm is genuinely debated (Kleppmann vs.
   antirez).

4. **Optimistic locking with a version column** — both processes read
   freely, conflict is only detected at write time, loser retries.

5. **Unique constraint** — the DB-level safety net, independent of
   application code correctness. A bare `UNIQUE (locker_id)` only works if
   checkouts are never kept as history; with a `checkouts` table that
   retains past rows, the real mechanism is a **partial unique index**,
   e.g. `CREATE UNIQUE INDEX ... ON checkouts (locker_id) WHERE
   released_at IS NULL`. Worth calling out explicitly: this is the one
   strategy where the fix isn't a locking *technique* at all — it's
   expressing the invariant directly as a constraint the database enforces
   unconditionally.

## Step 3 — Write up the comparison

Short doc/table: mechanism, where it lives (DB vs. app vs. cache),
correctness across multiple processes (yes/no), failure modes (lock
expiry mid-operation, deadlocks, retry cost under contention), and when
each is the right default for a serverless/container + Postgres + Redis
stack.

## Deferred / another time

- **PGlite** for fast tests without Docker. Not used here because it's
  single-process / single active backend (queues concurrent calls rather
  than truly running them concurrently, no listener for a second OS process
  to connect to by default) — which undermines the exact thing this repo
  is trying to prove. Worth evaluating separately for TDD-speed unit tests
  once this repo's concurrency work is done.
