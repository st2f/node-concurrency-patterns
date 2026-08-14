# Concurrency Patterns — Practice Plan

## Purpose

This project practices several ways to protect a shared rule when concurrent
Node.js operations modify the same data.

The rule in this project is:

> A locker can have at most one active checkout.

A naive checkout does this:

1. Read whether the locker is available.
2. If it is available, insert a checkout.

The race occurs when callers A and B both complete the read before either one
inserts. Both see an available locker, so both insert a checkout.

The project compares ways to prevent or detect that race in Node.js, Postgres,
and Redis. Every strategy is tested with real infrastructure rather than an
in-memory database fake.

A secondary goal is to practice the low-level APIs involved: `pg.Pool`, SQL
transactions, parameterized queries, and raw Redis commands without an ORM or
framework abstraction.

## Domain and infrastructure

- `checkout(userId, lockerId)` is the operation under test.
- Postgres stores lockers and their checkout history.
- Redis is used by the distributed-lock strategy.
- Docker Compose starts Postgres and Redis.
- SQL migrations create and evolve the Postgres schema.

## Step 1 — Establish the tests

### Baseline race

Use two Postgres connections and control the order explicitly:

```text
A reads "available"
B reads "available"
A inserts
B inserts
```

This test reliably demonstrates the original bug. It talks directly to
Postgres and deliberately bypasses `checkout()`, so it is not reused to test a
strategy.

### Shared behavior contract

Call a strategy's real `checkout()` function many times concurrently for the
same locker. A correct strategy should return exactly one successful checkout
and leave exactly one active checkout in Postgres.

This is useful shared coverage, but it is not enough by itself. Node.js may
happen to run the calls one after another, allowing a broken strategy to pass
by chance.

### Strategy-specific mechanism test

Control the point where callers contend so the test proves why a strategy
works. For example:

- Keep caller A inside a keyed mutex and prove caller B waits.
- Hold a Postgres advisory lock and prove another connection waits.
- Make two optimistic writers use the same version.
- Coordinate Redis lock holders with child-process messages.

### Process-topology test

Run two concurrent checkout attempts in two separate Node.js processes. This
reveals where each coordination mechanism lives:

- A keyed mutex coordinates only callers in one process, so two processes can
  both win.
- Postgres, Redis with stale-write protection, optimistic locking, and a
  database uniqueness rule must protect the invariant across processes.

## Step 2 — Implement one strategy at a time

For each strategy, complete its shared contract, mechanism test, and topology
test before starting the next strategy. Then add what was learned to
[`comparison.md`](comparison.md).

### App-level keyed mutex

Maintain a promise queue for each locker ID. A caller waits for the previous
promise before reading and writing, then releases its promise for the next
caller.

The queue is ordinary JavaScript memory. It works when every caller uses the
same checkout instance in one process. Two serverless invocations or container
replicas have different queues, so the mutex cannot coordinate between them.

### Postgres advisory lock

Acquire `pg_advisory_xact_lock(...)` inside the transaction before checking the
locker. Every process using the same lock key asks Postgres for the same lock,
so Postgres serializes them.

The transaction-scoped lock is automatically released on commit or rollback.
All code paths that modify this state must follow the locking convention.

### Optimistic locking with a version column

Optimistic locking does not make the second caller wait before reading.
Instead, it detects at write time that another caller has changed the data.

Suppose callers A and B both read version `7`. Each tries a conditional update:

```sql
UPDATE lockers
SET version = version + 1
WHERE id = $1 AND version = $2;
```

Here, `$2` is the version the caller read: `7` in this example.

Postgres performs each update atomically:

- A updates the row, changing the version to `8`.
- B's condition no longer matches, so B updates zero rows.

The affected-row count (`result.rowCount` in `pg`) tells the application who
won. The winner inserts the checkout in the same transaction. The loser reads
the current state again and, for this domain, normally returns `unavailable`.

This conditional update is often called **compare-and-swap**:

- **Compare:** is the current version still the version I read?
- **Swap:** if it is, apply my change and advance the version.

### Database uniqueness rule

Let Postgres enforce the invariant directly. Because the `checkouts` table
keeps historical rows, `UNIQUE (locker_id)` would be too strict: it would
prevent a locker from ever being checked out again.

Use a partial unique index that applies only to active rows:

```sql
CREATE UNIQUE INDEX one_active_checkout_per_locker
ON checkouts (locker_id)
WHERE released_at IS NULL;
```

If two processes insert concurrently, Postgres accepts one insert and rejects
the other. The application converts that expected conflict into an
`unavailable` result. This strategy is not a locking technique; it expresses
the business rule in the database.

### Redis distributed lock and fencing

First, implement a Redis lease:

```text
SET lock:<key> <owner-token> NX PX <ttl>
```

`NX` acquires the key only when it does not already exist. `PX` gives the lock
a time-to-live so a crashed owner cannot hold it forever. Release the lock with
an atomic Lua script that deletes the key only when its value still equals the
caller's owner token.

The owner-token check makes release safe, but it does not make an expired owner
safe. Consider this sequence:

```text
A acquires the lock
A pauses until its lease expires
B acquires the lock and writes
A resumes and tries to write
```

A must not be allowed to write after B. Checking the token during release only
stops A from deleting B's lock; it does not stop A's database write.

The owner token above is a random identity: it answers "is this still my
lock?" A fencing token is different. It is an increasing number that records
the order in which owners acquired the lock.

Postgres remembers the highest fencing token it has accepted and rejects a
write carrying an older token. A resumed owner can therefore be recognized as
stale even after its Redis lease expires.

Fencing and optimistic locking both use conditional database writes, but their
values mean different things:

- An optimistic-lock version is read from the database row. The write succeeds
  only if that row still has the same version.
- A fencing token represents the order in which owners acquired the lock. The
  database rejects an owner older than one it has already accepted.

Using `UPDATE ... WHERE version = $expected` alongside Redis can still be a
valid design, but it combines the Redis lease with optimistic locking. It is
not, by itself, a fencing-token implementation.

## Test-harness decisions

- **Database isolation:** Give each test a separate schema. In particular, the
  partial unique index must not change the behavior of other strategies.
- **Test-runner concurrency:** Run integration-test files serially or isolate
  their database state. One cleanup transaction cannot include work performed
  by other connections or processes.
- **Client ownership:** The process that creates a `pg.Pool` or Redis connection
  must close it with `pool.end()` or `redis.quit()`/`disconnect()`.
- **Deterministic cleanup:** Release barriers and close clients in `finally`
  blocks so a failed assertion does not leave a test or child process hanging.

## Related documents

- [Strategy comparison](comparison.md)
- [Test overview](tests-overview.md)
- [Incremental learning steps](learning-steps.md)
