# Strategy comparison

This page grows one strategy at a time. Entries distinguish behavior already
covered by tests from claims whose mechanism or process-topology tests are
still pending.

## At a glance

| Strategy                                  | Coordination lives in                             | Works in one process          | Works across processes          | Best fit                                                              |
| ----------------------------------------- | ------------------------------------------------- | ----------------------------- | ------------------------------- | --------------------------------------------------------------------- |
| Keyed mutex                               | A JavaScript `Map` owned by one checkout instance | Yes, with one shared instance | No                              | A single writer process, or a local optimization above a shared guard |
| Postgres advisory lock (`READ COMMITTED`) | PostgreSQL's advisory-lock manager                | Yes                           | Yes                             | Multiple application processes sharing one Postgres database          |

"Works across processes" is especially important for containers and
serverless functions. Each running instance has its own JavaScript memory.

## Keyed mutex

### How it works

`createKeyedMutexCheckout()` creates a `Map` of promise queues, keyed by locker
ID. Calls for the same locker wait in the same queue:

```text
caller A: read → insert → release
                              ↓
caller B:                  read → return unavailable
```

Calls for different lockers use different queues, so they can run at the same
time.

The `Map` belongs to one factory-created checkout instance. The application
must therefore create that instance once and reuse it. Creating another
instance creates another independent `Map`.

### What the tests prove

| Test                                                                                        | Evidence                                                                                                                                                       |
| ------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Shared contract](../test/integration/strategies/keyed-mutex/contract.test.ts)              | Many concurrent calls through one checkout instance produce one winner, while every other caller returns `unavailable`. Postgres contains one active checkout. |
| [Same-locker queueing](../test/integration/strategies/keyed-mutex/queueing.test.ts)         | While caller A is held inside the critical section, caller B cannot reach the same point or request a database connection.                                     |
| [Different-locker concurrency](../test/integration/strategies/keyed-mutex/queueing.test.ts) | Caller B can enter its critical section while caller A is paused when they use different locker IDs. Both calls succeed.                                       |
| [Two-process topology](../test/integration/strategies/keyed-mutex/topology.test.ts)         | Two explicit Node.js child processes both reach the availability-read seam before either is released. Both then succeed, leaving two active checkouts.         |

Together, the tests show the boundary clearly:

```text
same checkout instance → shared Map     → callers are coordinated
different processes    → separate Maps → callers are not coordinated
```

The two-process test passes by demonstrating this expected limitation. The
mutex is behaving correctly; it simply cannot coordinate memory owned by
another process.

### Tradeoffs

- **Process-local only.** Multiple factory instances, application processes,
  containers, or serverless invocations do not share the queue.
- **Hot lockers build a queue.** Calls for one popular locker wait one at a
  time, so latency grows with the number of callers.
- **The current critical section includes pool waiting.** A caller acquires the
  mutex before `pool.connect()`. A busy database pool can therefore delay every
  caller queued for that locker.
- **All writers must use the same instance.** Code that writes directly to
  Postgres bypasses the mutex completely.

### When to use it

A keyed mutex is sufficient when one process is genuinely the only writer and
all writes use the same checkout instance—for example, a CLI or a
single-instance worker.

It is not sufficient by itself in a multi-container or serverless deployment.
It can still be useful there as a local optimization: it can reduce contention
before requests reach Postgres or Redis, while the shared system provides the
actual correctness guarantee.

## Postgres advisory lock

### How it works

`createAdvisoryLockCheckout()` opens a transaction explicitly at `READ
COMMITTED` isolation and acquires a transaction-scoped advisory lock before it
checks availability:

```text
BEGIN ISOLATION LEVEL READ COMMITTED
    ↓
pg_advisory_xact_lock(namespace, locker ID)
    ↓
read availability → insert if available → COMMIT
                                            ↓
                                      lock released
```

Every checkout instance using the same lock key asks PostgreSQL to coordinate
through its shared lock manager. A caller that loses the race waits inside
`pg_advisory_xact_lock(...)`. After the winner commits, the waiter acquires the
lock and starts its availability query.

`READ COMMITTED` matters because PostgreSQL gives each statement a fresh
snapshot. The waiter's availability query therefore sees the previous
holder's committed checkout and returns `unavailable`. The implementation
sets this isolation level explicitly rather than relying on the connection's
default.

The lock uses PostgreSQL's two-`integer` key form:

```sql
pg_advisory_xact_lock(hashtext(lock_namespace), locker_id)
```

The namespace separates identical locker IDs belonging to different test
schemas or application domains. A `hashtext()` collision can make unrelated
namespaces wait unnecessarily, but it cannot allow conflicting checkouts to
run concurrently because a collision makes them share a lock.

### What the tests prove

| Test                                                                                    | Status and evidence                                                                                                                      |
| --------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| [Shared contract](../test/integration/strategies/advisory-lock/contract.test.ts)        | Complete. Many concurrent calls produce one winner, every other caller returns `unavailable`, and Postgres contains one active checkout. |
| [Lock contention](../test/integration/strategies/advisory-lock/lock-contention.test.ts) | Complete. Caller A pauses while holding the lock; `pg_locks` shows caller B waiting for that exact key. B acquires it only after A is released. |
| [Two-process topology](../test/integration/strategies/advisory-lock/topology.test.ts)   | Complete. Two explicit Node.js child processes request the same lock key. PostgreSQL makes B wait for A, producing one active checkout.   |

The contract establishes the behavior in one process, the contention test
proves that PostgreSQL serializes callers requesting the same key, and the
topology test confirms that this coordination crosses Node.js process
boundaries.

### Tradeoffs

- **All writers must follow the lock-key convention.** Advisory locks do not
  attach themselves to a table row or constraint. A direct insert that omits
  the lock can still violate the invariant.
- **Waiting consumes a database connection.** Advisory locks don’t inherently
  use more connections, but contended advisory locks can tie up many
  connections in waiters, reducing how effectively the pool can reuse them.
- **Hot lockers serialize.** Only one transaction for a given lock key can
  enter the availability-and-insert section at a time.
- **Transaction scope simplifies cleanup.** PostgreSQL releases the lock on
  commit, rollback, or connection loss; the application does not need a
  separate unlock command.
- **Isolation level is part of correctness.** This implementation requires
  `READ COMMITTED` so the post-wait availability statement receives a fresh
  snapshot.

### When to use it

An advisory lock fits when several application processes share PostgreSQL and
need to coordinate a short operation whose lock key can be derived reliably.
It is especially useful when the protected rule spans more than one statement
or is awkward to express as a database constraint.

It is less attractive when lock waits would occupy scarce pool connections,
or when not every writer can be required to use the same convention. If an
invariant can be expressed directly as a PostgreSQL constraint, enforcing it
in the schema provides a stronger guard against bypassing application code.
