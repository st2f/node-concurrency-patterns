# Strategy comparison

This page grows one strategy at a time. Entries distinguish behavior already covered by tests from claims whose mechanism or process-topology tests are still pending.

## At a glance

| Strategy | Coordination lives in | Works in one process | Works across processes | Best fit |
| --- | --- | --- | --- | --- |
| Keyed mutex | A JavaScript `Map` owned by one checkout instance | Yes, with one shared instance | No | A single writer process, or a local optimization above a shared guard |
| Postgres advisory lock (`READ COMMITTED`) | PostgreSQL's advisory-lock manager | Yes | Yes | Multiple application processes sharing one Postgres database |
| Redis lock with PostgreSQL fencing | Redis lock/token counter and a PostgreSQL row | Yes | Yes | Distributed coordination that must reject expired owners |
| Optimistic locking | A version column on the PostgreSQL locker row | Yes | Yes | Low-contention writes where callers can retry after conflicts |
| Database uniqueness rule | A PostgreSQL partial unique index | Yes | Yes | Invariants that can be expressed directly in the database schema |

"Works across processes" is especially important for containers and serverless functions. Each running instance has its own JavaScript memory.

## Keyed mutex

### How it works

`createKeyedMutexCheckout()` creates a `Map` of promise queues, keyed by locker ID. Calls for the same locker wait in the same queue:

```text
caller A: read → insert → release
                              ↓
caller B:                  read → return unavailable
```

Calls for different lockers use different queues, so they can run at the same time.

The `Map` belongs to one factory-created checkout instance. The application must therefore create that instance once and reuse it. Creating another instance creates another independent `Map`.

### What the tests prove

| Test | Evidence |
| --- | --- |
| [Shared contract](../test/integration/strategies/keyed-mutex/contract.test.ts) | Many concurrent calls through one checkout instance produce one winner, while every other caller returns `unavailable`. Postgres contains one active checkout. |
| [Same-locker queueing](../test/integration/strategies/keyed-mutex/queueing.test.ts) | While caller A is held inside the critical section, caller B cannot reach the same point or request a database connection. |
| [Different-locker concurrency](../test/integration/strategies/keyed-mutex/queueing.test.ts) | Caller B can enter its critical section while caller A is paused when they use different locker IDs. Both calls succeed. |
| [Two-process topology](../test/integration/strategies/keyed-mutex/topology.test.ts) | Two explicit Node.js child processes both reach the availability-read seam before either is released. Both then succeed, leaving two active checkouts. |

Together, the tests show the boundary clearly:

```text
same checkout instance → shared Map     → callers are coordinated
different processes    → separate Maps → callers are not coordinated
```

The two-process test passes by demonstrating this expected limitation. The mutex is behaving correctly; it simply cannot coordinate memory owned by another process.

### Tradeoffs

- **Process-local only.** Multiple factory instances, application processes, containers, or serverless invocations do not share the queue.
- **Hot lockers build a queue.** Calls for one popular locker wait one at a time, so latency grows with the number of callers.
- **The current critical section includes pool waiting.** A caller acquires the mutex before `pool.connect()`. A busy database pool can therefore delay every caller queued for that locker.
- **All writers must use the same instance.** Code that writes directly to Postgres bypasses the mutex completely.

### When to use it

A keyed mutex is sufficient when one process is genuinely the only writer and all writes use the same checkout instance—for example, a CLI or a single-instance worker.

It is not sufficient by itself in a multi-container or serverless deployment. It can still be useful there as a local optimization: it can reduce contention before requests reach Postgres or Redis, while the shared system provides the actual correctness guarantee.

## Postgres advisory lock

### How it works

`createAdvisoryLockCheckout()` opens a transaction explicitly at `READ COMMITTED` isolation and acquires a transaction-scoped advisory lock before it checks availability:

```text
BEGIN ISOLATION LEVEL READ COMMITTED
    ↓
pg_advisory_xact_lock(namespace, locker ID)
    ↓
read availability → insert if available → COMMIT
                                            ↓
                                      lock released
```

Every checkout instance using the same lock key asks PostgreSQL to coordinate through its shared lock manager. A caller that loses the race waits inside `pg_advisory_xact_lock(...)`. After the winner commits, the waiter acquires the lock and starts its availability query.

`READ COMMITTED` matters because PostgreSQL gives each statement a fresh snapshot. The waiter's availability query therefore sees the previous holder's committed checkout and returns `unavailable`. The implementation sets this isolation level explicitly rather than relying on the connection's default.

The lock uses PostgreSQL's two-`integer` key form:

```sql
pg_advisory_xact_lock(hashtext(lock_namespace), locker_id)
```

The namespace separates identical locker IDs belonging to different test schemas or application domains. A `hashtext()` collision can make unrelated namespaces wait unnecessarily, but it cannot allow conflicting checkouts to run concurrently because a collision makes them share a lock.

### What the tests prove

| Test | Status and evidence |
| --- | --- |
| [Shared contract](../test/integration/strategies/advisory-lock/contract.test.ts) | Many concurrent calls produce one winner, every other caller returns `unavailable`, and Postgres contains one active checkout. |
| [Lock contention](../test/integration/strategies/advisory-lock/lock-contention.test.ts) | Caller A pauses while holding the lock; `pg_locks` shows caller B waiting for that exact key. B acquires it only after A is released. |
| [Two-process topology](../test/integration/strategies/advisory-lock/topology.test.ts) | Two explicit Node.js child processes request the same lock key. PostgreSQL makes B wait for A, producing one active checkout. |

The contract establishes the behavior in one process, the contention test proves that PostgreSQL serializes callers requesting the same key, and the topology test confirms that this coordination crosses Node.js process boundaries.

### Tradeoffs

- **All writers must follow the lock-key convention.** Advisory locks do not attach themselves to a table row or constraint. A direct insert that omits the lock can still violate the invariant.
- **Waiting consumes a database connection.** Advisory locks don’t inherently use more connections, but contended advisory locks can tie up many connections in waiters, reducing how effectively the pool can reuse them.
- **Hot lockers serialize.** Only one transaction for a given lock key can enter the availability-and-insert section at a time.
- **Transaction scope simplifies cleanup.** PostgreSQL releases the lock on commit, rollback, or connection loss; the application does not need a separate unlock command.
- **Isolation level is part of correctness.** This implementation requires `READ COMMITTED` so the post-wait availability statement receives a fresh snapshot.

### When to use it

An advisory lock fits when several application processes share PostgreSQL and need to coordinate a short operation whose lock key can be derived reliably. It is especially useful when the protected rule spans more than one statement or is awkward to express as a database constraint.

It is less attractive when lock waits would occupy scarce pool connections, or when not every writer can be required to use the same convention. If an invariant can be expressed directly as a PostgreSQL constraint, enforcing it in the schema provides a stronger guard against bypassing application code.

## Redis lock with PostgreSQL fencing

### How it works

`createRedisFencingCheckout()` combines two mechanisms with different jobs:

| Mechanism | Purpose |
| --- | --- |
| Redis lock | Coordinates the owner expected to perform work now |
| Random owner token | Prevents an old owner from deleting another owner's replacement lock |
| Increasing fencing token | Orders owners so PostgreSQL can reject one that resumes too late |
| PostgreSQL `last_fencing_token` | Remembers the newest owner accepted by the protected storage resource |

Acquisition runs one short Redis Lua script. It attempts the lock with `SET NX PX` and increments the resource's fencing counter only after the lock was acquired:

```text
SET lock-key <random UUID> NX PX <TTL>
                 ↓ acquired
INCR fencing-token-key
                 ↓
return fencing token
```

`NX` means the lock is created only when the key is absent. `PX` sets its expiry in milliseconds so a crashed process does not retain the lock forever. Redis executes the complete Lua script without interleaving another command, making lock acquisition and token allocation one atomic operation. A caller that finds an existing lock currently returns `unavailable`; this implementation does not wait or retry.

The lock key and fencing-counter key use the same Redis Cluster hash tag:

```text
redis-fencing:{namespace:locker:42}:lock
redis-fencing:{namespace:locker:42}:token
```

Redis Cluster hashes only `namespace:locker:42`, placing both keys in the same hash slot. A multi-key Lua script cannot run across different cluster nodes, so this key placement is part of cluster compatibility. Different lockers use different hash tags and can still be distributed across the cluster.

### Why the lock is not enough

A TTL limits how long Redis recognizes an owner, but it cannot stop code that was paused while holding the lock:

```text
A acquires token 1
A pauses past its TTL
                    B acquires token 2
                    B reaches PostgreSQL
A resumes
```

The random owner token makes A's later release harmless: its compare-and-delete Lua script sees that A no longer owns the lock and leaves B's key intact. It does not, however, prevent A from sending a late database write. Ownership-safe release and stale-write prevention are separate guarantees.

Before reading availability, each owner presents its fencing token to PostgreSQL:

```sql
UPDATE lockers
SET last_fencing_token = $token
WHERE id = $locker_id
  AND last_fencing_token < $token;
```

If a newer token has already been recorded, the update affects zero rows and the stale checkout returns `unavailable`. If the token is accepted, the update locks that locker row until the transaction completes. The strategy then reads availability and inserts the checkout in the same `READ COMMITTED` transaction:

```text
accept fencing token and lock locker row
                    ↓
read current availability
                    ↓
insert checkout if available
                    ↓
commit and release row lock
```

The ordering matters. A competing database writer waits for the locker row, then its separate availability statement receives a fresh `READ COMMITTED` snapshot after the preceding transaction commits.

The Redis lock is released in `finally` with a second atomic Lua script. It deletes the key only when the stored UUID still matches the caller's owner token. If the lock expired or was replaced, the script deliberately does nothing.

### What the tests currently prove

| Test | Status and evidence |
| --- | --- |
| [Shared contract](../test/integration/strategies/redis-fencing/contract.test.ts) | Implemented: concurrent calls produce one winner, the other callers return `unavailable`, and PostgreSQL contains one active checkout. |
| [Lock ownership](../test/integration/strategies/redis-fencing/lock-ownership.test.ts) | Implemented: an unexpired lock excludes another owner; an old owner's release cannot delete a replacement lock, while the current owner can release it. |
| [Lock expiry](../test/integration/strategies/redis-fencing/lock-expiry.test.ts) | Implemented: another owner acquires after TTL expiry; without storage validation, the expired holder inserts a second active checkout while the newer owner holds the lock. |
| [Fencing](../test/integration/strategies/redis-fencing/fencing.test.ts) | Implemented: successive owners receive increasing tokens; an expired owner is rejected after PostgreSQL accepts a newer token, even when the locker is available again. |
| [Two-process topology](../test/integration/strategies/redis-fencing/topology.test.ts) | Implemented: A holds the Redis lock in one child process before writing; B in another process returns unavailable, then A commits the sole active checkout. |

The contract establishes black-box behavior under ordinary contention. The mechanism tests prove expiry behavior and stale-writer rejection. The topology test proves that Redis excludes a contender across explicit application processes before the winning checkout reaches PostgreSQL.

### Tradeoffs

- **Two infrastructure systems participate.** Correctness depends on both Redis lock/token operations and PostgreSQL's conditional write. This adds operational and failure-handling complexity compared with a PostgreSQL-only strategy.
- **The lock does not cancel JavaScript execution.** A process can continue after its TTL expires. Fencing works because the protected storage system checks the token, not because the old process knows that it became stale.
- **Every writer must honor fencing.** A direct checkout insert that does not condition its work on `last_fencing_token` bypasses stale-owner protection.
- **There is no lock renewal or contention queue.** A long operation can outlive the default five-second lock, while a caller encountering an existing lock immediately receives `unavailable`.
- **The fencing counter must retain its ordering.** If Redis loses or resets the counter while PostgreSQL retains `last_fencing_token`, newly generated values are no longer newer. A production design must make the counter durable enough for its safety assumptions, recover it safely, or generate the ordering token in durable storage. This repository's intentionally ephemeral Redis container is safe for isolated tests with fresh namespaces, but it does not by itself provide that production durability.
- **Hot lockers still serialize in PostgreSQL.** Fencing is not a substitute for the row-level serialization needed around the availability read and insert.

### When to use it

Redis locks with storage-enforced fencing fit distributed workers that already depend on Redis, need crash recovery through lock expiry, and can require every protected storage write to validate an ordering token. The pattern is especially relevant when work extends beyond one PostgreSQL lock or transaction and an expired process may resume later.

It is unnecessary complexity when PostgreSQL alone can express the invariant with a constraint, conditional update, or advisory lock. A Redis lock without storage-side fencing is also insufficient when stale owners can cause damage: safe release protects the lock key, while fencing protects the resource.

## Optimistic locking

### How it works

`createOptimisticLockingCheckout()` reads the locker's availability and current `version` without first taking an application-level or advisory lock. If the locker appears available, it tries to claim the observed version:

```sql
UPDATE lockers
SET version = version + 1
WHERE id = $1 AND version = $2;
```

The `pg` query result's `rowCount` distinguishes the outcomes:

```text
rowCount = 1 → this caller claimed the version → insert checkout → commit
rowCount = 0 → another writer changed it       → read fresh state again
```

The version update and checkout insert share one transaction. PostgreSQL therefore cannot expose the increment without its corresponding checkout, and a failed insert rolls both changes back.

The transaction uses `READ COMMITTED`. After a zero-row conditional update, the loop's next query gets a fresh snapshot. It normally sees the winning caller's active checkout and returns `unavailable`. If the conflicting change instead made the locker available, the caller can retry with the new version.

Optimistic locking avoids making callers queue before reading, but it does not mean PostgreSQL never waits internally. Concurrent conditional updates of the same row can briefly wait on PostgreSQL's row lock while the winning transaction finishes. The version predicate is then checked against the newly committed row.

### What the tests prove

| Test | Status and evidence |
| --- | --- |
| [Shared contract](../test/integration/strategies/optimistic-locking/contract.test.ts) | Sixteen concurrent calls produce one winner, every other caller returns `unavailable`, and PostgreSQL contains one active checkout. |
| [Version conflict](../test/integration/strategies/optimistic-locking/version-conflict.test.ts) | Both callers pause after reading version `0`; one wins, while the loser re-reads version `1` after its conditional update affects zero rows. |
| [Two-process topology](../test/integration/strategies/optimistic-locking/topology.test.ts) | Two child processes read version `0`; PostgreSQL accepts one conditional update, producing one winner and one active checkout at version `1`. |

The contract establishes the intended black-box behavior within one process. The version-conflict test supplies the deterministic mechanism evidence that the contract cannot: both writers compare the same version, but only one advances it and inserts a checkout. The topology test proves that this coordination crosses Node.js process boundaries because the compared state lives in PostgreSQL rather than application memory.

### Tradeoffs

- **No external lock service.** The mechanism uses an ordinary PostgreSQL column and conditional `UPDATE`; there is no lock key or lock to manage.
- **Conflicts become application control flow.** A zero-row update is an expected race outcome, not an infrastructure error. The application must re-read and decide whether to retry or return `unavailable`.
- **Best under low contention.** Callers do useful work concurrently when conflicts are rare. A hot locker causes repeated reads, failed conditional updates, and database row-lock waits.
- **All relevant writers must advance the version.** Checkout, release, and any other operation that changes availability must update the same locker version in its transaction. A direct checkout insert that does not advance the version bypasses this protection.
- **The version is state, not ownership.** It detects that the row changed since a read; it does not identify a lock holder or impose the acquisition ordering provided by a fencing token.

### When to use it

Optimistic locking fits when conflicting updates are uncommon, callers can cheaply re-read after losing a race, and every writer shares PostgreSQL and follows the version convention. It avoids introducing a separate coordination system and allows unrelated lockers to proceed independently.

It is less attractive for highly contended rows or operations whose work is expensive to repeat. It also does not protect against code paths that modify the invariant without participating in the version update. When the business rule can be expressed directly as a database constraint, the constraint is a stronger final line of defense.

## Database uniqueness rule

### How it works

The schema expresses the invariant with a partial unique index:

```sql
CREATE UNIQUE INDEX one_active_checkout_per_locker
ON checkouts (locker_id)
WHERE released_at IS NULL;
```

The predicate matters because `checkouts` keeps history. A plain `UNIQUE (locker_id)` would reject every future checkout for a locker once its first checkout row existed. The partial index ignores released rows and therefore limits only rows representing active checkouts.

`createUniqueConstraintCheckout()` does not read availability before writing. The insert itself is the attempt to claim the locker:

```text
caller A: INSERT ──→ accepted
caller B: INSERT ──→ SQLSTATE 23505
```

When concurrent inserts target the same locker, PostgreSQL accepts one active row and rejects the other. The strategy recognizes a `23505` unique violation only when PostgreSQL names `one_active_checkout_per_locker` as the violated constraint. That expected conflict becomes `{ outcome: "unavailable" }`. Other unique violations, foreign-key failures, and infrastructure errors are re-thrown rather than being mislabeled as normal contention.

This is an invariant enforced by the database, not an application locking protocol. PostgreSQL may wait internally while it determines whether a conflicting transaction commits, but callers do not acquire or release an explicit application-visible lock.

### Migration scope

The partial index is deliberately a strategy-specific migration. The shared base migrations omit it so the baseline race and the limitations of the other strategies remain observable.

During tests, `applyUniqueConstraintMigration()` applies `migrations/strategies/unique-constraint.sql` only to the isolated schema used by this strategy. The normal `npm run migrate` command scans only SQL files at the top level of `migrations/`, so it does not currently install this index in the application's default schema. A deployment using this strategy must add the strategy migration to its selected migration path.

### What the tests prove

| Test | Status and evidence |
| --- | --- |
| [Shared contract](../test/integration/strategies/unique-constraint/contract.test.ts) | Sixteen concurrent calls produce one winner, every other caller returns `unavailable`, and PostgreSQL contains one active checkout. |
| [Constraint collision](../test/integration/strategies/unique-constraint/constraint-collision.test.ts) | Caller B waits on caller A's uncommitted active row, then receives SQLSTATE `23505` for the named index after A commits. A released historical row permits a later active checkout. |
| [Two-process topology](../test/integration/strategies/unique-constraint/topology.test.ts) | Two child processes pause before inserting into the same isolated schema. PostgreSQL accepts one active row, and the other process returns `unavailable`. |

The contract establishes the black-box behavior within one process under likely contention. The collision tests isolate the database mechanism: they observe the losing insert waiting inside PostgreSQL, verify the exact error identity, and exercise the partial predicate after release. The topology test proves the same database rule protects the invariant when no application memory is shared between callers.

### Tradeoffs

- **The invariant covers every writer.** Direct SQL and new application code cannot bypass the rule while they write to the constrained PostgreSQL table.
- **No read-before-write race.** One atomic insert replaces the separate availability check and insert used by the naive implementation.
- **Expected contention arrives as an error.** The application must identify the intended constraint precisely and translate its violation into a domain result without hiding unrelated database failures.
- **The rule must fit a database constraint.** A partial index works well for this row-local invariant, but rules spanning complex workflows or external systems may still require another coordination mechanism.
- **Schema rollout is part of the feature.** Existing duplicate active rows must be resolved before creating the index, and every deployed environment must apply the strategy-specific migration.
- **PostgreSQL remains the coordination boundary.** Writers using another datastore are outside this guarantee.

### When to use it

A database uniqueness rule is usually the strongest and simplest choice when the business invariant can be represented by a PostgreSQL constraint. It protects the data even when a new code path forgets an application-level locking convention.

It is less suitable when the invariant cannot be expressed in one database, when writes must coordinate with external side effects, or when conflict handling requires work before the database statement is attempted. In those cases a lock or conditional-write protocol may still be needed, often with a database constraint retained as the final line of defense where possible.
