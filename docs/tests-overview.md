# Tests overview

## 1a — Demonstrate the bug deterministically

You manually create:

```text
A: read "available"
B: read "available"
A: write
B: write
```

This proves the baseline problem exists. It deliberately doesn’t test checkout().

## 1b — Common behavioral contract

Within one Node process, every strategy gets the same black-box question:

```text
N concurrent checkout() calls
          ↓
       strategy
          ↓
exactly 1 winner?
exactly 1 active checkout?
```

That’s useful because all implementations must satisfy the same externally observable requirement within their intended synchronization boundary.

## 1c — Prove why each strategy works

Deliberately exercise the synchronization mechanism:

```text
mutex → hold callback A → verify B waits
advisory (READ COMMITTED) → hold PG lock A → verify B waits
Redis → holder A → verify B can't acquire
optimistic → same version → verify one UPDATE affects zero rows
DB constraint → concurrent INSERTs → verify one fails with SQLSTATE 23505
```

So 1b tests behavior, while 1c tests the mechanism.

## 1d — Change the topology

```text
             one process       two processes
                  │                  │
keyed mutex       ✓                  ✗
PG advisory
(READ COMMITTED)  ✓                  ✓
Redis + storage
validation        ✓                  ✓
optimistic        ✓                  ✓
DB constraint     ✓                  ✓
```

That will make the limitation of an in-memory mutex extremely concrete. The mutex isn’t defective; its synchronization boundary is simply the Node process.

A lease-only Redis lock can pass ordinary contention tests across processes while its TTL remains valid, but it fails the expired-holder scenario shown below. The checkmarks therefore apply to Redis combined with storage-layer validation, such as fencing.

## Redis progression

```text
SET NX PX
   ↓
token-safe release
   ↓
"so distributed locking is solved?"
   ↓
NO — lease expires while A is paused
   ↓
B obtains lock
   ↓
A wakes up and can still write
   ↓
fencing token
   ↓
storage rejects stale A
```

## Proposed file layout

```text
test/
  integration/
    baseline-race.test.ts

    strategies/
      keyed-mutex/
        contract.test.ts
        queueing.test.ts
        topology.test.ts

      advisory-lock/
        contract.test.ts
        lock-contention.test.ts
        topology.test.ts

      optimistic-locking/
        contract.test.ts
        version-conflict.test.ts
        topology.test.ts

      unique-constraint/
        contract.test.ts
        constraint-collision.test.ts
        topology.test.ts

      redis-fencing/
        contract.test.ts
        lock-ownership.test.ts
        lease-expiry.test.ts
        fencing.test.ts
        topology.test.ts

  support/
    checkout-contract.ts
    database.ts
    checkout-worker.ts
```

The baseline remains separate because it deliberately bypasses every strategy.
Within each strategy directory:

- `contract.test.ts` applies the shared black-box checkout contract.
- The mechanism-specific file proves why that strategy works, such as lock
  contention, a version conflict, or a constraint violation.
- `topology.test.ts` exercises the strategy through explicit application
  processes rather than relying on Vitest's worker processes.

The worker and parent-side IPC barrier are shared infrastructure: build them
for the keyed-mutex topology test, then reuse them for the remaining strategy
directories.

Redis has additional files because acquiring and safely releasing a lease,
demonstrating expiry failure, and rejecting stale writers through fencing are
separate claims.
