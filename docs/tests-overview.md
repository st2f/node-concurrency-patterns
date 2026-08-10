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
advisory → hold PG lock A → verify B waits
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
PG advisory       ✓                  ✓
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

## Notes

This project is a compact exploration of `how do concurrent Node processes safely modify shared state?`

It moves through increasingly important boundaries:

```text
JS async execution
      ↓
single Node process
      ↓
multiple Node processes
      ↓
Postgres transactions
      ↓
external coordination (Redis)
      ↓
storage-level correctness
```
