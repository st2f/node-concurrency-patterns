# Strategy comparison

This page grows one strategy at a time. A strategy is added after its shared
contract, mechanism, and process-topology tests are complete.

## At a glance

| Strategy | Coordination lives in | Works in one process | Works across processes | Best fit |
| --- | --- | --- | --- | --- |
| Keyed mutex | A JavaScript `Map` owned by one checkout instance | Yes, with one shared instance | No | A single writer process, or a local optimization above a shared guard |

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
