# Strategy comparison

One row per strategy, drafted during its slice and considered complete once its
behavioral, mechanism, and topology tests are in place. "Correct across
processes" is the question that matters for a container or serverless deployment,
where the same image runs many times.

| Strategy | Mechanism | Coordination lives in | Correct across processes | Main failure mode |
| --- | --- | --- | --- | --- |
| Keyed mutex | Per-locker promise chain: each caller awaits the tail promise for its locker id before reading availability, and releases its tail after the database operation completes | A `Map` owned by one factory instance, normally reused as a process-level singleton | No | Two processes hold two independent mutexes, so both can pass the availability read and both insert |

## Keyed mutex

After the preceding tail settles, the caller owns the critical section before it
acquires a connection. Its tail is released by the outer `finally` after the
database work and client cleanup, whether the operation commits, rolls back, or
fails earlier. Therefore, the availability read and insert cannot be interleaved
by another caller using the same checkout instance. Nothing about the mechanism
reaches storage: the queue is plain JavaScript state, which is why its
synchronization boundary is exactly the factory instance that owns the `Map`.

Failure modes:

- **Multiple instances.** Each instance gets its own queue, and the invariant is
  no longer enforced between them. Normally this means one queue per process,
  and that is the defining limitation, not a bug.
- **Serialization cost.** All callers for one locker are queued, including the
  ones that will lose. Under heavy contention on a single locker, latency grows
  with queue depth even though only the first caller can win.
- **Long critical section.** The mutex is acquired before `pool.connect()`, so a
  caller holds it while waiting for a connection as well as across its database
  round trips. The queue convoys behind whichever caller is slowest, and pool
  exhaustion elsewhere in the process shows up as checkout latency here.

Appropriate when the process is genuinely the only writer and every write uses
the same checkout instance: a single-instance worker, a CLI, or a test. In a
container or serverless stack it is not sufficient on its own. It remains useful
*above* a shared mechanism, where a process-level singleton collapses local
contention before that contention reaches Postgres or Redis — but the correctness
guarantee has to come from the shared layer.

The queueing and topology tests that demonstrate both halves of this claim are
still pending; the row above records the mechanism's boundary, and
`topology.test.ts` is what will make the "No" concrete.
