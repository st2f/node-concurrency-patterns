# Learning steps — vertical slices

## Strategy module boundary

Keep production strategies separate from their tests and mirror the strategy
names used under `test/integration/strategies/`:

```text
src/
  checkout.ts
  strategies/
    keyed-mutex.ts
    advisory-lock.ts
    optimistic-locking.ts
    unique-constraint.ts
    redis-fencing.ts
```

Each module exports a factory that receives its infrastructure dependencies
and returns the shared `Checkout` function directly. It must not import
`CheckoutContractSubject` or anything else from `test/`.

The keyed-mutex factory gains this options boundary when its deterministic
seam is introduced:

```ts
import type { Pool } from "pg";
import type { Checkout } from "../checkout.ts";

export interface KeyedMutexCheckoutOptions {
  afterAvailabilityRead?(): Promise<void>;
}

export function createKeyedMutexCheckout(
  pool: Pool,
  options: KeyedMutexCheckoutOptions = {},
): Checkout {
  // Strategy implementation.
}
```

`afterAvailabilityRead` is awaited inside the mutex-held critical section,
immediately after the availability query and before the insert. It is a
strategy-specific orchestration seam for deterministic tests, not part of the
domain-level `Checkout` type.

The contract test adapts the returned function to its test-only subject shape:

```ts
checkoutContract({
  name: "keyed mutex",
  createSubject: (pool) => ({
    checkout: createKeyedMutexCheckout(pool),
  }),
});
```

The keyed mutex does not need the harness namespace because its queue map is
owned by one factory instance. This depends on a strict ownership rule: create
the map inside `createKeyedMutexCheckout()` on every factory call and capture
it in the returned function—never place it in module scope. The application
creates one checkout instance at process startup and reuses it for that
process's lifetime; tests create one instance per subject. Strategies using
database-wide or external coordination must consume the namespace: advisory
locks include it in their lock key, and Redis includes it in lock and fencing
keys.

## Incremental workflow

Complete one strategy directory at a time. For example:

```text
test/integration/strategies/
  keyed-mutex/
    contract.test.ts
    queueing.test.ts
    topology.test.ts
```

Implement it incrementally:

1. `contract.test.ts`
   - Create `src/strategies/keyed-mutex.ts`, exporting
     `createKeyedMutexCheckout(pool, options?)`.
   - Apply the shared checkout contract.
   - Run it red locally to establish that it detects the missing behavior, but
     never commit the deliberately red state.
   - Implement the keyed-mutex `checkout()` until it passes.

2. `queueing.test.ts`
   - Introduce the strategy-specific `afterAvailabilityRead` seam described
     above.
   - Hold caller A at that seam, inside the mutex-held critical section and
     immediately after its availability read.
   - Start caller B and prove it cannot reach the same seam until A is
     released.
   - This proves the mutex queues callers within one Node process.

3. `topology.test.ts`
   - Build `test/support/checkout-worker.ts` and its parent-side IPC barrier.
     This is a substantial one-time investment reused by every later topology
     test.
   - Make the worker configurable through its initial IPC message (or command
     arguments): receive the strategy identifier, serializable infrastructure
     configuration, and optional orchestration mode, then construct the
     requested strategy. Do not hard-code the worker to
     `createKeyedMutexCheckout()`.
   - Spawn two explicit application processes; do not use Vitest workers as
     the topology under test.
   - Demonstrate that each process owns a different in-memory mutex.
   - Reuse `afterAvailabilityRead` across the two processes. Each worker
     reports reaching the seam while holding its own process-local mutex, then
     waits until the parent releases both workers.
   - Later strategies reuse the worker's process, IPC, result, and cleanup
     protocol without enabling the keyed-mutex seam; their real coordination
     mechanisms provide the contention boundary.
   - This proves two mutex-held critical sections can overlap when their mutex
     instances live in different processes.
   - The test passes by proving the expected limitation: both processes can
     win under the controlled interleaving.

4. Run the entire strategy directory.
   - Keep the directory green after each addition.
   - Commit each completed learning milestone separately: contract behavior,
     mechanism proof, and topology proof. Do not commit a deliberately red
     test.

5. Update the comparison.
   - Create `docs/comparison.md` during the first strategy slice.
   - Add the completed strategy's row immediately: coordination location,
     multi-process behavior, failure modes, and appropriate use cases.

The queueing and topology tests make the coordination-boundary claim jointly:

```text
queueing.test.ts: mutex serializes callers within one process
                              +
topology.test.ts: mutexes do not coordinate across processes
                              =
             coordination boundary is the Node process
```

Then repeat for the next strategy.

The original 1b/1c/1d labels are better understood as test categories, not implementation phases:

```text
one strategy
    ├── behavioral contract
    ├── mechanism proof
    └── process-topology proof
```

For learning progression, use this order:

1. Keyed mutex — introduces JavaScript promise queues and process-local state.
2. PostgreSQL advisory lock — moves coordination into shared storage.
3. Optimistic locking — introduces versioned compare-and-swap.
4. Unique constraint — expresses the invariant directly in PostgreSQL.
5. Redis lease and fencing — combines the previous lessons and has the most complex failure modes.

Redis is especially valuable last because its fencing solution is easier to understand after optimistic locking and database-enforced invariants.

So the next concrete milestone is
`test/integration/strategies/keyed-mutex/contract.test.ts`, followed by the
other keyed-mutex tests one at a time.
