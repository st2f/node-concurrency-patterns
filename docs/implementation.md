# Implementation — vertical slices

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
and returns the shared `Checkout` function directly.

## Factory example

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

## Incremental workflow

Complete one strategy directory at a time.

```text
one strategy
    ├── behavioral contract
    ├── mechanism proof
    └── process-topology proof
```
