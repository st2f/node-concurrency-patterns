import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import type { Pool } from "pg";
import { it } from "vitest";
import type { CheckoutResult } from "../../../../src/checkout.ts";
import { createKeyedMutexCheckout } from "../../../../src/strategies/keyed-mutex.ts";
import { createIsolatedTestDatabase } from "../../../support/database.ts";

const SEAM_TIMEOUT_MS = 2_000;

interface Barrier {
  promise: Promise<void>;
  release(): void;
}

function createBarrier(): Barrier {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });

  return { promise, release };
}

async function waitForSeam(
  barrier: Barrier,
  attempt: Promise<CheckoutResult>,
): Promise<void> {
  const finishedBeforeSeam = attempt.then(
    (result) => {
      throw new Error(
        `checkout finished with ${result.outcome} before reaching the seam`,
      );
    },
    (error: unknown) => {
      throw error;
    },
  );

  let timeout: ReturnType<typeof setTimeout> | undefined;
  const timedOut = new Promise<never>((_, reject) => {
    timeout = setTimeout(() => {
      reject(
        new Error(
          `checkout did not reach the seam within ${SEAM_TIMEOUT_MS}ms`,
        ),
      );
    }, SEAM_TIMEOUT_MS);
    timeout.unref();
  });

  try {
    await Promise.race([barrier.promise, finishedBeforeSeam, timedOut]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}

function drainEventLoop(): Promise<void> {
  return new Promise((resolve) => {
    setImmediate(resolve);
  });
}

it("queues keyed-mutex callers for the same locker", async () => {
  const database = await createIsolatedTestDatabase({ maxConnections: 2 });
  const firstAtAvailabilityRead = createBarrier();
  const releaseFirstCaller = createBarrier();
  const secondAtAvailabilityRead = createBarrier();
  let seamVisits = 0;
  let connectAttempts = 0;
  let attemptA: Promise<CheckoutResult> | undefined;
  let attemptB: Promise<CheckoutResult> | undefined;

  // Observing connect() makes the negative assertion deterministic: after the
  // event loop drains, an unqueued caller would have requested a connection.
  const observedPool = new Proxy(database.pool, {
    get(target, property) {
      if (property === "connect") {
        return () => {
          connectAttempts += 1;
          return target.connect();
        };
      }

      return Reflect.get(target, property, target);
    },
  }) as Pool;

  const checkout = createKeyedMutexCheckout(observedPool, {
    async afterAvailabilityRead() {
      seamVisits += 1;

      if (seamVisits === 1) {
        firstAtAvailabilityRead.release();
        await releaseFirstCaller.promise;
      } else if (seamVisits === 2) {
        secondAtAvailabilityRead.release();
      } else {
        assert.fail("the availability seam was reached more than twice");
      }
    },
  });

  try {
    const locker = await database.pool.query<{ id: number }>(
      "INSERT INTO lockers (label) VALUES ($1) RETURNING id",
      [`queueing-${randomUUID()}`],
    );
    const lockerId = locker.rows[0]?.id;
    assert.ok(lockerId !== undefined);

    attemptA = checkout("user-a", lockerId);
    await waitForSeam(firstAtAvailabilityRead, attemptA);

    attemptB = checkout("user-b", lockerId);
    await drainEventLoop();

    assert.equal(
      seamVisits,
      1,
      "caller B must not reach the seam before A exits",
    );
    assert.equal(
      connectAttempts,
      1,
      "caller B must remain queued before requesting a database connection",
    );

    releaseFirstCaller.release();
    await waitForSeam(secondAtAvailabilityRead, attemptB);

    const [resultA, resultB] = await Promise.all([attemptA, attemptB]);
    assert.equal(resultA.outcome, "checked_out");
    assert.equal(resultB.outcome, "unavailable");
    assert.equal(seamVisits, 2);
  } finally {
    // Resolving an already-resolved Promise is harmless. This also prevents a
    // failed assertion from leaving caller A and its PG connection stranded.
    releaseFirstCaller.release();
    await Promise.allSettled(
      [attemptA, attemptB].filter(
        (attempt): attempt is Promise<CheckoutResult> => attempt !== undefined,
      ),
    );
    await database.close();
  }
});

it("does not block concurrent keyed-mutex checkouts for different lockers", async () => {
  // One connection per caller: both must be in flight at the same time, and
  // neither may wait on the pool for the other to finish.
  const database = await createIsolatedTestDatabase({ maxConnections: 2 });
  const firstAtAvailabilityRead = createBarrier();
  const releaseFirstCaller = createBarrier();
  const secondAtAvailabilityRead = createBarrier();
  let seamVisits = 0;
  let attemptA: Promise<CheckoutResult> | undefined;
  let attemptB: Promise<CheckoutResult> | undefined;

  const checkout = createKeyedMutexCheckout(database.pool, {
    async afterAvailabilityRead() {
      seamVisits += 1;

      if (seamVisits === 1) {
        firstAtAvailabilityRead.release();
        await releaseFirstCaller.promise;
      } else if (seamVisits === 2) {
        secondAtAvailabilityRead.release();
      } else {
        assert.fail("the availability seam was reached more than twice");
      }
    },
  });

  try {
    const lockers = await database.pool.query<{ id: number }>(
      "INSERT INTO lockers (label) VALUES ($1), ($2) RETURNING id",
      [`keying-a-${randomUUID()}`, `keying-b-${randomUUID()}`],
    );
    const firstLocker = lockers.rows[0]?.id;
    const secondLocker = lockers.rows[1]?.id;
    assert.ok(firstLocker !== undefined && secondLocker !== undefined);

    attemptA = checkout("user-a", firstLocker);
    await waitForSeam(firstAtAvailabilityRead, attemptA);

    // A remains inside its locker's critical section. Because B uses another
    // locker, it must reach the seam without waiting for A to be released.
    attemptB = checkout("user-b", secondLocker);
    await waitForSeam(secondAtAvailabilityRead, attemptB);
    assert.equal(seamVisits, 2);

    releaseFirstCaller.release();
    const [resultA, resultB] = await Promise.all([attemptA, attemptB]);
    assert.equal(resultA.outcome, "checked_out");
    assert.equal(resultB.outcome, "checked_out");
  } finally {
    releaseFirstCaller.release();
    await Promise.allSettled(
      [attemptA, attemptB].filter(
        (attempt): attempt is Promise<CheckoutResult> => attempt !== undefined,
      ),
    );
    await database.close();
  }
});
