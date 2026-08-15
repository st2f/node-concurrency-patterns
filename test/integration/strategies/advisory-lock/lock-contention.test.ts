import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { it } from "vitest";
import type { CheckoutResult } from "../../../../src/checkout.ts";
import { createAdvisoryLockCheckout } from "../../../../src/strategies/advisory-lock.ts";
import { waitForPendingLockRequest } from "../../../support/advisory-lock.ts";
import { createIsolatedTestDatabase } from "../../../support/database.ts";
import {
  createTestSignal,
  waitForSignal,
} from "../../../support/orchestration.ts";

it("makes a second connection wait for the same advisory lock", async () => {
  // A and B each hold a strategy connection. The third connection observes
  // their lock state without competing with them for pool capacity.
  const database = await createIsolatedTestDatabase({ maxConnections: 3 });
  const firstAcquiredLock = createTestSignal();
  const releaseFirstCaller = createTestSignal();
  const secondAcquiredLock = createTestSignal();
  let seamVisits = 0;
  let attemptA: Promise<CheckoutResult> | undefined;
  let attemptB: Promise<CheckoutResult> | undefined;

  const checkout = createAdvisoryLockCheckout(database.pool, {
    lockNamespace: database.namespace,
    async afterAdvisoryLockAcquired() {
      seamVisits += 1;

      if (seamVisits === 1) {
        firstAcquiredLock.release();
        await releaseFirstCaller.promise;
      } else if (seamVisits === 2) {
        secondAcquiredLock.release();
      } else {
        assert.fail("the advisory-lock seam was reached more than twice");
      }
    },
  });

  try {
    const locker = await database.pool.query<{ id: number }>(
      "INSERT INTO lockers (label) VALUES ($1) RETURNING id",
      [`lock-contention-${randomUUID()}`],
    );
    const lockerId = locker.rows[0]?.id;
    assert.ok(lockerId !== undefined);

    attemptA = checkout("user-a", lockerId);
    await waitForSignal(
      firstAcquiredLock,
      [attemptA],
      "caller A to acquire the advisory lock",
    );

    attemptB = checkout("user-b", lockerId);

    await waitForPendingLockRequest(database, lockerId);

    assert.equal(
      seamVisits,
      1,
      "caller B must not acquire the lock while caller A holds it",
    );

    releaseFirstCaller.release();
    await waitForSignal(
      secondAcquiredLock,
      [attemptB],
      "caller B to acquire the advisory lock",
    );

    const [resultA, resultB] = await Promise.all([attemptA, attemptB]);
    assert.equal(resultA.outcome, "checked_out");
    assert.equal(resultB.outcome, "unavailable");
    assert.equal(seamVisits, 2);
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
