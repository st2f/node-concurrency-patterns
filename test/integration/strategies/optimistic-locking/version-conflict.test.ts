import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { it } from "vitest";
import type { CheckoutResult } from "../../../../src/checkout.ts";
import {
  createOptimisticLockingCheckout,
  type OptimisticLockingVersionRead,
} from "../../../../src/strategies/optimistic-locking.ts";
import { createIsolatedTestDatabase } from "../../../support/database.ts";
import {
  createTestSignal,
  waitForSignal,
} from "../../../support/orchestration.ts";
import { promiseState } from "../../../support/trace.ts";

it("allows only one writer to update when both read the same version", async () => {
  // A and B each hold a strategy connection while the third connection
  // verifies the database state at the orchestration boundary.
  const database = await createIsolatedTestDatabase({ maxConnections: 3 });
  const bothReadInitialVersion = createTestSignal();
  const releaseInitialReaders = createTestSignal();
  const versionReads: OptimisticLockingVersionRead[] = [];
  let attemptA: Promise<CheckoutResult> | undefined;
  let attemptB: Promise<CheckoutResult> | undefined;

  const checkout = createOptimisticLockingCheckout(database.pool, {
    async afterVersionRead(state) {
      versionReads.push({ ...state });

      // Park both callers after their initial read and before either
      // conditional UPDATE. After release, one UPDATE wins and advances
      // the version. The loser's UPDATE matches no row, so it retries and
      // reaches this hook again. It must not wait on the initial-read barrier.
      if (versionReads.length <= 2) {
        assert.deepEqual(state, { version: 0, available: true });
        if (versionReads.length === 2) bothReadInitialVersion.release();
        await releaseInitialReaders.promise;
      } else if (versionReads.length === 3) {
        assert.deepEqual(state, { version: 1, available: false });
      } else {
        assert.fail("the version-read seam was reached more than three times");
      }
    },
  });

  try {
    const locker = await database.pool.query<{ id: number }>(
      "INSERT INTO lockers (label) VALUES ($1) RETURNING id",
      [`version-conflict-${randomUUID()}`],
    );
    const lockerId = locker.rows[0]?.id;
    assert.ok(lockerId !== undefined);

    attemptA = checkout("user-a", lockerId);
    attemptB = checkout("user-b", lockerId);
    await waitForSignal(
      bothReadInitialVersion,
      [attemptA, attemptB],
      "both callers to read the initial version",
    );

    assert.deepEqual(versionReads, [
      { version: 0, available: true },
      { version: 0, available: true },
    ]);
    assert.equal(promiseState(attemptA), "pending");
    assert.equal(promiseState(attemptB), "pending");

    const beforeUpdates = await database.pool.query<{
      version: number;
      active_checkouts: number;
    }>(
      `
        SELECT
          lockers.version,
          COUNT(checkouts.id)::integer AS active_checkouts
        FROM lockers
        LEFT JOIN checkouts
          ON checkouts.locker_id = lockers.id
          AND checkouts.released_at IS NULL
        WHERE lockers.id = $1
        GROUP BY lockers.id
      `,
      [lockerId],
    );
    assert.deepEqual(beforeUpdates.rows[0], {
      version: 0,
      active_checkouts: 0,
    });

    releaseInitialReaders.release();
    const [resultA, resultB] = await Promise.all([attemptA, attemptB]);
    assert.deepEqual([resultA.outcome, resultB.outcome].sort(), [
      "checked_out",
      "unavailable",
    ]);

    // Only the winner increments the version. The third seam visit proves the
    // loser's zero-row UPDATE sent it around the READ COMMITTED retry loop.
    assert.deepEqual(versionReads, [
      { version: 0, available: true },
      { version: 0, available: true },
      { version: 1, available: false },
    ]);

    const afterUpdates = await database.pool.query<{
      version: number;
      active_checkouts: number;
    }>(
      `
        SELECT
          lockers.version,
          COUNT(checkouts.id)::integer AS active_checkouts
        FROM lockers
        LEFT JOIN checkouts
          ON checkouts.locker_id = lockers.id
          AND checkouts.released_at IS NULL
        WHERE lockers.id = $1
        GROUP BY lockers.id
      `,
      [lockerId],
    );
    assert.deepEqual(afterUpdates.rows[0], {
      version: 1,
      active_checkouts: 1,
    });
  } finally {
    releaseInitialReaders.release();
    await Promise.allSettled(
      [attemptA, attemptB].filter(
        (attempt): attempt is Promise<CheckoutResult> => attempt !== undefined,
      ),
    );
    await database.close();
  }
});
