import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { it } from "vitest";
import type { CheckoutResult } from "../../../../src/checkout.ts";
import { createAdvisoryLockCheckout } from "../../../../src/strategies/advisory-lock.ts";
import { createIsolatedTestDatabase } from "../../../support/database.ts";

const WAIT_TIMEOUT_MS = 2_000;
const POLL_INTERVAL_MS = 10;

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

async function waitForBarrier(
  barrier: Barrier,
  attempt: Promise<CheckoutResult>,
): Promise<void> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const timedOut = new Promise<never>((_, reject) => {
    timeout = setTimeout(() => {
      reject(new Error(`barrier was not reached within ${WAIT_TIMEOUT_MS}ms`));
    }, WAIT_TIMEOUT_MS);
    timeout.unref();
  });

  const finishedBeforeBarrier = attempt.then(
    (result) => {
      throw new Error(
        `checkout finished with ${result.outcome} before reaching the barrier`,
      );
    },
    (error: unknown) => {
      throw error;
    },
  );

  try {
    await Promise.race([barrier.promise, finishedBeforeBarrier, timedOut]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    const timeout = setTimeout(resolve, milliseconds);
    timeout.unref();
  });
}

async function waitForAdvisoryLockWaiter(
  query: () => Promise<{ rowCount: number }>,
): Promise<void> {
  const deadline = Date.now() + WAIT_TIMEOUT_MS;

  while (Date.now() < deadline) {
    const result = await query();
    if (result.rowCount > 0) return;
    await delay(POLL_INTERVAL_MS);
  }

  assert.fail(`no advisory-lock waiter appeared within ${WAIT_TIMEOUT_MS}ms`);
}

it("makes a second connection wait for the same advisory lock", async () => {
  // A and B each hold a strategy connection. The third connection observes
  // their lock state without competing with them for pool capacity.
  const database = await createIsolatedTestDatabase({ maxConnections: 3 });
  const firstAcquiredLock = createBarrier();
  const releaseFirstCaller = createBarrier();
  const secondAcquiredLock = createBarrier();
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
    await waitForBarrier(firstAcquiredLock, attemptA);

    attemptB = checkout("user-b", lockerId);

    // A pending advisory-lock request exists only while a PostgreSQL connection
    // is actively waiting to acquire the lock. Seeing this row therefore proves
    // that B submitted its lock query and is blocked inside PostgreSQL, rather
    // than simply not having run yet.
    await waitForAdvisoryLockWaiter(async () => {
      const waiting = await database.pool.query(
        `
          SELECT 1
          FROM pg_locks
          WHERE locktype = 'advisory'
            AND granted = false
            AND classid = hashtext($1::text)::oid
            AND objid = $2::integer::oid
            AND objsubid = 2
        `,
        [database.namespace, lockerId],
      );
      return { rowCount: waiting.rowCount ?? 0 };
    });

    assert.equal(
      seamVisits,
      1,
      "caller B must not acquire the lock while caller A holds it",
    );

    releaseFirstCaller.release();
    await waitForBarrier(secondAcquiredLock, attemptB);

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
