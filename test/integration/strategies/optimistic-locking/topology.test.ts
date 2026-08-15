import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { it } from "vitest";
import { env } from "../../../../src/env.ts";
import {
  spawnCheckoutWorker,
  type CheckoutWorker,
  type CheckoutWorkerAttempt,
} from "../../../support/checkout-worker.ts";
import { createIsolatedTestDatabase } from "../../../support/database.ts";
import { promiseState } from "../../../support/trace.ts";

it("allows exactly one checkout to win across application processes", async () => {
  const database = await createIsolatedTestDatabase({ maxConnections: 1 });
  let workerA: CheckoutWorker | undefined;
  let workerB: CheckoutWorker | undefined;
  let attemptA: CheckoutWorkerAttempt | undefined;
  let attemptB: CheckoutWorkerAttempt | undefined;

  try {
    const locker = await database.pool.query<{ id: number }>(
      "INSERT INTO lockers (label) VALUES ($1) RETURNING id",
      [`optimistic-topology-${randomUUID()}`],
    );
    const lockerId = locker.rows[0]?.id;
    assert.ok(lockerId !== undefined);

    const workerOptions = {
      strategy: "optimistic-locking" as const,
      namespace: database.namespace,
      postgres: {
        ...env.pg,
        options: `-c search_path=${database.namespace}`,
      },
      orchestration: "pause-after-version-read" as const,
    };
    workerA = await spawnCheckoutWorker(workerOptions);
    workerB = await spawnCheckoutWorker(workerOptions);

    assert.notEqual(workerA.pid, process.pid);
    assert.notEqual(workerB.pid, process.pid);
    assert.notEqual(workerA.pid, workerB.pid);

    attemptA = workerA.startCheckout("user-a", lockerId);
    attemptB = workerB.startCheckout("user-b", lockerId);

    // Neither process is released until both have read the same initial
    // database state. Their separate PIDs prove that no JavaScript memory is
    // shared between the two strategy instances.
    await Promise.all([
      attemptA.atOrchestrationSeam,
      attemptB.atOrchestrationSeam,
    ]);
    assert.equal(promiseState(attemptA.result), "pending");
    assert.equal(promiseState(attemptB.result), "pending");

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

    attemptA.releaseOrchestrationSeam();
    attemptB.releaseOrchestrationSeam();

    const [resultA, resultB] = await Promise.all([
      attemptA.result,
      attemptB.result,
    ]);
    assert.deepEqual([resultA.outcome, resultB.outcome].sort(), [
      "checked_out",
      "unavailable",
    ]);

    const winner =
      resultA.outcome === "checked_out"
        ? { checkoutId: resultA.checkoutId, userId: "user-a" }
        : resultB.outcome === "checked_out"
          ? { checkoutId: resultB.checkoutId, userId: "user-b" }
          : assert.fail("one checkout attempt should win");

    const afterUpdates = await database.pool.query<{
      version: number;
      checkout_id: number;
      user_id: string;
    }>(
      `
        SELECT
          lockers.version,
          checkouts.id AS checkout_id,
          checkouts.user_id
        FROM lockers
        JOIN checkouts
          ON checkouts.locker_id = lockers.id
          AND checkouts.released_at IS NULL
        WHERE lockers.id = $1
      `,
      [lockerId],
    );
    assert.equal(afterUpdates.rowCount, 1);
    assert.deepEqual(afterUpdates.rows[0], {
      version: 1,
      checkout_id: winner.checkoutId,
      user_id: winner.userId,
    });
  } finally {
    // Release messages are buffered by the worker, so cleanup is safe even if
    // an assertion fails before an attempt reaches its seam.
    attemptA?.releaseOrchestrationSeam();
    attemptB?.releaseOrchestrationSeam();
    await Promise.allSettled(
      [attemptA, attemptB]
        .filter(
          (attempt): attempt is CheckoutWorkerAttempt => attempt !== undefined,
        )
        .map((attempt) => attempt.result),
    );
    await Promise.allSettled(
      [workerA, workerB]
        .filter((worker): worker is CheckoutWorker => worker !== undefined)
        .map((worker) => worker.close()),
    );
    await database.close();
  }
});
