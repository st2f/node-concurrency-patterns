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
import { applyUniqueConstraintMigration } from "../../../support/unique-constraint.ts";

/*
Two distinct Node.js child processes enter the strategy.
Both pause before inserting, with no active checkout yet.
PostgreSQL accepts exactly one insert.
The losing process returns unavailable.
The stored checkout matches the winning process.
*/
it("allows exactly one checkout to win across application processes", async () => {
  const database = await createIsolatedTestDatabase({ maxConnections: 1 });
  let workerA: CheckoutWorker | undefined;
  let workerB: CheckoutWorker | undefined;
  let attemptA: CheckoutWorkerAttempt | undefined;
  let attemptB: CheckoutWorkerAttempt | undefined;

  try {
    await applyUniqueConstraintMigration(database.pool);

    const locker = await database.pool.query<{ id: number }>(
      "INSERT INTO lockers (label) VALUES ($1) RETURNING id",
      [`unique-constraint-topology-${randomUUID()}`],
    );
    const lockerId = locker.rows[0]?.id;
    assert.ok(lockerId !== undefined);

    const workerOptions = {
      strategy: "unique-constraint" as const,
      namespace: database.namespace,
      postgres: {
        ...env.pg,
        options: `-c search_path=${database.namespace}`,
      },
      orchestration: "pause-before-insert" as const,
    };
    workerA = await spawnCheckoutWorker(workerOptions);
    workerB = await spawnCheckoutWorker(workerOptions);

    assert.notEqual(workerA.pid, process.pid);
    assert.notEqual(workerB.pid, process.pid);
    assert.notEqual(workerA.pid, workerB.pid);

    attemptA = workerA.startCheckout("user-a", lockerId);
    attemptB = workerB.startCheckout("user-b", lockerId);

    // Both independent processes have entered the strategy and are ready to
    // issue their INSERT before either is released. They cannot coordinate
    // through shared JavaScript memory because their PIDs are different.
    await Promise.all([
      attemptA.atOrchestrationSeam,
      attemptB.atOrchestrationSeam,
    ]);
    assert.equal(promiseState(attemptA.result), "pending");
    assert.equal(promiseState(attemptB.result), "pending");

    const beforeInserts = await database.pool.query<{ count: number }>(
      `
        SELECT COUNT(*)::integer AS count
        FROM checkouts
        WHERE locker_id = $1 AND released_at IS NULL
      `,
      [lockerId],
    );
    assert.equal(beforeInserts.rows[0]?.count, 0);

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

    const active = await database.pool.query<{
      id: number;
      user_id: string;
    }>(
      `
        SELECT id, user_id
        FROM checkouts
        WHERE locker_id = $1 AND released_at IS NULL
      `,
      [lockerId],
    );
    assert.equal(active.rowCount, 1);
    assert.deepEqual(active.rows[0], {
      id: winner.checkoutId,
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
