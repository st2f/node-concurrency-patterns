import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { it } from "vitest";
import { env } from "../../../../src/env.ts";
import {
  spawnCheckoutWorker,
  type CheckoutWorker,
  type CheckoutWorkerAttempt,
} from "../../../support/checkout-worker.ts";
import { waitForPendingLockRequest } from "../../../support/advisory-lock.ts";
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
      [`advisory-topology-${randomUUID()}`],
    );
    const lockerId = locker.rows[0]?.id;
    assert.ok(lockerId !== undefined);

    const workerOptions = {
      strategy: "advisory-lock" as const,
      namespace: database.namespace,
      postgres: {
        ...env.pg,
        options: `-c search_path=${database.namespace}`,
      },
      orchestration: "pause-after-advisory-lock-acquired" as const,
    };
    workerA = await spawnCheckoutWorker(workerOptions);
    workerB = await spawnCheckoutWorker(workerOptions);

    assert.notEqual(workerA.pid, process.pid);
    assert.notEqual(workerB.pid, process.pid);
    assert.notEqual(workerA.pid, workerB.pid);

    // Establish A as the lock holder before B starts. This makes the expected
    // winner deterministic while the separate PIDs establish the topology.
    attemptA = workerA.startCheckout("user-a", lockerId);
    await attemptA.atOrchestrationSeam;
    assert.equal(promiseState(attemptA.result), "pending");

    attemptB = workerB.startCheckout("user-b", lockerId);

    await waitForPendingLockRequest(database, lockerId);

    assert.equal(promiseState(attemptA.result), "pending");
    assert.equal(promiseState(attemptB.atOrchestrationSeam), "pending");
    assert.equal(promiseState(attemptB.result), "pending");

    attemptA.releaseOrchestrationSeam();
    const [resultA] = await Promise.all([
      attemptA.result,
      attemptB.atOrchestrationSeam,
    ]);
    assert.equal(resultA.outcome, "checked_out");

    // B has now acquired the same lock after A's commit. Its READ COMMITTED
    // availability query runs only after this release and must see A's row.
    attemptB.releaseOrchestrationSeam();
    const resultB = await attemptB.result;
    assert.equal(resultB.outcome, "unavailable");

    const activeCheckouts = await database.pool.query<{
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
    assert.equal(activeCheckouts.rowCount, 1);
    assert.deepEqual(activeCheckouts.rows[0], {
      id: resultA.checkoutId,
      user_id: "user-a",
    });
  } finally {
    // Release messages are buffered by the worker, so these calls are safe
    // even when cleanup begins before an attempt reaches its seam.
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
