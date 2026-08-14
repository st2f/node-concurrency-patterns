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
import { createTracer } from "../../../support/trace.ts";

/*
Scenario:

checkout messages sent
    ↓
seamA fulfilled
seamB fulfilled
    ↓
both processes parked
    ↓
release messages sent
    ↓
resultA fulfilled
resultB fulfilled
    ↓
both attempts finished
*/
it("allows both checkout attempts to win across application processes", async () => {
  const trace = createTracer("topology");
  const database = await createIsolatedTestDatabase({ maxConnections: 1 });
  let workerA: CheckoutWorker | undefined;
  let workerB: CheckoutWorker | undefined;
  let attemptA: CheckoutWorkerAttempt | undefined;
  let attemptB: CheckoutWorkerAttempt | undefined;

  try {
    const locker = await database.pool.query<{ id: number }>(
      "INSERT INTO lockers (label) VALUES ($1) RETURNING id",
      [`topology-${randomUUID()}`],
    );
    const lockerId = locker.rows[0]?.id;
    assert.ok(lockerId !== undefined);

    const workerOptions = {
      strategy: "keyed-mutex" as const,
      namespace: database.namespace,
      postgres: {
        ...env.pg,
        options: `-c search_path=${database.namespace}`,
      },
      orchestration: "pause-after-availability-read" as const,
    };
    // Assign each worker as soon as it starts so the first can still be closed
    // if starting the second one fails.
    workerA = await spawnCheckoutWorker(workerOptions);
    workerB = await spawnCheckoutWorker(workerOptions);

    assert.notEqual(workerA.pid, process.pid);
    assert.notEqual(workerB.pid, process.pid);
    assert.notEqual(workerA.pid, workerB.pid);

    attemptA = workerA.startCheckout("user-a", lockerId);
    attemptB = workerB.startCheckout("user-b", lockerId);

    const states = {
      seamA: attemptA.atAvailabilityRead,
      seamB: attemptB.atAvailabilityRead,
      resultA: attemptA.result,
      resultB: attemptB.result,
    };
    for (const [name, promise] of Object.entries(states)) {
      trace.watch(name, promise);
    }
    trace.mark("checkout messages sent", states);

    // Neither attempt is released until both processes report reaching the
    // seam. Therefore both are simultaneously inside critical sections owned
    // by different in-memory mutex instances.
    await Promise.all([
      attemptA.atAvailabilityRead,
      attemptB.atAvailabilityRead,
    ]);
    trace.mark("both processes parked at the seam", states);

    attemptA.releaseAvailabilityRead();
    attemptB.releaseAvailabilityRead();
    // Sending an IPC message settles nothing synchronously: both results are
    // still pending here, and stay pending until each child replies.
    trace.mark("release messages sent", states);

    const [resultA, resultB] = await Promise.all([
      attemptA.result,
      attemptB.result,
    ]);
    trace.mark("both attempts finished", states);

    assert.equal(resultA.outcome, "checked_out");
    assert.equal(resultB.outcome, "checked_out");
    assert.notEqual(resultA.checkoutId, resultB.checkoutId);

    const activeCheckouts = await database.pool.query<{ count: number }>(
      `
        SELECT COUNT(*)::integer AS count
        FROM checkouts
        WHERE locker_id = $1 AND released_at IS NULL
      `,
      [lockerId],
    );
    assert.equal(activeCheckouts.rows[0]?.count, 2);
  } finally {
    // Release messages are buffered by the worker, so cleanup is safe even if
    // an assertion fails before an attempt reaches its seam.
    attemptA?.releaseAvailabilityRead();
    attemptB?.releaseAvailabilityRead();
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
