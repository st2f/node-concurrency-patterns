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
import { createIsolatedTestRedis } from "../../../support/redis.ts";
import { promiseState } from "../../../support/trace.ts";

it("allows exactly one checkout to win across application processes", async () => {
  const database = await createIsolatedTestDatabase({ maxConnections: 1 });
  // Workers own their connections; this client removes their namespace keys.
  const redis = createIsolatedTestRedis(database.namespace);
  let workerA: CheckoutWorker | undefined;
  let workerB: CheckoutWorker | undefined;
  let attemptA: CheckoutWorkerAttempt | undefined;
  let attemptB: CheckoutWorkerAttempt | undefined;

  try {
    const locker = await database.pool.query<{ id: number }>(
      "INSERT INTO lockers (label) VALUES ($1) RETURNING id",
      [`redis-topology-${randomUUID()}`],
    );
    const lockerId = locker.rows[0]?.id;
    assert.ok(lockerId !== undefined);
    const workerOptions = {
      strategy: "redis-fencing" as const,
      namespace: database.namespace,
      postgres: {
        ...env.pg,
        options: `-c search_path=${database.namespace}`,
      },
      redis: env.redis,
      lockTtlMs: 60_000,
    };
    workerA = await spawnCheckoutWorker({
      ...workerOptions,
      orchestration: "pause-after-redis-lock-acquired",
    });
    workerB = await spawnCheckoutWorker(workerOptions);
    assert.notEqual(workerA.pid, process.pid);
    assert.notEqual(workerB.pid, process.pid);
    assert.notEqual(workerA.pid, workerB.pid);

    // Hold A before it writes. B must be excluded by Redis across processes,
    // rather than merely seeing an already committed checkout in PostgreSQL.
    attemptA = workerA.startCheckout("user-a", lockerId);
    await attemptA.atOrchestrationSeam;
    attemptB = workerB.startCheckout("user-b", lockerId);
    assert.deepEqual(await attemptB.result, { outcome: "unavailable" });
    assert.equal(promiseState(attemptA.result), "pending");

    const beforeWrite = await database.pool.query<{ count: number }>(
      `SELECT COUNT(*)::integer AS count FROM checkouts
       WHERE locker_id = $1 AND released_at IS NULL`,
      [lockerId],
    );
    assert.equal(beforeWrite.rows[0]?.count, 0);

    attemptA.releaseOrchestrationSeam();
    const resultA = await attemptA.result;
    assert.equal(resultA.outcome, "checked_out");
    const active = await database.pool.query<{ id: number; user_id: string }>(
      `SELECT id, user_id FROM checkouts
       WHERE locker_id = $1 AND released_at IS NULL`,
      [lockerId],
    );
    assert.deepEqual(active.rows, [{ id: resultA.checkoutId, user_id: "user-a" }]);
  } finally {
    attemptA?.releaseOrchestrationSeam();
    attemptB?.releaseOrchestrationSeam();
    await Promise.allSettled(
      [attemptA, attemptB]
        .filter((attempt): attempt is CheckoutWorkerAttempt => attempt !== undefined)
        .map((attempt) => attempt.result),
    );
    await Promise.allSettled(
      [workerA, workerB]
        .filter((worker): worker is CheckoutWorker => worker !== undefined)
        .map((worker) => worker.close()),
    );
    try {
      await redis.close();
    } finally {
      await database.close();
    }
  }
});
