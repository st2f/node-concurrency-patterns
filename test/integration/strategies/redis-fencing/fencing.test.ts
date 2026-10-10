import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { it } from "vitest";
import type { CheckoutResult } from "../../../../src/checkout.ts";
import {
  acquireRedisLock,
  createRedisFencingCheckout,
  releaseRedisLock,
  type RedisLock,
} from "../../../../src/strategies/redis-fencing.ts";
import { createIsolatedTestDatabase } from "../../../support/database.ts";
import {
  createTestSignal,
  waitForSignal,
} from "../../../support/orchestration.ts";
import { pollUntil } from "../../../support/polling.ts";
import { createIsolatedTestRedis } from "../../../support/redis.ts";

it("issues increasing fencing tokens to successive lock owners", async () => {
  const namespace = `fencing-tokens-${randomUUID()}`;
  const ownerA = createIsolatedTestRedis(namespace);
  const ownerB = createIsolatedTestRedis(namespace);
  const options = { lockNamespace: namespace, resourceId: 1, ttlMs: 60_000 };

  try {
    await Promise.all([ownerA.client.ping(), ownerB.client.ping()]);
    const first = await acquireRedisLock(ownerA.client, options);
    assert.ok(first !== null);
    assert.equal(await releaseRedisLock(ownerA.client, options, first), true);

    const second = await acquireRedisLock(ownerB.client, options);
    assert.ok(second !== null);
    assert.ok(second.fencingToken > first.fencingToken);
    assert.equal(await releaseRedisLock(ownerB.client, options, second), true);

    const third = await acquireRedisLock(ownerA.client, options);
    assert.ok(third !== null);
    assert.ok(
      third.fencingToken > second.fencingToken,
      "a returning owner must receive a token newer than the intervening owner's",
    );
  } finally {
    try {
      await ownerA.close();
    } finally {
      await ownerB.close();
    }
  }
});

it("rejects a stale write after a newer fencing token has been accepted", async () => {
  const database = await createIsolatedTestDatabase({ maxConnections: 2 });
  const ownerA = createIsolatedTestRedis(database.namespace);
  const ownerB = createIsolatedTestRedis(database.namespace);
  const firstAtLock = createTestSignal();
  const resumeFirstOwner = createTestSignal();
  let lockA: Readonly<RedisLock> | undefined;
  let lockB: Readonly<RedisLock> | undefined;
  let attemptA: Promise<CheckoutResult> | undefined;

  const checkoutA = createRedisFencingCheckout(database.pool, ownerA.client, {
    lockNamespace: database.namespace,
    lockTtlMs: 100,
    async afterLockAcquired(lock) {
      lockA = { ...lock };
      firstAtLock.release();
      await resumeFirstOwner.promise;
    },
  });
  const checkoutB = createRedisFencingCheckout(database.pool, ownerB.client, {
    lockNamespace: database.namespace,
    lockTtlMs: 60_000,
    async afterLockAcquired(lock) {
      lockB = { ...lock };
    },
  });

  try {
    await Promise.all([ownerA.client.ping(), ownerB.client.ping()]);
    const locker = await database.pool.query<{ id: number }>(
      "INSERT INTO lockers (label) VALUES ($1) RETURNING id",
      [`stale-fencing-${randomUUID()}`],
    );
    const lockerId = locker.rows[0]?.id;
    assert.ok(lockerId !== undefined);

    attemptA = checkoutA("user-a", lockerId);
    await waitForSignal(firstAtLock, [attemptA], "owner A to acquire its lock");

    // A remains paused. B can finish checkout only after A's Redis TTL expires.
    let resultB: CheckoutResult | undefined;
    await pollUntil(async () => {
      resultB = await checkoutB("user-b", lockerId);
      return resultB.outcome === "checked_out";
    }, "owner B to acquire the expired lock and commit its checkout");
    assert.ok(resultB?.outcome === "checked_out");
    assert.ok(lockA !== undefined && lockB !== undefined);
    assert.ok(lockB.fencingToken > lockA.fencingToken);

    const beforeResume = await database.pool.query<{
      last_fencing_token: string;
      user_id: string;
    }>(
      `SELECT lockers.last_fencing_token, checkouts.user_id
       FROM lockers JOIN checkouts ON checkouts.locker_id = lockers.id
       WHERE lockers.id = $1 AND checkouts.released_at IS NULL`,
      [lockerId],
    );
    assert.deepEqual(beforeResume.rows, [{
      last_fencing_token: String(lockB.fencingToken),
      user_id: "user-b",
    }]);

    // Make the locker available again while retaining B's accepted token.
    // Otherwise availability alone could reject A and hide broken fencing.
    await database.pool.query(
      "UPDATE checkouts SET released_at = now() WHERE id = $1",
      [resultB.checkoutId],
    );
    resumeFirstOwner.release();
    assert.deepEqual(await attemptA, { outcome: "unavailable" });

    const afterResume = await database.pool.query<{
      last_fencing_token: string;
      active_checkouts: number;
      total_checkouts: number;
    }>(
      `SELECT lockers.last_fencing_token,
         COUNT(checkouts.id) FILTER (WHERE checkouts.released_at IS NULL)::integer
           AS active_checkouts,
         COUNT(checkouts.id)::integer AS total_checkouts
       FROM lockers LEFT JOIN checkouts ON checkouts.locker_id = lockers.id
       WHERE lockers.id = $1 GROUP BY lockers.id`,
      [lockerId],
    );
    assert.deepEqual(afterResume.rows[0], {
      last_fencing_token: String(lockB.fencingToken),
      active_checkouts: 0,
      total_checkouts: 1,
    });
  } finally {
    // Always unblock A and drain its checkout before closing its clients.
    resumeFirstOwner.release();
    await Promise.allSettled(attemptA === undefined ? [] : [attemptA]);
    try {
      await ownerA.close();
    } finally {
      try {
        await ownerB.close();
      } finally {
        await database.close();
      }
    }
  }
});
