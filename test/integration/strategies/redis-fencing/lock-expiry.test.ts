import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import type { Redis } from "ioredis";
import { it } from "vitest";
import {
  acquireRedisLock,
  type RedisLock,
  type RedisLockOptions,
} from "../../../../src/strategies/redis-fencing.ts";
import { createIsolatedTestDatabase } from "../../../support/database.ts";
import { pollUntil } from "../../../support/polling.ts";
import { createIsolatedTestRedis } from "../../../support/redis.ts";

async function acquireAfterExpiry(
  redis: Redis,
  options: RedisLockOptions,
): Promise<RedisLock> {
  let lock: RedisLock | null = null;
  await pollUntil(async () => {
    lock = await acquireRedisLock(redis, options);
    return lock !== null;
  }, "the expired lock to become available to another owner");
  assert.ok(lock !== null);
  return lock;
}

it("allows another owner to acquire the lock after it expires", async () => {
  const namespace = `lock-expiry-${randomUUID()}`;
  const ownerA = createIsolatedTestRedis(namespace);
  const ownerB = createIsolatedTestRedis(namespace);
  const options = { lockNamespace: namespace, resourceId: 1, ttlMs: 100 };

  try {
    await Promise.all([ownerA.client.ping(), ownerB.client.ping()]);
    const lockA = await acquireRedisLock(ownerA.client, options);
    assert.ok(lockA !== null);

    // Never release A's lock. Poll acquisition rather than guessing a sleep.
    const lockB = await acquireAfterExpiry(ownerB.client, {
      ...options,
      ttlMs: 60_000,
    });
    assert.notEqual(lockB.ownerToken, lockA.ownerToken);
    assert.equal(
      await acquireRedisLock(ownerA.client, options),
      null,
      "owner B must hold the replacement lock",
    );
  } finally {
    try {
      await ownerA.close();
    } finally {
      await ownerB.close();
    }
  }
});

it("demonstrates that a lock without fencing cannot stop an expired holder from writing", async () => {
  const database = await createIsolatedTestDatabase({ maxConnections: 2 });
  const ownerA = createIsolatedTestRedis(database.namespace);
  const ownerB = createIsolatedTestRedis(database.namespace);

  try {
    await Promise.all([ownerA.client.ping(), ownerB.client.ping()]);
    const locker = await database.pool.query<{ id: number }>(
      "INSERT INTO lockers (label) VALUES ($1) RETURNING id",
      [`expired-holder-${randomUUID()}`],
    );
    const lockerId = locker.rows[0]?.id;
    assert.ok(lockerId !== undefined);
    const options = {
      lockNamespace: database.namespace,
      resourceId: lockerId,
      ttlMs: 100,
    };
    const availabilitySql = `
      SELECT NOT EXISTS (
        SELECT 1 FROM checkouts
        WHERE locker_id = $1 AND released_at IS NULL
      ) AS available
    `;
    const insertSql = `
      INSERT INTO checkouts (locker_id, user_id) VALUES ($1, $2)
    `;

    // Model checkout guarded only by Redis. These SQL writes deliberately
    // omit the storage validation in createRedisFencingCheckout().
    const lockA = await acquireRedisLock(ownerA.client, options);
    assert.ok(lockA !== null);
    const availabilityA = await database.pool.query<{ available: boolean }>(
      availabilitySql,
      [lockerId],
    );
    assert.equal(availabilityA.rows[0]?.available, true);

    // Leave A paused after its read. B acquires after A's TTL expires, checks
    // availability, and commits its checkout before A resumes.
    await acquireAfterExpiry(ownerB.client, { ...options, ttlMs: 60_000 });
    const availabilityB = await database.pool.query<{ available: boolean }>(
      availabilitySql,
      [lockerId],
    );
    assert.equal(availabilityB.rows[0]?.available, true);
    await database.pool.query(insertSql, [lockerId, "user-b"]);

    // A resumes with its earlier decision. Expiry did not revoke its ability
    // to write to PostgreSQL, even while B owns the replacement Redis lock.
    await database.pool.query(insertSql, [lockerId, "user-a"]);
    const active = await database.pool.query<{ user_id: string }>(
      `SELECT user_id FROM checkouts
       WHERE locker_id = $1 AND released_at IS NULL ORDER BY id`,
      [lockerId],
    );
    assert.deepEqual(
      active.rows.map((row) => row.user_id),
      ["user-b", "user-a"],
      "the expired holder must demonstrate the unprotected duplicate checkout",
    );
    assert.equal(
      await acquireRedisLock(ownerA.client, options),
      null,
      "A's stale write must occur while B still owns the replacement lock",
    );
  } finally {
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
