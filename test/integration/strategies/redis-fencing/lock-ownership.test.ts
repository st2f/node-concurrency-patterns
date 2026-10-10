import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { it } from "vitest";
import {
  acquireRedisLock,
  releaseRedisLock,
} from "../../../../src/strategies/redis-fencing.ts";
import { createIsolatedTestRedis } from "../../../support/redis.ts";

it("prevents another owner from acquiring an unexpired lock", async () => {
  const namespace = `lock-contention-${randomUUID()}`;
  const ownerA = createIsolatedTestRedis(namespace);
  const ownerB = createIsolatedTestRedis(namespace);
  const options = {
    lockNamespace: namespace,
    resourceId: 1,
    // Longer than the test timeout: this proves contention without relying on
    // sleeps or racing a short lock's expiry.
    ttlMs: 60_000,
  };

  try {
    await Promise.all([ownerA.client.ping(), ownerB.client.ping()]);

    const lockA = await acquireRedisLock(ownerA.client, options);
    assert.ok(lockA !== null, "owner A must acquire the available lock");

    const lockB = await acquireRedisLock(ownerB.client, options);
    assert.equal(
      lockB,
      null,
      "owner B must not acquire the lock while owner A holds it",
    );
  } finally {
    try {
      await ownerA.close();
    } finally {
      await ownerB.close();
    }
  }
});

it("releases a lock only when the owner token matches", async () => {
  const namespace = `lock-release-${randomUUID()}`;
  const ownerA = createIsolatedTestRedis(namespace);
  const ownerB = createIsolatedTestRedis(namespace);
  const options = {
    lockNamespace: namespace,
    resourceId: 1,
    ttlMs: 60_000,
  };

  try {
    await Promise.all([ownerA.client.ping(), ownerB.client.ping()]);

    const lockA = await acquireRedisLock(ownerA.client, options);
    assert.ok(lockA !== null);
    assert.equal(
      await releaseRedisLock(ownerA.client, options, lockA),
      true,
      "owner A must be able to release its own lock",
    );

    const lockB = await acquireRedisLock(ownerB.client, options);
    assert.ok(lockB !== null, "owner B must acquire the released lock");

    // Reuse A's old handle after B takes ownership. Explicit release and
    // reacquisition create the owner mismatch without waiting for expiry.
    assert.equal(
      await releaseRedisLock(ownerA.client, options, lockA),
      false,
      "owner A's old token must not release owner B's lock",
    );
    assert.equal(
      await acquireRedisLock(ownerA.client, options),
      null,
      "owner B's lock must remain held after A's rejected release",
    );

    assert.equal(
      await releaseRedisLock(ownerB.client, options, lockB),
      true,
      "owner B must still be able to release its own lock",
    );
    assert.ok(
      (await acquireRedisLock(ownerA.client, options)) !== null,
      "the lock must become available after owner B releases it",
    );
  } finally {
    try {
      await ownerA.close();
    } finally {
      await ownerB.close();
    }
  }
});
