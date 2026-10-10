import { randomUUID } from "node:crypto";
import type { Redis } from "ioredis";
import type { Pool } from "pg";
import type { Checkout } from "../checkout.ts";

const DEFAULT_LOCK_TTL_MS = 5_000;

const ACQUIRE_LOCK_SCRIPT = `
  local acquired = redis.call(
    "SET",
    KEYS[1],
    ARGV[1],
    "NX",
    "PX",
    ARGV[2]
  )

  if not acquired then
    return nil
  end

  local fencingToken = redis.pcall("INCR", KEYS[2])
  if type(fencingToken) == "table" and fencingToken.err then
    redis.call("DEL", KEYS[1])
    return redis.error_reply(fencingToken.err)
  end

  return fencingToken
`;

const RELEASE_LOCK_SCRIPT = `
  if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
  end

  return 0
`;

export interface RedisLock {
  ownerToken: string;
  fencingToken: number;
}

export interface RedisLockOptions {
  lockNamespace: string;
  resourceId: number;
  ttlMs: number;
}

export interface RedisFencingCheckoutOptions {
  lockNamespace: string;
  lockTtlMs?: number;
  afterLockAcquired?(lock: Readonly<RedisLock>): Promise<void>;
}

interface RedisLockKeys {
  lockKey: string;
  fencingTokenKey: string;
}

function assertLockNamespace(lockNamespace: string): void {
  if (lockNamespace.length === 0) {
    throw new RangeError("lockNamespace must not be empty");
  }
}

function assertLockTtl(ttlMs: number): void {
  if (!Number.isSafeInteger(ttlMs) || ttlMs <= 0) {
    throw new RangeError("lockTtlMs must be a positive safe integer");
  }
}

function resourceKeyPrefix(lockNamespace: string): string {
  return `redis-fencing:{${lockNamespace}:locker:`;
}

/** Match every lock and persistent fencing-token key in one namespace. */
export function redisFencingNamespaceKeyPattern(
  lockNamespace: string,
): string {
  assertLockNamespace(lockNamespace);
  return `${resourceKeyPrefix(lockNamespace)}*}:*`;
}

function lockKeys(
  lockNamespace: string,
  resourceId: number,
): RedisLockKeys {
  // The braces form a Redis Cluster hash tag. Both keys therefore occupy the
  // same hash slot, which allows the acquisition script to use them together.
  const resource = `${resourceKeyPrefix(lockNamespace)}${resourceId}}`;
  return {
    lockKey: `${resource}:lock`,
    fencingTokenKey: `${resource}:token`,
  };
}

/**
 * Attempt to acquire one expiring Redis lock and allocate its fencing token.
 * A null result means another owner currently holds the lock.
 */
export async function acquireRedisLock(
  redis: Redis,
  options: RedisLockOptions,
): Promise<RedisLock | null> {
  assertLockNamespace(options.lockNamespace);
  assertLockTtl(options.ttlMs);

  if (!Number.isSafeInteger(options.resourceId)) {
    throw new RangeError("resourceId must be a safe integer");
  }

  const ownerToken = randomUUID();
  const keys = lockKeys(options.lockNamespace, options.resourceId);
  const result: unknown = await redis.eval(
    ACQUIRE_LOCK_SCRIPT,
    2,
    keys.lockKey,
    keys.fencingTokenKey,
    ownerToken,
    options.ttlMs,
  );

  if (result === null) return null;
  if (
    typeof result !== "number" ||
    !Number.isSafeInteger(result) ||
    result <= 0
  ) {
    throw new Error(
      `Redis returned an invalid fencing token: ${String(result)}`,
    );
  }

  return { ownerToken, fencingToken: result };
}

/**
 * Release a lock only if Redis still contains this owner's random token.
 * The comparison and deletion share one Lua script so they are atomic.
 */
export async function releaseRedisLock(
  redis: Redis,
  options: Pick<RedisLockOptions, "lockNamespace" | "resourceId">,
  lock: RedisLock,
): Promise<boolean> {
  assertLockNamespace(options.lockNamespace);
  if (!Number.isSafeInteger(options.resourceId)) {
    throw new RangeError("resourceId must be a safe integer");
  }

  const keys = lockKeys(options.lockNamespace, options.resourceId);
  const result: unknown = await redis.eval(
    RELEASE_LOCK_SCRIPT,
    1,
    keys.lockKey,
    lock.ownerToken,
  );

  if (result !== 0 && result !== 1) {
    throw new Error(
      `Redis returned an invalid lock-release result: ${String(result)}`,
    );
  }

  return result === 1;
}

export function createRedisFencingCheckout(
  pool: Pool,
  redis: Redis,
  options: RedisFencingCheckoutOptions,
): Checkout {
  assertLockNamespace(options.lockNamespace);
  const lockTtlMs = options.lockTtlMs ?? DEFAULT_LOCK_TTL_MS;
  assertLockTtl(lockTtlMs);

  return async (userId, lockerId) => {
    const lockOptions = {
      lockNamespace: options.lockNamespace,
      resourceId: lockerId,
      ttlMs: lockTtlMs,
    };
    const lock = await acquireRedisLock(redis, lockOptions);
    if (lock === null) return { outcome: "unavailable" };

    try {
      await options.afterLockAcquired?.(lock);

      const client = await pool.connect();
      let transactionOpen = false;

      try {
        await client.query("BEGIN ISOLATION LEVEL READ COMMITTED");
        transactionOpen = true;

        // This UPDATE has two jobs. The fencing predicate rejects a lock owner
        // older than one PostgreSQL has already seen, while the row lock
        // serializes the availability read and insert that follow it.
        const accepted = await client.query(
          `
            UPDATE lockers
            SET last_fencing_token = $2
            WHERE id = $1 AND last_fencing_token < $2
          `,
          [lockerId, lock.fencingToken],
        );

        if (accepted.rowCount === 0) {
          const locker = await client.query<{ exists: boolean }>(
            "SELECT EXISTS (SELECT 1 FROM lockers WHERE id = $1) AS exists",
            [lockerId],
          );
          if (locker.rows[0]?.exists !== true) {
            throw new Error(`locker ${lockerId} does not exist`);
          }

          await client.query("COMMIT");
          transactionOpen = false;
          return { outcome: "unavailable" };
        }

        // This is a separate READ COMMITTED statement, so a caller that waited
        // for the locker row sees the preceding holder's committed checkout.
        const availability = await client.query<{ available: boolean }>(
          `
            SELECT NOT EXISTS (
              SELECT 1
              FROM checkouts
              WHERE locker_id = $1 AND released_at IS NULL
            ) AS available
          `,
          [lockerId],
        );

        if (availability.rows[0]?.available !== true) {
          await client.query("COMMIT");
          transactionOpen = false;
          return { outcome: "unavailable" };
        }

        const inserted = await client.query<{ id: number }>(
          `
            INSERT INTO checkouts (locker_id, user_id)
            VALUES ($1, $2)
            RETURNING id
          `,
          [lockerId, userId],
        );
        const checkoutId = inserted.rows[0]?.id;
        if (checkoutId === undefined) {
          throw new Error("checkout insert did not return an id");
        }

        await client.query("COMMIT");
        transactionOpen = false;
        return { outcome: "checked_out", checkoutId };
      } catch (error) {
        if (transactionOpen) await client.query("ROLLBACK");
        throw error;
      } finally {
        client.release();
      }
    } finally {
      // If the TTL elapsed or another owner replaced this lock, the Lua
      // script returns false and deliberately leaves the newer lock intact.
      await releaseRedisLock(redis, lockOptions, lock);
    }
  };
}
