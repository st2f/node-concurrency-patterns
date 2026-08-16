import { randomUUID } from "node:crypto";
import type { Redis } from "ioredis";
import type { Pool } from "pg";
import type { Checkout } from "../checkout.ts";

const DEFAULT_LEASE_TTL_MS = 5_000;

const ACQUIRE_LEASE_SCRIPT = `
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

const RELEASE_LEASE_SCRIPT = `
  if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
  end

  return 0
`;

export interface RedisLease {
  ownerToken: string;
  fencingToken: number;
}

export interface RedisLeaseOptions {
  lockNamespace: string;
  resourceId: number;
  ttlMs: number;
}

export interface RedisFencingCheckoutOptions {
  lockNamespace: string;
  leaseTtlMs?: number;
  afterLeaseAcquired?(lease: Readonly<RedisLease>): Promise<void>;
}

interface RedisLeaseKeys {
  leaseKey: string;
  fencingTokenKey: string;
}

function assertLockNamespace(lockNamespace: string): void {
  if (lockNamespace.length === 0) {
    throw new RangeError("lockNamespace must not be empty");
  }
}

function assertLeaseTtl(ttlMs: number): void {
  if (!Number.isSafeInteger(ttlMs) || ttlMs <= 0) {
    throw new RangeError("leaseTtlMs must be a positive safe integer");
  }
}

function resourceKeyPrefix(lockNamespace: string): string {
  return `redis-fencing:{${lockNamespace}:locker:`;
}

/** Match every lease and persistent fencing-token key in one namespace. */
export function redisFencingNamespaceKeyPattern(
  lockNamespace: string,
): string {
  assertLockNamespace(lockNamespace);
  return `${resourceKeyPrefix(lockNamespace)}*}:*`;
}

function leaseKeys(
  lockNamespace: string,
  resourceId: number,
): RedisLeaseKeys {
  // The braces form a Redis Cluster hash tag. Both keys therefore occupy the
  // same hash slot, which allows the acquisition script to use them together.
  const resource = `${resourceKeyPrefix(lockNamespace)}${resourceId}}`;
  return {
    leaseKey: `${resource}:lease`,
    fencingTokenKey: `${resource}:token`,
  };
}

/**
 * Attempt to acquire one expiring Redis lease and allocate its fencing token.
 * A null result means another owner currently holds the lease.
 */
export async function acquireRedisLease(
  redis: Redis,
  options: RedisLeaseOptions,
): Promise<RedisLease | null> {
  assertLockNamespace(options.lockNamespace);
  assertLeaseTtl(options.ttlMs);

  if (!Number.isSafeInteger(options.resourceId)) {
    throw new RangeError("resourceId must be a safe integer");
  }

  const ownerToken = randomUUID();
  const keys = leaseKeys(options.lockNamespace, options.resourceId);
  const result: unknown = await redis.eval(
    ACQUIRE_LEASE_SCRIPT,
    2,
    keys.leaseKey,
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
 * Release a lease only if Redis still contains this owner's random token.
 * The comparison and deletion share one Lua script so they are atomic.
 */
export async function releaseRedisLease(
  redis: Redis,
  options: Pick<RedisLeaseOptions, "lockNamespace" | "resourceId">,
  lease: RedisLease,
): Promise<boolean> {
  assertLockNamespace(options.lockNamespace);
  if (!Number.isSafeInteger(options.resourceId)) {
    throw new RangeError("resourceId must be a safe integer");
  }

  const keys = leaseKeys(options.lockNamespace, options.resourceId);
  const result: unknown = await redis.eval(
    RELEASE_LEASE_SCRIPT,
    1,
    keys.leaseKey,
    lease.ownerToken,
  );

  if (result !== 0 && result !== 1) {
    throw new Error(
      `Redis returned an invalid lease-release result: ${String(result)}`,
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
  const leaseTtlMs = options.leaseTtlMs ?? DEFAULT_LEASE_TTL_MS;
  assertLeaseTtl(leaseTtlMs);

  return async (userId, lockerId) => {
    const leaseOptions = {
      lockNamespace: options.lockNamespace,
      resourceId: lockerId,
      ttlMs: leaseTtlMs,
    };
    const lease = await acquireRedisLease(redis, leaseOptions);
    if (lease === null) return { outcome: "unavailable" };

    try {
      await options.afterLeaseAcquired?.(lease);

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
          [lockerId, lease.fencingToken],
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
      // If the TTL elapsed or another owner replaced this lease, the Lua
      // script returns false and deliberately leaves the newer lease intact.
      await releaseRedisLease(redis, leaseOptions, lease);
    }
  };
}
