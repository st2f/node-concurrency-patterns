import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import type { Pool } from "pg";
import { test } from "vitest";
import type { Checkout, CheckoutResult } from "../../src/checkout.ts";
import { createIsolatedTestDatabase } from "./database.ts";

export interface CheckoutContractSubject {
  checkout: Checkout;
  close?(): Promise<void>;
}

export interface CheckoutContractContext {
  /** Prefix database-wide and external coordination keys with this value. */
  namespace: string;
}

export interface CheckoutContractOptions {
  name: string;
  createSubject(pool: Pool, context: CheckoutContractContext):
    | CheckoutContractSubject
    | Promise<CheckoutContractSubject>;
  prepareDatabase?(pool: Pool): Promise<void>;
  attemptCount?: number;
}

interface SettledAttempt {
  userId: string;
  result: CheckoutResult;
}

interface WinningAttempt extends SettledAttempt {
  result: Extract<CheckoutResult, { outcome: "checked_out" }>;
}

/**
 * Fill the pool with idle connections before any attempt runs.
 *
 * A cold pool hides races. Every `pool.connect()` would pay a TCP and auth
 * round trip, and those resolve far enough apart that each attempt finishes its
 * whole transaction before the next one connects. An entirely unprotected
 * checkout then serializes by accident and satisfies this contract. Warming the
 * pool lets `connect()` resolve from the idle list, so the attempts overlap.
 */
async function warmConnections(pool: Pool, count: number): Promise<void> {
  const clients = await Promise.all(
    Array.from({ length: count }, () => pool.connect()),
  );
  for (const client of clients) client.release();
}

/** Register the shared Step 1b black-box contract for a checkout strategy. */
export function checkoutContract(options: CheckoutContractOptions): void {
  test(`${options.name}: concurrent checkout contract`, async () => {
    const attemptCount = options.attemptCount ?? 16;
    assert.ok(
      Number.isInteger(attemptCount) && attemptCount >= 2,
      "attemptCount must be an integer of at least two",
    );

    // Allow every attempt to hold one PG connection while requesting another.
    // Making this relationship explicit avoids accidental pool starvation.
    const connectionCount = attemptCount * 2;
    const database = await createIsolatedTestDatabase({
      maxConnections: connectionCount,
    });
    let subject: CheckoutContractSubject | undefined;

    try {
      await options.prepareDatabase?.(database.pool);
      subject = await options.createSubject(database.pool, {
        namespace: database.namespace,
      });
      const checkout = subject.checkout;

      const locker = await database.pool.query<{ id: number }>(
        "INSERT INTO lockers (label) VALUES ($1) RETURNING id",
        [`step-1b-${randomUUID()}`],
      );
      const lockerId = locker.rows[0]?.id;
      assert.ok(lockerId !== undefined);

      await warmConnections(database.pool, connectionCount);

      let releaseStart: (() => void) | undefined;
      const start = new Promise<void>((resolve) => {
        releaseStart = resolve;
      });

      // The barrier plus the warm pool make real overlap likely: every attempt
      // is released once the others exist, and none of them stalls on a new
      // connection. Overlap is still probabilistic, so this contract cannot
      // prove contention on its own; the deterministic mechanism tests control
      // the boundary that matters to each strategy.
      const attempts = Array.from({ length: attemptCount }, (_, index) => {
        const userId = `user-${index + 1}`;
        return (async (): Promise<SettledAttempt> => {
          await start;
          return {
            userId,
            result: await checkout(userId, lockerId),
          };
        })();
      });

      releaseStart?.();
      const settled = await Promise.allSettled(attempts);
      const failures = settled
        .filter((result) => result.status === "rejected")
        .map((result) => String(result.reason));

      assert.deepEqual(
        failures,
        [],
        "contention is an expected outcome and must not reject checkout()",
      );

      const results = settled
        .filter((result) => result.status === "fulfilled")
        .map((result) => result.value);
      const winners = results.filter(
        (attempt): attempt is WinningAttempt =>
          attempt.result.outcome === "checked_out",
      );
      const unavailable = results.filter(
        (attempt) => attempt.result.outcome === "unavailable",
      );

      assert.equal(winners.length, 1, "exactly one call should win");
      assert.equal(
        unavailable.length,
        attemptCount - 1,
        "every losing call should return unavailable",
      );

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

      assert.equal(active.rowCount, 1, "exactly one checkout should be active");

      const winner = winners[0];
      assert.ok(winner !== undefined);
      assert.deepEqual(active.rows[0], {
        id: winner.result.checkoutId,
        user_id: winner.userId,
      });
    } finally {
      try {
        await subject?.close?.();
      } finally {
        await database.close();
      }
    }
  });
}
