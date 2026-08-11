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

export interface CheckoutContractOptions {
  name: string;
  createSubject(pool: Pool):
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

/** Register the shared Step 1b black-box contract for a checkout strategy. */
export function checkoutContract(options: CheckoutContractOptions): void {
  test(`${options.name}: concurrent checkout contract`, async () => {
    const attemptCount = options.attemptCount ?? 16;
    assert.ok(attemptCount >= 2, "the contract requires concurrent contention");

    const database = await createIsolatedTestDatabase();
    let subject: CheckoutContractSubject | undefined;

    try {
      await options.prepareDatabase?.(database.pool);
      subject = await options.createSubject(database.pool);
      const checkout = subject.checkout;

      const locker = await database.pool.query<{ id: number }>(
        "INSERT INTO lockers (label) VALUES ($1) RETURNING id",
        [`step-1b-${randomUUID()}`],
      );
      const lockerId = locker.rows[0]?.id;
      assert.ok(lockerId !== undefined);

      let releaseStart: (() => void) | undefined;
      const start = new Promise<void>((resolve) => {
        releaseStart = resolve;
      });

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
