import assert from "node:assert/strict";
import { test, type TestContext } from "node:test";
import type { CheckoutOutcome, CheckoutStrategy } from "../../src/checkout.ts";
import { POOL_SIZE } from "../../src/db.ts";
import { countActiveCheckouts, createLocker, warmPool } from "./fixtures.ts";

/**
 * Kept at or below the pool size: a burst larger than the pool would queue on
 * `pool.connect()` and serialize itself, which would make a broken strategy
 * look correct for reasons that have nothing to do with its guard.
 */
export const DEFAULT_CONCURRENCY = 8;

export interface BurstResult {
  winners: Array<{ ok: true; checkoutId: number }>;
  /** Callers the strategy turned away cleanly, with the reason it reported. */
  rejections: Array<{ ok: false; reason: string }>;
  /** Callers whose `checkout()` threw. A strategy is expected to map its own
   * contention (a lost lock, a duplicate-key violation) to a rejection rather
   * than let it escape, so anything here is a contract failure. */
  errors: unknown[];
  /** Ground truth from the database, independent of what `checkout()` claimed. */
  activeCheckouts: number;
}

/**
 * Fire N simultaneous checkouts of the same locker through a strategy's real
 * `checkout()` and report both what the callers were told and what actually
 * landed in the database.
 */
export async function runCheckoutBurst(
  strategy: CheckoutStrategy,
  lockerId: number,
  concurrency = DEFAULT_CONCURRENCY,
): Promise<BurstResult> {
  await warmPool();

  const settled = await Promise.allSettled(
    Array.from({ length: concurrency }, (_, i) =>
      strategy.checkout(`user-${i}`, lockerId),
    ),
  );

  const winners: BurstResult["winners"] = [];
  const rejections: BurstResult["rejections"] = [];
  const errors: unknown[] = [];

  for (const result of settled) {
    if (result.status === "rejected") {
      errors.push(result.reason);
      continue;
    }

    const outcome: CheckoutOutcome = result.value;
    if (outcome.ok) winners.push(outcome);
    else rejections.push(outcome);
  }

  return {
    winners,
    rejections,
    errors,
    activeCheckouts: await countActiveCheckouts(lockerId),
  };
}

function describeBurst(result: BurstResult): string {
  return `winners=${result.winners.length} rejections=${result.rejections.length} errors=${result.errors.length} activeCheckouts=${result.activeCheckouts}`;
}

/**
 * The shared single-process contract from Step 1b. Every *protected* strategy
 * must satisfy it: exactly one caller is told it won, and exactly one active
 * checkout exists afterwards.
 *
 * This exercises the real guard code, but natural scheduling can accidentally
 * serialize a broken implementation — so passing here is necessary, not
 * sufficient. Steps 1c and 1d supply the deterministic proof.
 */
export function testCheckoutContract(options: {
  strategy: CheckoutStrategy;
  concurrency?: number;
}): void {
  const { strategy, concurrency = DEFAULT_CONCURRENCY } = options;

  assert.ok(
    concurrency <= POOL_SIZE,
    `concurrency ${concurrency} exceeds pool size ${POOL_SIZE}; the burst would queue instead of contend`,
  );

  test(`${strategy.name}: ${concurrency} concurrent checkouts produce exactly one winner`, async (t: TestContext) => {
    const lockerId = await createLocker(t);
    const result = await runCheckoutBurst(strategy, lockerId, concurrency);

    t.diagnostic(describeBurst(result));

    assert.deepEqual(result.errors, [], "checkout() must not throw on contention");
    assert.equal(result.winners.length, 1, "exactly one caller should be told it won");
    assert.equal(result.activeCheckouts, 1, "exactly one active checkout should exist");
    assert.equal(
      result.rejections.length,
      concurrency - 1,
      "every other caller should be cleanly rejected",
    );
  });
}
