import assert from "node:assert/strict";
import { after, test } from "node:test";
import { pool } from "../../src/db.ts";
import { createUnprotectedStrategy } from "../../src/strategies/unprotected.ts";
import {
  DEFAULT_CONCURRENCY,
  runCheckoutBurst,
} from "../helpers/checkout-contract.ts";
import { createLocker } from "../helpers/fixtures.ts";

after(async () => {
  await pool.end();
});

/**
 * Step 1b ships the shared contract that Step 2's strategies will be held to.
 * A contract nothing is measured against yet is worth nothing, so this file
 * points it at the unprotected baseline and checks it reports the violation.
 *
 * The burst is not merely "likely" to race: `Promise.all` creates all N calls
 * in one tick over already-open pooled connections, so every SELECT is in
 * flight before the first INSERT round-trip returns. Measured locally at
 * 8 winners in 40/40 rounds. The assertion below is still the weaker "more
 * than one winner", because the claim under test is that the harness detects
 * a violated invariant — not that a broken strategy loses by any exact margin.
 */
test("the shared contract detects an unprotected strategy", async (t) => {
  const strategy = createUnprotectedStrategy();
  const lockerId = await createLocker(t);

  const result = await runCheckoutBurst(strategy, lockerId, DEFAULT_CONCURRENCY);

  t.diagnostic(
    `winners=${result.winners.length}/${DEFAULT_CONCURRENCY} activeCheckouts=${result.activeCheckouts}`,
  );

  assert.deepEqual(result.errors, [], "the baseline should fail silently, not throw");
  assert.ok(
    result.winners.length > 1,
    `expected the unprotected strategy to hand the same locker to several callers, got ${result.winners.length}`,
  );
  assert.equal(
    result.activeCheckouts,
    result.winners.length,
    "database state should corroborate the results the callers were handed",
  );
});

/**
 * Guards the harness itself: if `runCheckoutBurst` silently stopped firing
 * concurrently, the burst above would serialize and every later strategy would
 * pass its contract for the wrong reason.
 */
test("a burst of one caller is the trivially safe case", async (t) => {
  const strategy = createUnprotectedStrategy();
  const lockerId = await createLocker(t);

  const result = await runCheckoutBurst(strategy, lockerId, 1);

  assert.equal(result.winners.length, 1);
  assert.equal(result.activeCheckouts, 1);
});
