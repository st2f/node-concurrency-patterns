import { randomUUID } from "node:crypto";
import type { TestContext } from "node:test";
import { POOL_SIZE, pool } from "../../src/db.ts";

/**
 * Isolation is per row, not per schema: every test claims a locker nothing else
 * can reach, so integration files stay independent even though they share one
 * database. Cleanup is registered on the test context, so it still runs when
 * the test throws (an assertion failure skips the rest of the test body).
 */
export async function createLocker(t: TestContext): Promise<number> {
  const result = await pool.query<{ id: number }>(
    "INSERT INTO lockers (label) VALUES ($1) RETURNING id",
    [`test-${randomUUID()}`],
  );

  const lockerId = result.rows[0]?.id;
  if (lockerId === undefined) {
    throw new Error("locker fixture insert returned no row");
  }

  t.after(async () => {
    await pool.query("DELETE FROM checkouts WHERE locker_id = $1", [lockerId]);
    await pool.query("DELETE FROM lockers WHERE id = $1", [lockerId]);
  });

  return lockerId;
}

export async function countActiveCheckouts(lockerId: number): Promise<number> {
  const result = await pool.query<{ count: number }>(
    `
      SELECT COUNT(*)::integer AS count
      FROM checkouts
      WHERE locker_id = $1 AND released_at IS NULL
    `,
    [lockerId],
  );

  return result.rows[0]?.count ?? 0;
}

/**
 * Open every pooled connection once and hand them all back. Without this the
 * first burst spends its first milliseconds doing TCP + auth handshakes at
 * staggered times, which staggers the callers and hides the contention the
 * test is trying to create.
 */
export async function warmPool(size = POOL_SIZE): Promise<void> {
  const clients = await Promise.all(
    Array.from({ length: size }, () => pool.connect()),
  );

  for (const client of clients) client.release();
}
