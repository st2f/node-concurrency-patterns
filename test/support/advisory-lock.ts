import assert from "node:assert/strict";
import type { IsolatedTestDatabase } from "./database.ts";

const WAIT_TIMEOUT_MS = 2_000;
const POLL_INTERVAL_MS = 10;

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    const timeout = setTimeout(resolve, milliseconds);
    timeout.unref();
  });
}

// A pending advisory-lock request exists only while a PostgreSQL connection
// is actively waiting to acquire the lock. Seeing this row therefore proves
// that B submitted its lock query and is blocked inside PostgreSQL, rather
// than simply not having run yet.
export async function waitForPendingLockRequest(
  database: IsolatedTestDatabase,
  lockerId: number,
): Promise<void> {
  const deadline = Date.now() + WAIT_TIMEOUT_MS;

  while (Date.now() < deadline) {
    const pending = await database.pool.query(
      `
        SELECT 1
        FROM pg_locks
        WHERE locktype = 'advisory'
          AND granted = false
          AND database = (
            SELECT oid
            FROM pg_database
            WHERE datname = current_database()
          )
          AND classid = hashtext($1::text)::oid
          AND objid = $2::integer::oid
          AND objsubid = 2
      `,
      [database.namespace, lockerId],
    );
    if ((pending.rowCount ?? 0) > 0) return;
    await delay(POLL_INTERVAL_MS);
  }

  assert.fail(
    `no pending advisory-lock request appeared within ${WAIT_TIMEOUT_MS}ms`,
  );
}
