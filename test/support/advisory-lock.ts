import type { IsolatedTestDatabase } from "./database.ts";
import { pollUntil } from "./polling.ts";

// A pending advisory-lock request exists only while a PostgreSQL connection
// is actively waiting to acquire the lock. Seeing this row therefore proves
// that B submitted its lock query and is blocked inside PostgreSQL, rather
// than simply not having run yet.
export async function waitForPendingLockRequest(
  database: IsolatedTestDatabase,
  lockerId: number,
): Promise<void> {
  await pollUntil(
    async () => {
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
      return (pending.rowCount ?? 0) > 0;
    },
    "a pending advisory-lock request to appear",
  );
}
