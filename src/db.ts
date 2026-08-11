import pg from "pg";
import { env } from "./env.ts";

const { Pool } = pg;

/**
 * Explicit rather than relying on the `pg` default: the concurrency tests fire
 * bursts of simultaneous checkouts, and a burst larger than this queues on the
 * pool instead of actually contending in Postgres.
 */
export const POOL_SIZE = 10;

export const pool = new Pool({ ...env.pg, max: POOL_SIZE });

export async function withTransaction<T>(
  fn: (client: pg.PoolClient) => Promise<T>,
): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}
