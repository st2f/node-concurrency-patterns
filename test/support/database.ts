import { randomUUID } from "node:crypto";
import pg from "pg";
import { env } from "../../src/env.ts";
import { runMigrations } from "../../src/migrations.ts";

const { Pool } = pg;

export interface IsolatedTestDatabase {
  pool: pg.Pool;
  close(): Promise<void>;
}

/**
 * Provision a migrated schema for one contract subject.
 *
 * A separate schema matters once the unique-constraint strategy exists: its
 * partial index must not make the other strategies appear safer than they are.
 */
export async function createIsolatedTestDatabase(): Promise<IsolatedTestDatabase> {
  const schema = `test_${randomUUID().replaceAll("-", "")}`;
  const adminPool = new Pool(env.pg);
  await adminPool.query(`CREATE SCHEMA ${schema}`);

  const strategyPool = new Pool({
    ...env.pg,
    options: `-c search_path=${schema}`,
  });

  try {
    await runMigrations(strategyPool);
  } catch (error) {
    await strategyPool.end();
    await adminPool.query(`DROP SCHEMA ${schema} CASCADE`);
    await adminPool.end();
    throw error;
  }

  let closed = false;

  return {
    pool: strategyPool,
    async close() {
      if (closed) return;
      closed = true;

      try {
        await strategyPool.end();
      } finally {
        try {
          await adminPool.query(`DROP SCHEMA ${schema} CASCADE`);
        } finally {
          await adminPool.end();
        }
      }
    },
  };
}
