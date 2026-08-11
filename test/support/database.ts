import { randomUUID } from "node:crypto";
import pg from "pg";
import { env } from "../../src/env.ts";
import { runMigrations } from "../../src/migrations.ts";

const { Pool } = pg;

export interface IsolatedTestDatabase {
  namespace: string;
  pool: pg.Pool;
  close(): Promise<void>;
}

export interface IsolatedTestDatabaseOptions {
  maxConnections: number;
}

/**
 * Provision a migrated schema for one contract subject.
 *
 * A separate schema matters once the unique-constraint strategy exists: its
 * partial index must not make the other strategies appear safer than they are.
 * Its name also namespaces coordination mechanisms that are not schema-scoped,
 * such as Postgres advisory locks and Redis keys.
 */
export async function createIsolatedTestDatabase(
  options: IsolatedTestDatabaseOptions,
): Promise<IsolatedTestDatabase> {
  if (!Number.isInteger(options.maxConnections) || options.maxConnections < 1) {
    throw new RangeError("maxConnections must be a positive integer");
  }

  const namespace = `test_${randomUUID().replaceAll("-", "")}`;
  const adminPool = new Pool({ ...env.pg, max: 1 });
  try {
    await adminPool.query(`CREATE SCHEMA ${namespace}`);
  } catch (error) {
    await adminPool.end();
    throw error;
  }

  const strategyPool = new Pool({
    ...env.pg,
    max: options.maxConnections,
    options: `-c search_path=${namespace}`,
  });

  try {
    await runMigrations(strategyPool);
  } catch (error) {
    try {
      await strategyPool.end();
    } finally {
      try {
        await adminPool.query(`DROP SCHEMA ${namespace} CASCADE`);
      } finally {
        await adminPool.end();
      }
    }
    throw error;
  }

  let closed = false;

  return {
    namespace,
    pool: strategyPool,
    async close() {
      if (closed) return;
      closed = true;

      try {
        await strategyPool.end();
      } finally {
        try {
          await adminPool.query(`DROP SCHEMA ${namespace} CASCADE`);
        } finally {
          await adminPool.end();
        }
      }
    },
  };
}
