import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { Pool } from "pg";

const migrationsDir = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
  "migrations",
);

/** Apply every pending migration to the schema selected by `database`. */
export async function runMigrations(database: Pool): Promise<void> {
  await database.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      filename TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  const applied = new Set(
    (
      await database.query<{ filename: string }>(
        "SELECT filename FROM schema_migrations",
      )
    ).rows.map((row) => row.filename),
  );

  const files = (await readdir(migrationsDir))
    .filter((file) => file.endsWith(".sql"))
    .sort();

  for (const file of files) {
    if (applied.has(file)) continue;

    const sql = await readFile(path.join(migrationsDir, file), "utf8");
    const client = await database.connect();
    try {
      await client.query("BEGIN");
      await client.query(sql);
      await client.query(
        "INSERT INTO schema_migrations (filename) VALUES ($1)",
        [file],
      );
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw new Error(`migration ${file} failed`, { cause: error });
    } finally {
      client.release();
    }
  }
}
