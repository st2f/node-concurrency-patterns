import { readFile } from "node:fs/promises";
import type { Pool } from "pg";

const migrationUrl = new URL(
  "../../migrations/strategies/unique-constraint.sql",
  import.meta.url,
);

/** Apply the uniqueness strategy only to the current isolated test schema. */
export async function applyUniqueConstraintMigration(
  pool: Pool,
): Promise<void> {
  const sql = await readFile(migrationUrl, "utf8");
  await pool.query(sql);
}
