import { pool } from "./db.ts";
import { runMigrations } from "./migrations.ts";

async function migrate() {
  try {
    await runMigrations(pool);
  } finally {
    await pool.end();
  }
}

migrate().catch((err) => {
  console.error(err);
  process.exit(1);
});
