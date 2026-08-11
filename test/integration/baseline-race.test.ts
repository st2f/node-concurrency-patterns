import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { afterAll, test } from "vitest";
import { pool } from "../../src/db.ts";

afterAll(async () => {
  await pool.end();
});

test("two unprotected checkouts can claim the same locker", async () => {
  const label = `step-1a-${randomUUID()}`;
  const lockerResult = await pool.query<{ id: number }>(
    "INSERT INTO lockers (label) VALUES ($1) RETURNING id",
    [label],
  );
  const lockerId = lockerResult.rows[0]?.id;

  assert.ok(lockerId !== undefined);

  try {
    const clientA = await pool.connect();
    const clientB = await pool.connect();
    let transactionAOpen = false;
    let transactionBOpen = false;

    try {
      const [backendA, backendB] = await Promise.all([
        clientA.query<{ pid: number }>("SELECT pg_backend_pid() AS pid"),
        clientB.query<{ pid: number }>("SELECT pg_backend_pid() AS pid"),
      ]);

      assert.notEqual(backendA.rows[0]?.pid, backendB.rows[0]?.pid);

      await clientA.query("BEGIN");
      transactionAOpen = true;
      await clientB.query("BEGIN");
      transactionBOpen = true;

      const availabilitySql = `
        SELECT NOT EXISTS (
          SELECT 1
          FROM checkouts
          WHERE locker_id = $1 AND released_at IS NULL
        ) AS available
      `;

      // Force both transactions to decide before either transaction writes.
      const availabilityA = await clientA.query<{ available: boolean }>(
        availabilitySql,
        [lockerId],
      );
      const availabilityB = await clientB.query<{ available: boolean }>(
        availabilitySql,
        [lockerId],
      );

      assert.equal(availabilityA.rows[0]?.available, true);
      assert.equal(availabilityB.rows[0]?.available, true);

      const insertSql = `
        INSERT INTO checkouts (locker_id, user_id)
        VALUES ($1, $2)
      `;

      await clientA.query(insertSql, [lockerId, "user-a"]);
      await clientB.query(insertSql, [lockerId, "user-b"]);

      await clientA.query("COMMIT");
      transactionAOpen = false;
      await clientB.query("COMMIT");
      transactionBOpen = false;
    } finally {
      if (transactionAOpen) await clientA.query("ROLLBACK");
      if (transactionBOpen) await clientB.query("ROLLBACK");
      clientA.release();
      clientB.release();
    }

    const activeCheckouts = await pool.query<{ count: number }>(
      `
        SELECT COUNT(*)::integer AS count
        FROM checkouts
        WHERE locker_id = $1 AND released_at IS NULL
      `,
      [lockerId],
    );

    // The test passes when it proves the deliberately unprotected invariant
    // can be violated.
    assert.equal(activeCheckouts.rows[0]?.count, 2);
  } finally {
    await pool.query("DELETE FROM checkouts WHERE locker_id = $1", [lockerId]);
    await pool.query("DELETE FROM lockers WHERE id = $1", [lockerId]);
  }
});
