import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import pg, { type Pool, type PoolClient } from "pg";
import { it } from "vitest";
import { createIsolatedTestDatabase } from "../../../support/database.ts";
import { pollUntil } from "../../../support/polling.ts";
import { promiseState } from "../../../support/trace.ts";
import { applyUniqueConstraintMigration } from "../../../support/unique-constraint.ts";

const { DatabaseError } = pg;

// A unique-index collision waits on the transaction that inserted the
// conflicting value. Observing B's ungranted transaction-ID lock proves its
// INSERT reached PostgreSQL while A's transaction was still open.
async function waitForPendingTransactionLock(
  pool: Pool,
  backendPid: number,
): Promise<void> {
  await pollUntil(
    async () => {
      const pending = await pool.query<{ waiting: boolean }>(
        `
          SELECT EXISTS (
            SELECT 1
            FROM pg_locks
            WHERE pid = $1
              AND locktype = 'transactionid'
              AND granted = false
          ) AS waiting
        `,
        [backendPid],
      );
      return pending.rows[0]?.waiting === true;
    },
    `backend ${backendPid} to wait on a transaction lock`,
  );
}

/*
Deterministic concurrent collision:
A holds an uncommitted active checkout.
B’s insert is confirmed waiting inside PostgreSQL.
After A commits, B receives 23505 for one_active_checkout_per_locker
*/
it("rejects one of two concurrent active-checkout inserts with SQLSTATE 23505", async () => {
  // A and B each hold a connection while the third observes B's lock wait.
  const database = await createIsolatedTestDatabase({ maxConnections: 3 });
  let clientA: PoolClient | undefined;
  let clientB: PoolClient | undefined;
  let transactionAOpen = false;
  let attemptB: Promise<pg.QueryResult<{ id: number }>> | undefined;
  let expectedConflict: Promise<void> | undefined;

  try {
    await applyUniqueConstraintMigration(database.pool);

    const locker = await database.pool.query<{ id: number }>(
      "INSERT INTO lockers (label) VALUES ($1) RETURNING id",
      [`constraint-collision-${randomUUID()}`],
    );
    const lockerId = locker.rows[0]?.id;
    assert.ok(lockerId !== undefined);

    clientA = await database.pool.connect();
    clientB = await database.pool.connect();

    const backendB = await clientB.query<{ pid: number }>(
      "SELECT pg_backend_pid() AS pid",
    );
    const backendBPid = backendB.rows[0]?.pid;
    assert.ok(backendBPid !== undefined);

    await clientA.query("BEGIN");
    transactionAOpen = true;

    const insertedA = await clientA.query<{ id: number }>(
      `
        INSERT INTO checkouts (locker_id, user_id)
        VALUES ($1, $2)
        RETURNING id
      `,
      [lockerId, "user-a"],
    );
    const checkoutAId = insertedA.rows[0]?.id;
    assert.ok(checkoutAId !== undefined);

    // B's statement uses an implicit transaction. It cannot decide whether
    // its value is unique until A commits or rolls back its uncommitted row.
    attemptB = clientB.query<{ id: number }>(
      `
        INSERT INTO checkouts (locker_id, user_id)
        VALUES ($1, $2)
        RETURNING id
      `,
      [lockerId, "user-b"],
    );
    // Register the expected rejection immediately, before the test waits at
    // the database boundary. Keep attemptB itself so its pending state can be
    // inspected independently.
    expectedConflict = assert.rejects(attemptB, (error: unknown) => {
      assert.ok(error instanceof DatabaseError);
      assert.equal(error.code, "23505");
      assert.equal(error.constraint, "one_active_checkout_per_locker");
      return true;
    });

    await waitForPendingTransactionLock(database.pool, backendBPid);
    assert.equal(promiseState(attemptB), "pending");

    await clientA.query("COMMIT");
    transactionAOpen = false;

    await expectedConflict;

    const active = await database.pool.query<{
      id: number;
      user_id: string;
    }>(
      `
        SELECT id, user_id
        FROM checkouts
        WHERE locker_id = $1 AND released_at IS NULL
      `,
      [lockerId],
    );
    assert.equal(active.rowCount, 1);
    assert.deepEqual(active.rows[0], {
      id: checkoutAId,
      user_id: "user-a",
    });
  } finally {
    try {
      if (transactionAOpen) await clientA?.query("ROLLBACK");
    } finally {
      const pendingCleanup: Promise<unknown>[] = [];
      if (attemptB !== undefined) pendingCleanup.push(attemptB);
      if (expectedConflict !== undefined) pendingCleanup.push(expectedConflict);
      await Promise.allSettled(pendingCleanup);
      clientB?.release();
      clientA?.release();
      await database.close();
    }
  }
});

/*
Partial-index lifecycle:
The first checkout is released.
A second active checkout succeeds.
Both historical and active rows are verified.
*/
it("allows another checkout after the previous checkout is released", async () => {
  const database = await createIsolatedTestDatabase({ maxConnections: 1 });

  try {
    await applyUniqueConstraintMigration(database.pool);

    const locker = await database.pool.query<{ id: number }>(
      "INSERT INTO lockers (label) VALUES ($1) RETURNING id",
      [`constraint-release-${randomUUID()}`],
    );
    const lockerId = locker.rows[0]?.id;
    assert.ok(lockerId !== undefined);

    const first = await database.pool.query<{ id: number }>(
      `
        INSERT INTO checkouts (locker_id, user_id)
        VALUES ($1, $2)
        RETURNING id
      `,
      [lockerId, "user-a"],
    );
    const firstCheckoutId = first.rows[0]?.id;
    assert.ok(firstCheckoutId !== undefined);

    await database.pool.query(
      "UPDATE checkouts SET released_at = now() WHERE id = $1",
      [firstCheckoutId],
    );

    const second = await database.pool.query<{ id: number }>(
      `
        INSERT INTO checkouts (locker_id, user_id)
        VALUES ($1, $2)
        RETURNING id
      `,
      [lockerId, "user-b"],
    );
    const secondCheckoutId = second.rows[0]?.id;
    assert.ok(secondCheckoutId !== undefined);
    assert.notEqual(secondCheckoutId, firstCheckoutId);

    const history = await database.pool.query<{
      id: number;
      user_id: string;
      active: boolean;
    }>(
      `
        SELECT id, user_id, released_at IS NULL AS active
        FROM checkouts
        WHERE locker_id = $1
        ORDER BY id
      `,
      [lockerId],
    );
    assert.deepEqual(history.rows, [
      { id: firstCheckoutId, user_id: "user-a", active: false },
      { id: secondCheckoutId, user_id: "user-b", active: true },
    ]);
  } finally {
    await database.close();
  }
});
