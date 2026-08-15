import type { Pool } from "pg";
import type { Checkout } from "../checkout.ts";

export interface OptimisticLockingCheckoutOptions {
  afterVersionRead?(): Promise<void>;
}

interface LockerState {
  version: number;
  available: boolean;
}

export function createOptimisticLockingCheckout(
  pool: Pool,
  options: OptimisticLockingCheckoutOptions = {},
): Checkout {
  return async (userId, lockerId) => {
    const client = await pool.connect();
    let transactionOpen = false;

    try {
      // READ COMMITTED gives every loop iteration a fresh view after a
      // compare-and-swap conflict with another committed writer.
      await client.query("BEGIN ISOLATION LEVEL READ COMMITTED");
      transactionOpen = true;

      while (true) {
        const state = await client.query<LockerState>(
          `
            SELECT
              lockers.version,
              NOT EXISTS (
                SELECT 1
                FROM checkouts
                WHERE locker_id = lockers.id
                  AND released_at IS NULL
              ) AS available
            FROM lockers
            WHERE lockers.id = $1
          `,
          [lockerId],
        );
        const locker = state.rows[0];
        if (locker === undefined) {
          throw new Error(`locker ${lockerId} does not exist`);
        }

        await options.afterVersionRead?.();

        if (!locker.available) {
          await client.query("COMMIT");
          transactionOpen = false;
          return { outcome: "unavailable" };
        }

        const claimed = await client.query(
          `
            UPDATE lockers
            SET version = version + 1
            WHERE id = $1 AND version = $2
          `,
          [lockerId, locker.version],
        );

        // Another transaction changed the version after our read. At READ
        // COMMITTED, looping re-reads its committed state. This matters when
        // the conflicting change made the locker available rather than taking
        // it; in that case this caller gets another chance to claim it.
        if (claimed.rowCount === 0) continue;

        const inserted = await client.query<{ id: number }>(
          `
            INSERT INTO checkouts (locker_id, user_id)
            VALUES ($1, $2)
            RETURNING id
          `,
          [lockerId, userId],
        );
        const checkoutId = inserted.rows[0]?.id;
        if (checkoutId === undefined) {
          throw new Error("checkout insert did not return an id");
        }

        await client.query("COMMIT");
        transactionOpen = false;
        return { outcome: "checked_out", checkoutId };
      }
    } catch (error) {
      if (transactionOpen) await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  };
}
