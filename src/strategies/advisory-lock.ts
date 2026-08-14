import type { Pool } from "pg";
import type { Checkout } from "../checkout.ts";

export interface AdvisoryLockCheckoutOptions {
  lockNamespace: string;
}

export function createAdvisoryLockCheckout(
  pool: Pool,
  options: AdvisoryLockCheckoutOptions,
): Checkout {
  if (options.lockNamespace.length === 0) {
    throw new RangeError("lockNamespace must not be empty");
  }

  return async (userId, lockerId) => {
    const client = await pool.connect();
    let transactionOpen = false;

    try {
      // Relies on READ COMMITTED
      // so this query gets a fresh snapshot after waiting for
      // the advisory lock and sees the previous holder's commit.
      await client.query("BEGIN ISOLATION LEVEL READ COMMITTED");
      transactionOpen = true;

      // The two-integer form gives every locker its own lock within this
      // strategy instance's namespace. hashtext collisions can only cause
      // unnecessary serialization; they cannot allow conflicting writes.
      await client.query(
        `
          SELECT pg_advisory_xact_lock(
            hashtext($1::text),
            $2::integer
          )
        `,
        [options.lockNamespace, lockerId],
      );

      const availability = await client.query<{ available: boolean }>(
        `
          SELECT NOT EXISTS (
            SELECT 1
            FROM checkouts
            WHERE locker_id = $1 AND released_at IS NULL
          ) AS available
        `,
        [lockerId],
      );

      if (availability.rows[0]?.available !== true) {
        await client.query("COMMIT");
        transactionOpen = false;
        return { outcome: "unavailable" };
      }

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
    } catch (error) {
      if (transactionOpen) await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  };
}
