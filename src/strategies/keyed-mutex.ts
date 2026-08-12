import type { Pool } from "pg";
import type { Checkout } from "../checkout.ts";

export interface KeyedMutexCheckoutOptions {
  afterAvailabilityRead?(): Promise<void>;
}

// Factory/closure lifecycle diagram: docs/factory-createKeyedMutexCheckout.png
export function createKeyedMutexCheckout(
  pool: Pool,
  options: KeyedMutexCheckoutOptions = {},
): Checkout {
  // Promise tails are the mutexes. JavaScript runs this setup synchronously,
  // so two calls for the same locker cannot both observe an empty queue.

  // Each pending Promise acts as a lock for one locker.
  // While it is pending, the next checkout waits for it.
  // Resolving it releases the lock and lets the next checkout continue.
  const tails = new Map<number, Promise<void>>();

  return async (userId, lockerId) => {
    // find the lock/check-out currently ahead of me
    const previous = tails.get(lockerId) ?? Promise.resolve();
    let release!: () => void;
    // create my lock: it stays locked while this Promise is pending
    const current = new Promise<void>((resolve) => {
      release = resolve;
    });
    // my lock is now the last one in the queue
    tails.set(lockerId, current);

    // wait until the previous lock is released
    await previous;

    try {
      const client = await pool.connect();
      let transactionOpen = false;

      try {
        await client.query("BEGIN");
        transactionOpen = true;

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

        await options.afterAvailabilityRead?.();

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
    } finally {
      // release my lock so the next checkout can continue
      release();
      if (tails.get(lockerId) === current) tails.delete(lockerId);
    }
  };
}
