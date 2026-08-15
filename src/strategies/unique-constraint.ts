import pg, { type Pool } from "pg";
import type { Checkout } from "../checkout.ts";

const { DatabaseError } = pg;
const UNIQUE_VIOLATION = "23505";
const ACTIVE_CHECKOUT_CONSTRAINT = "one_active_checkout_per_locker";

function isActiveCheckoutConflict(error: unknown): boolean {
  return (
    error instanceof DatabaseError &&
    error.code === UNIQUE_VIOLATION &&
    error.constraint === ACTIVE_CHECKOUT_CONSTRAINT
  );
}

/**
 * Create a checkout operation whose concurrency guarantee comes from the
 * partial unique index installed by this strategy's schema migration.
 */
export function createUniqueConstraintCheckout(pool: Pool): Checkout {
  return async (userId, lockerId) => {
    try {
      // There is deliberately no read-before-write availability check here.
      // The INSERT is the attempt to claim the locker, and PostgreSQL chooses
      // one winner atomically when active-checkout inserts race.
      const inserted = await pool.query<{ id: number }>(
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

      return { outcome: "checked_out", checkoutId };
    } catch (error) {
      // A conflict on this particular index is expected domain control flow.
      // Other 23505 errors, foreign-key errors, and infrastructure failures
      // remain rejected promises rather than being mislabeled unavailable.
      if (isActiveCheckoutConflict(error)) {
        return { outcome: "unavailable" };
      }

      throw error;
    }
  };
}
