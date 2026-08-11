import {
  insertCheckout,
  isLockerAvailable,
  type CheckoutStrategy,
} from "../checkout.ts";
import { withTransaction } from "../db.ts";

/**
 * The naive baseline: find -> mutate -> save with no coordination at all.
 *
 * It runs inside a transaction on purpose. At Postgres' default READ COMMITTED
 * isolation a transaction alone does not serialize these two callers, so this
 * strategy is the control that shows "just wrap it in a transaction" is not a
 * fix. Every Step 2 strategy is this function plus one guard.
 */
export function createUnprotectedStrategy(): CheckoutStrategy {
  return {
    name: "unprotected",
    async checkout(userId, lockerId) {
      return withTransaction(async (client) => {
        if (!(await isLockerAvailable(client, lockerId))) {
          return { ok: false, reason: "already-checked-out" };
        }

        return { ok: true, checkoutId: await insertCheckout(client, userId, lockerId) };
      });
    },
  };
}
