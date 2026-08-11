import type pg from "pg";

/**
 * Why a checkout did not happen. Each strategy reports the reason matching the
 * boundary it guards, so the shared contract test can tell "someone else holds
 * this locker" apart from "I lost the race for the lock itself".
 */
export type CheckoutFailure =
  | "already-checked-out"
  | "lock-unavailable"
  | "version-conflict";

export type CheckoutOutcome =
  | { ok: true; checkoutId: number }
  | { ok: false; reason: CheckoutFailure };

/**
 * The shape every Step 2 strategy implements, so the Step 1 tests can drive all
 * of them through the same entry point.
 */
export interface CheckoutStrategy {
  readonly name: string;
  checkout(userId: string, lockerId: number): Promise<CheckoutOutcome>;
  /**
   * Strategies that open their own clients (Redis) release them here. The
   * caller that created the strategy is responsible for calling this.
   */
  close?(): Promise<void>;
}

/** The "find" half of the find -> mutate -> save sequence. */
export async function isLockerAvailable(
  client: pg.ClientBase,
  lockerId: number,
): Promise<boolean> {
  const result = await client.query<{ available: boolean }>(
    `
      SELECT NOT EXISTS (
        SELECT 1
        FROM checkouts
        WHERE locker_id = $1 AND released_at IS NULL
      ) AS available
    `,
    [lockerId],
  );

  return result.rows[0]?.available ?? false;
}

/** The "save" half. Returns the id of the newly created checkout. */
export async function insertCheckout(
  client: pg.ClientBase,
  userId: string,
  lockerId: number,
): Promise<number> {
  const result = await client.query<{ id: number }>(
    `
      INSERT INTO checkouts (locker_id, user_id)
      VALUES ($1, $2)
      RETURNING id
    `,
    [lockerId, userId],
  );

  const id = result.rows[0]?.id;
  if (id === undefined) {
    throw new Error("INSERT INTO checkouts returned no row");
  }

  return id;
}
