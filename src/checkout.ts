export type CheckoutResult =
  | { outcome: "checked_out"; checkoutId: number }
  | { outcome: "unavailable" };

/**
 * The common boundary exercised by every concurrency strategy.
 *
 * Expected contention is represented by `unavailable`; rejected promises are
 * reserved for unexpected infrastructure or programming failures.
 */
export type Checkout = (
  userId: string,
  lockerId: number,
) => Promise<CheckoutResult>;
