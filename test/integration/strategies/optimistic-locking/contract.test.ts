import { createOptimisticLockingCheckout } from "../../../../src/strategies/optimistic-locking.ts";
import { checkoutContract } from "../../../support/checkout-contract.ts";

checkoutContract({
  name: "optimistic locking",
  createSubject: (pool) => ({
    checkout: createOptimisticLockingCheckout(pool),
  }),
});
