import { createKeyedMutexCheckout } from "../../../../src/strategies/keyed-mutex.ts";
import { checkoutContract } from "../../../support/checkout-contract.ts";

checkoutContract({
  name: "keyed mutex",
  createSubject: (pool) => ({
    checkout: createKeyedMutexCheckout(pool),
  }),
});
