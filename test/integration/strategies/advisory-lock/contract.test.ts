import { createAdvisoryLockCheckout } from "../../../../src/strategies/advisory-lock.ts";
import { checkoutContract } from "../../../support/checkout-contract.ts";

checkoutContract({
  name: "advisory lock (READ COMMITTED)",
  createSubject: (pool, { namespace }) => ({
    checkout: createAdvisoryLockCheckout(pool, {
      lockNamespace: namespace,
    }),
  }),
});
