import { createUniqueConstraintCheckout } from "../../../../src/strategies/unique-constraint.ts";
import { checkoutContract } from "../../../support/checkout-contract.ts";
import { applyUniqueConstraintMigration } from "../../../support/unique-constraint.ts";

checkoutContract({
  name: "database uniqueness rule",
  prepareDatabase: applyUniqueConstraintMigration,
  createSubject: (pool) => ({
    checkout: createUniqueConstraintCheckout(pool),
  }),
});
