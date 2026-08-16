import { createRedisFencingCheckout } from "../../../../src/strategies/redis-fencing.ts";
import { checkoutContract } from "../../../support/checkout-contract.ts";
import { createIsolatedTestRedis } from "../../../support/redis.ts";

checkoutContract({
  name: "Redis fencing",
  createSubject: (pool, { namespace }) => {
    const redis = createIsolatedTestRedis(namespace);
    return {
      checkout: createRedisFencingCheckout(pool, redis.client, {
        lockNamespace: namespace,
      }),
      close: redis.close,
    };
  },
});
