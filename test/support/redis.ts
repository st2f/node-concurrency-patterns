import { Redis } from "ioredis";
import { env } from "../../src/env.ts";
import { redisFencingNamespaceKeyPattern } from "../../src/strategies/redis-fencing.ts";

export interface IsolatedTestRedis {
  client: Redis;
  close(): Promise<void>;
}

/** Create a Redis client whose namespace keys are removed during cleanup. */
export function createIsolatedTestRedis(namespace: string): IsolatedTestRedis {
  const client = new Redis({
    ...env.redis,
    maxRetriesPerRequest: 3,
  });
  let closed = false;

  return {
    client,
    async close() {
      if (closed) return;
      closed = true;

      try {
        let cursor = "0";
        do {
          const [nextCursor, keys] = await client.scan(
            cursor,
            "MATCH",
            redisFencingNamespaceKeyPattern(namespace),
            "COUNT",
            100,
          );
          cursor = nextCursor;
          if (keys.length > 0) await client.unlink(...keys);
        } while (cursor !== "0");
      } finally {
        await client.quit();
      }
    },
  };
}
