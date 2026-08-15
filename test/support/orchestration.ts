import type { CheckoutResult } from "../../src/checkout.ts";

const WAIT_TIMEOUT_MS = 2_000;

export interface TestSignal {
  readonly promise: Promise<void>;
  release(): void;
}

/*
one required checkout attempt
          +
zero or more additional attempts
          +
the collection cannot be mutated here
*/
export type CheckoutAttempts = readonly [
  Promise<CheckoutResult>,
  ...Promise<CheckoutResult>[],
];

/** Create a one-shot signal for coordinating concurrent test operations. */
export function createTestSignal(): TestSignal {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });

  return { promise, release };
}

/**
 * Wait for a test signal while failing promptly if a checkout settles first.
 */
export async function waitForSignal(
  signal: TestSignal,
  attempts: CheckoutAttempts,
  description: string,
): Promise<void> {
  const checkoutFinished = Promise.race(attempts).then(
    (result) => {
      throw new Error(
        `checkout finished with ${result.outcome} while waiting for ${description}`,
      );
    },
    (error: unknown) => {
      throw error;
    },
  );

  let timeout: ReturnType<typeof setTimeout> | undefined;
  const timedOut = new Promise<never>((_, reject) => {
    timeout = setTimeout(() => {
      reject(
        new Error(
          `timed out waiting for ${description} after ${WAIT_TIMEOUT_MS}ms`,
        ),
      );
    }, WAIT_TIMEOUT_MS);
    timeout.unref();
  });

  try {
    await Promise.race([signal.promise, checkoutFinished, timedOut]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}
