import { performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";

const POLL_TIMEOUT_MS = 2_000;
const POLL_INTERVAL_MS = 10;

/** Poll an observable condition until it succeeds or its deadline expires. */
export async function pollUntil(
  condition: () => boolean | Promise<boolean>,
  description: string,
): Promise<void> {
  // performance.now() is monotonic, so adjusting the system clock cannot
  // move this deadline backwards or forwards during a test.
  const deadline = performance.now() + POLL_TIMEOUT_MS;

  while (true) {
    if (await condition()) return;

    const remainingMs = deadline - performance.now();
    if (remainingMs <= 0) {
      throw new Error(
        `timed out waiting for ${description} after ${POLL_TIMEOUT_MS}ms`,
      );
    }

    await delay(Math.min(POLL_INTERVAL_MS, remainingMs), undefined, {
      ref: false,
    });
  }
}
