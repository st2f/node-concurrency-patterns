import { fork, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import pg from "pg";
import type { Checkout, CheckoutResult } from "../../src/checkout.ts";
import { createAdvisoryLockCheckout } from "../../src/strategies/advisory-lock.ts";
import { createKeyedMutexCheckout } from "../../src/strategies/keyed-mutex.ts";
import { createOptimisticLockingCheckout } from "../../src/strategies/optimistic-locking.ts";
import { createUniqueConstraintCheckout } from "../../src/strategies/unique-constraint.ts";

const { Pool } = pg;
const IPC_TIMEOUT_MS = 5_000;

export type CheckoutStrategyId =
  | "keyed-mutex"
  | "advisory-lock"
  | "optimistic-locking"
  | "unique-constraint";
export type CheckoutWorkerOrchestration =
  | "pause-after-availability-read"
  | "pause-after-advisory-lock-acquired"
  | "pause-after-version-read"
  | "pause-before-insert";

export interface CheckoutWorkerPostgresConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  options: string;
}

export interface SpawnCheckoutWorkerOptions {
  strategy: CheckoutStrategyId;
  namespace: string;
  postgres: CheckoutWorkerPostgresConfig;
  orchestration?: CheckoutWorkerOrchestration;
}

export interface CheckoutWorkerAttempt {
  attemptId: string;
  atOrchestrationSeam: Promise<void>;
  result: Promise<CheckoutResult>;
  releaseOrchestrationSeam(): void;
}

export interface CheckoutWorker {
  pid: number;
  startCheckout(userId: string, lockerId: number): CheckoutWorkerAttempt;
  close(): Promise<void>;
}

interface InitializeMessage extends SpawnCheckoutWorkerOptions {
  type: "initialize";
}

interface CheckoutMessage {
  type: "checkout";
  attemptId: string;
  userId: string;
  lockerId: number;
}

interface ReleaseOrchestrationSeamMessage {
  type: "release-orchestration-seam";
  attemptId: string;
}

interface CloseMessage {
  type: "close";
}

type ParentMessage =
  | InitializeMessage
  | CheckoutMessage
  | ReleaseOrchestrationSeamMessage
  | CloseMessage;

interface ReadyMessage {
  type: "ready";
}

interface AtOrchestrationSeamMessage {
  type: "at-orchestration-seam";
  attemptId: string;
}

interface ResultMessage {
  type: "result";
  attemptId: string;
  result: CheckoutResult;
}

interface FailureMessage {
  type: "failure";
  attemptId?: string;
  error: {
    name: string;
    message: string;
    stack?: string;
  };
}

interface ClosedMessage {
  type: "closed";
}

type WorkerMessage =
  | ReadyMessage
  | AtOrchestrationSeamMessage
  | ResultMessage
  | FailureMessage
  | ClosedMessage;

interface Deferred<T> {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
}

interface AttemptState {
  atOrchestrationSeam: Deferred<void>;
  result: Deferred<CheckoutResult>;
}

function createDeferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });

  return { promise, resolve, reject };
}

function waitWithTimeout<T>(
  promise: Promise<T>,
  description: string,
): Promise<T> {
  const timed = new Promise<T>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`${description} timed out after ${IPC_TIMEOUT_MS}ms`));
    }, IPC_TIMEOUT_MS);
    timeout.unref();

    promise.then(
      (value) => {
        clearTimeout(timeout);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timeout);
        reject(error);
      },
    );
  });

  // The caller may await the seam before the result. Mark both promises as
  // handled immediately while preserving rejection for the later await.
  void timed.catch(() => undefined);
  return timed;
}

function errorFromMessage(message: FailureMessage): Error {
  const error = new Error(message.error.message);
  error.name = message.error.name;
  if (message.error.stack !== undefined) error.stack = message.error.stack;
  return error;
}

function sendToWorker(child: ChildProcess, message: ParentMessage): void {
  if (!child.connected) {
    throw new Error("checkout worker IPC channel is closed");
  }
  child.send(message);
}

/*
checkout-worker.ts
│
├── Parent-side code
│   └── spawnCheckoutWorker()
│       └── fork(...)
│
└── Child-side code
    └── runWorker()
*/
export async function spawnCheckoutWorker(
  options: SpawnCheckoutWorkerOptions,
): Promise<CheckoutWorker> {
  const modulePath = fileURLToPath(import.meta.url);
  const child = fork(modulePath, [], {
    execArgv: [],
    stdio: ["ignore", "inherit", "inherit", "ipc"],
  }); // start a new Node process and use this very file as its entry point, which will run runWorker()
  const pid = child.pid;
  if (pid === undefined) {
    child.kill();
    throw new Error("checkout worker did not receive a process id");
  }

  const ready = createDeferred<void>();
  const closed = createDeferred<void>();
  const exited = createDeferred<void>();
  const attempts = new Map<string, AttemptState>();
  let attemptSequence = 0;
  let closePromise: Promise<void> | undefined;

  function rejectPending(error: unknown): void {
    ready.reject(error);
    for (const attempt of attempts.values()) {
      attempt.atOrchestrationSeam.reject(error);
      attempt.result.reject(error);
    }
    attempts.clear();
  }

  child.on("message", (message: WorkerMessage) => {
    if (message.type === "ready") {
      ready.resolve();
      return;
    }

    if (message.type === "closed") {
      closed.resolve();
      return;
    }

    if (message.type === "failure") {
      const error = errorFromMessage(message);
      if (message.attemptId === undefined) {
        rejectPending(error);
        return;
      }

      const attempt = attempts.get(message.attemptId);
      if (attempt !== undefined) {
        attempt.atOrchestrationSeam.reject(error);
        attempt.result.reject(error);
        attempts.delete(message.attemptId);
      }
      return;
    }

    const attempt = attempts.get(message.attemptId);
    if (attempt === undefined) return;

    if (message.type === "at-orchestration-seam") {
      attempt.atOrchestrationSeam.resolve();
    } else {
      attempt.result.resolve(message.result);
      attempts.delete(message.attemptId);
    }
  });

  child.once("error", (error) => {
    rejectPending(error);
  });

  child.once("exit", (code, signal) => {
    exited.resolve();
    if (closePromise === undefined) {
      rejectPending(
        new Error(
          `checkout worker exited unexpectedly (code=${String(code)}, signal=${String(signal)})`,
        ),
      );
    }
  });

  try {
    sendToWorker(child, { type: "initialize", ...options });
    await waitWithTimeout(ready.promise, "checkout worker initialization");
  } catch (error) {
    child.kill();
    await waitWithTimeout(exited.promise, "checkout worker termination");
    throw error;
  }

  return {
    pid,
    startCheckout(userId, lockerId) {
      const attemptId = `${pid}-${++attemptSequence}`;
      const atOrchestrationSeam = createDeferred<void>();
      const result = createDeferred<CheckoutResult>();
      attempts.set(attemptId, { atOrchestrationSeam, result });

      try {
        sendToWorker(child, {
          type: "checkout",
          attemptId,
          userId,
          lockerId,
        });
      } catch (error) {
        attempts.delete(attemptId);
        atOrchestrationSeam.reject(error);
        result.reject(error);
      }

      return {
        attemptId,
        atOrchestrationSeam: waitWithTimeout(
          atOrchestrationSeam.promise,
          `checkout attempt ${attemptId} reaching its orchestration seam`,
        ),
        result: waitWithTimeout(
          result.promise,
          `checkout attempt ${attemptId} completing`,
        ),
        releaseOrchestrationSeam() {
          if (child.connected) {
            sendToWorker(child, {
              type: "release-orchestration-seam",
              attemptId,
            });
          }
        },
      };
    },
    async close() {
      if (closePromise !== undefined) return closePromise;

      closePromise = (async () => {
        try {
          sendToWorker(child, { type: "close" });
          await waitWithTimeout(closed.promise, "checkout worker close");
          await waitWithTimeout(exited.promise, "checkout worker exit");
        } catch (error) {
          child.kill();
          await waitWithTimeout(exited.promise, "checkout worker termination");
          throw error;
        }
      })();

      return closePromise;
    },
  };
}

function serializeError(error: unknown): FailureMessage["error"] {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      ...(error.stack === undefined ? {} : { stack: error.stack }),
    };
  }

  return { name: "Error", message: String(error) };
}

function sendFromWorker(message: WorkerMessage): void {
  if (process.send === undefined) {
    throw new Error("checkout worker requires an IPC channel");
  }
  process.send(message);
}

async function runWorker(): Promise<void> {
  let pool: pg.Pool | undefined;
  let checkout: Checkout | undefined;
  let orchestration: CheckoutWorkerOrchestration | undefined;
  let activeAttemptId: string | undefined;
  let releaseActiveSeam: (() => void) | undefined;
  const releasedAttempts = new Set<string>();
  const inFlight = new Set<Promise<void>>();
  let closing = false;

  function buildCheckout(
    strategy: CheckoutStrategyId,
    strategyPool: pg.Pool,
    namespace: string,
  ): Checkout {
    async function pauseAtOrchestrationSeam(
      expected: CheckoutWorkerOrchestration,
      seamDescription: string,
    ): Promise<void> {
      if (orchestration !== expected) return;
      if (activeAttemptId === undefined) {
        throw new Error(`${seamDescription} reached without an attempt`);
      }

      const attemptId = activeAttemptId;
      sendFromWorker({ type: "at-orchestration-seam", attemptId });
      if (releasedAttempts.delete(attemptId) || closing) return;

      await new Promise<void>((resolve) => {
        releaseActiveSeam = resolve;
      });
      releaseActiveSeam = undefined;
    }

    switch (strategy) {
      case "keyed-mutex":
        return createKeyedMutexCheckout(strategyPool, {
          async afterAvailabilityRead() {
            await pauseAtOrchestrationSeam(
              "pause-after-availability-read",
              "availability seam",
            );
          },
        });
      case "advisory-lock":
        return createAdvisoryLockCheckout(strategyPool, {
          lockNamespace: namespace,
          async afterAdvisoryLockAcquired() {
            await pauseAtOrchestrationSeam(
              "pause-after-advisory-lock-acquired",
              "advisory-lock seam",
            );
          },
        });
      case "optimistic-locking":
        return createOptimisticLockingCheckout(strategyPool, {
          async afterVersionRead(state) {
            // Both processes pause after their initial read while the locker
            // is available. After one process advances the version, the
            // loser's conditional UPDATE matches no row and it reads again.
            // That second read sees the winner's checkout, so let it continue
            // to `unavailable` without introducing another IPC pause.
            if (!state.available) return;
            await pauseAtOrchestrationSeam(
              "pause-after-version-read",
              "version-read seam",
            );
          },
        });
      case "unique-constraint":
        return createUniqueConstraintCheckout(strategyPool, {
          async beforeInsert() {
            await pauseAtOrchestrationSeam(
              "pause-before-insert",
              "pre-insert seam",
            );
          },
        });
    }
  }

  async function handleMessage(message: ParentMessage): Promise<void> {
    if (message.type === "initialize") {
      if (pool !== undefined) throw new Error("worker is already initialized");
      orchestration = message.orchestration;
      pool = new Pool({ ...message.postgres, max: 1 });
      checkout = buildCheckout(message.strategy, pool, message.namespace);
      sendFromWorker({ type: "ready" });
      return;
    }

    if (message.type === "release-orchestration-seam") {
      releasedAttempts.add(message.attemptId);
      if (activeAttemptId === message.attemptId) releaseActiveSeam?.();
      return;
    }

    if (message.type === "close") {
      if (closing) return;
      closing = true;
      releaseActiveSeam?.();
      await Promise.allSettled(inFlight);
      await pool?.end();
      sendFromWorker({ type: "closed" });
      process.disconnect();
      return;
    }

    if (checkout === undefined) throw new Error("worker is not initialized");
    if (closing) throw new Error("worker is closing");
    if (activeAttemptId !== undefined) {
      throw new Error("worker supports only one checkout attempt at a time");
    }

    const attempt = (async () => {
      activeAttemptId = message.attemptId;
      try {
        const result = await checkout(message.userId, message.lockerId);
        sendFromWorker({
          type: "result",
          attemptId: message.attemptId,
          result,
        });
      } catch (error) {
        sendFromWorker({
          type: "failure",
          attemptId: message.attemptId,
          error: serializeError(error),
        });
      } finally {
        activeAttemptId = undefined;
        releasedAttempts.delete(message.attemptId);
      }
    })();
    inFlight.add(attempt);
    void attempt.then(
      () => inFlight.delete(attempt),
      () => inFlight.delete(attempt),
    );
  }

  process.on("message", (message: ParentMessage) => {
    void handleMessage(message).catch((error: unknown) => {
      sendFromWorker({ type: "failure", error: serializeError(error) });
    });
  });
}

// for child processes, this file is main
if (import.meta.main) {
  await runWorker();
}
