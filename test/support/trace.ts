import { inspect } from "node:util";

export type PromiseState = "pending" | "fulfilled" | "rejected";

/*
Reading a Promise's state without awaiting it.

`util.inspect` renders V8's internal promise slot, so this is synchronous and
does not schedule anything: it answers "what is true right now", unlike
`Promise.race`, which can only answer after at least one microtask.
*/
export function promiseState(promise: Promise<unknown>): PromiseState {
  const rendered = inspect(promise, { depth: 0 });
  if (rendered.startsWith("Promise { <pending>")) return "pending";
  return rendered.startsWith("Promise { <rejected>") ? "rejected" : "fulfilled";
}

export interface Tracer {
  // Snapshot: the state of every named promise at this line
  mark(label: string, promises?: Record<string, Promise<unknown>>): void;
  // Event log: report the moment a named promise settles
  watch(name: string, promise: Promise<unknown>): void;
}

const NO_OP: Tracer = { mark() {}, watch() {} };

// Tracing stays off unless TRACE is set, so an ordinary `npm test` run is quiet:
// TRACE=1 npx vitest run test/integration/strategies/keyed-mutex/topology.test.ts
export function createTracer(scope: string): Tracer {
  if (process.env["TRACE"] === undefined) return NO_OP;

  const start = performance.now();
  const at = (): string =>
    `${(performance.now() - start).toFixed(1).padStart(7)}ms`;

  return {
    mark(label, promises = {}) {
      const states = Object.entries(promises)
        .map(([name, promise]) => `${name}=${promiseState(promise)}`)
        .join(" ");
      console.log(
        `[${scope}] ${at()} | ${label}${states ? ` | ${states}` : ""}`,
      );
    },
    watch(name, promise) {
      // The handler runs one microtask after the promise settles, which is far
      // below the IPC latency this trace is used to order.
      promise.then(
        () => {
          console.log(`[${scope}] ${at()} | ${name} -> fulfilled`);
        },
        (error: unknown) => {
          console.log(
            `[${scope}] ${at()} | ${name} -> rejected (${String(error)})`,
          );
        },
      );
    },
  };
}
