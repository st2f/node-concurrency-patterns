import { globSync } from "node:fs";
import { resolve } from "node:path";
import { run } from "node:test";
import { spec } from "node:test/reporters";

/**
 * `node --test` exits 0 both when its glob matches no files and when the files
 * it matches contain no tests. Either would report a green integration run that
 * proved nothing, so this runner refuses to succeed unless every file it found
 * actually contributed tests.
 *
 * Counting is deliberately done from the per-file summaries. The run-level
 * summary counts each *file* as one test when that file registers none, so
 * `tests: 1` there does not mean a test ran.
 *
 * Files run one at a time: these tests open real connections and contend on
 * shared rows, so parallel files would compete for the pool and each other's
 * timing.
 */
const PATTERN = "test/integration/*.test.ts";

const files = globSync(PATTERN).sort();
if (files.length === 0) {
  console.error(`No integration test files matched ${PATTERN}`);
  process.exit(1);
}

const reporter = new spec();
reporter.pipe(process.stdout);

const testsPerFile = new Map<string, number>();
let success = false;

for await (const event of run({ files, concurrency: 1 })) {
  if (event.type === "test:summary") {
    // Per-file summaries carry a `file`; the run-wide one does not.
    if (event.data.file === undefined) success = event.data.success;
    else testsPerFile.set(resolve(event.data.file), event.data.counts.tests);
  }

  reporter.write(event);
}

reporter.end();

const barren = files.filter((file) => (testsPerFile.get(resolve(file)) ?? 0) === 0);
if (barren.length > 0) {
  console.error(`\nNo tests ran in: ${barren.join(", ")}`);
  process.exit(1);
}

process.exit(success ? 0 : 1);
