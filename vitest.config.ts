import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    pool: "forks",
    fileParallelism: false,
    testTimeout: 15_000,
    hookTimeout: 15_000,
  },
});
