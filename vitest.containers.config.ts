import { defineConfig, mergeConfig } from "vitest/config";
import baseConfig from "./vitest.config.ts";

export default mergeConfig(
  baseConfig,
  defineConfig({
    test: {
      globalSetup: ["./test/support/containers-global-setup.ts"],
      setupFiles: ["./test/support/containers-setup.ts"],
    },
  }),
);
