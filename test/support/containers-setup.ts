import { inject } from "vitest";

// Setup files run before test imports, so src/env.ts sees the mapped endpoints.
// Child processes also inherit these settings through process.env.
Object.assign(process.env, inject("containerEnv"));
