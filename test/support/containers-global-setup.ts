import { PostgreSqlContainer } from "@testcontainers/postgresql";
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers";
import type { TestProject } from "vitest/node";

declare module "vitest" {
  export interface ProvidedContext {
    containerEnv: Record<string, string>;
  }
}

/** Own one pair of services for the suite; individual tests still isolate data. */
export default async function setup(project: TestProject) {
  const containers: StartedTestContainer[] = [];

  async function stopContainers(): Promise<void> {
    // Attempt every stop even if one container cannot be removed.
    const results = await Promise.allSettled(
      containers.map((container) => container.stop()),
    );
    const errors = results
      .filter((result) => result.status === "rejected")
      .map((result) => result.reason);
    if (errors.length > 0) {
      throw new AggregateError(errors, "Failed to stop test containers");
    }
  }

  try {
    // The Postgres module waits for its health check and forwarded port.
    const postgres = await new PostgreSqlContainer("postgres:16-alpine")
      .withDatabase("concurrency")
      .withUsername("app")
      .withPassword("app")
      .start();
    containers.push(postgres);

    const redis = await new GenericContainer("redis:7-alpine")
      .withExposedPorts(6379)
      .withWaitStrategy(
        Wait.forAll([
          Wait.forLogMessage("Ready to accept connections"),
          Wait.forListeningPorts(),
        ]),
      )
      .withStartupTimeout(120_000)
      .start();
    containers.push(redis);

    project.provide("containerEnv", {
      PGHOST: postgres.getHost(),
      PGPORT: String(postgres.getPort()),
      PGUSER: postgres.getUsername(),
      PGPASSWORD: postgres.getPassword(),
      PGDATABASE: postgres.getDatabase(),
      REDIS_HOST: redis.getHost(),
      REDIS_PORT: String(redis.getMappedPort(6379)),
    });

    return stopContainers;
  } catch (error) {
    try {
      await stopContainers();
    } catch (cleanupError) {
      throw new AggregateError(
        [error, cleanupError],
        "Test container setup and cleanup failed",
      );
    }
    throw error;
  }
}
