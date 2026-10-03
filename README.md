# node-concurrency-patterns

A learning project exploring different ways to protect shared state when concurrent Node.js operations race to modify the same resource.

The project uses a simple locker checkout invariant: **a locker can have at most one active checkout**. Concurrent requests deliberately race against that invariant, then several strategies are implemented and tested to understand where each one provides its guarantee.

The strategies include:

- in-process keyed mutexes
- PostgreSQL advisory locks with `READ COMMITTED` isolation
- Redis-based distributed locks
- optimistic concurrency control
- PostgreSQL constraints

The goal is not just to make the race disappear, but to understand **why each strategy works, what its coordination boundary is, and what changes when the application runs across multiple Node.js processes**.

See also [docs/PLAN.md](docs/PLAN.md) for the full progression and [docs/tests-overview.md](docs/tests-overview.md).

## Setup

```bash
npm ci
npm run docker:up   # Postgres (localhost:5433) + Redis (localhost:6379)
npm run migrate
npm run typecheck
npm test
```

- `npm run docker:down` stops and removes the containers. Postgres data is kept in the named `postgres_data` volume; Redis is intentionally ephemeral and starts empty the next time the containers are created.
- `npm run docker:reset` stops the containers **and deletes the Postgres volume**, dropping its schema and data. Re-run `docker:up` + `migrate` afterward to get back to a clean database.

Requires Node >=24.7 (see `.nvmrc`). Runtime commands such as `migrate` execute TypeScript directly through Node's built-in type stripping; Vitest transforms test files and their imports. The `erasableSyntaxOnly` compiler option keeps the application source compatible with Node's native execution. There is no build step.

## Running tests

Choose the command for your database and container runtime:

| Command | Services |
| --- | --- |
| `npm test` | Existing PostgreSQL and Redis, such as those started by `npm run docker:up` |
| `npm run test:containers` | Temporary PostgreSQL and Redis containers; requires a running Docker daemon |
| `npm run test:containers:colima` | The same temporary containers, with connection settings for Colima's default profile |

The container configuration starts one PostgreSQL (`postgres:16-alpine`) and one Redis (`redis:7-alpine`) container per suite, waits for readiness, and uses dynamically assigned ports. It removes the containers after the suite. Each test creates its own migrated database schema and namespaces Redis keys where needed, so no separate `docker:up` or `migrate` step is required for container tests. The first run may take longer while Docker downloads images.
