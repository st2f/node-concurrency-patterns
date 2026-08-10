# node-concurrency-patterns

Practicing strategies for closing a classic concurrency race — see
[docs/PLAN.md](docs/PLAN.md) for the full writeup.

## Setup

```bash
npm ci
npm run docker:up   # Postgres (localhost:5433) + Redis (localhost:6379)
npm run migrate
npm run typecheck
npm test
```

- `npm run docker:down` stops and removes the containers. Postgres data is
  kept in the named `postgres_data` volume; Redis is intentionally
  ephemeral and starts empty the next time the containers are created.
- `npm run docker:reset` stops the containers **and deletes the Postgres
  volume**, dropping its schema and data. Re-run `docker:up` + `migrate`
  afterward to get back to a clean database.

Requires Node >=24.7 (see `.nvmrc`) — TypeScript files run directly via
Node's built-in type stripping, with `erasableSyntaxOnly` ensuring the
type checker rejects TypeScript syntax that Node cannot strip. There is no
build step.
