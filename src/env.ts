export const env = {
  pg: {
    host: process.env.PGHOST ?? "localhost",
    port: Number(process.env.PGPORT ?? 5433),
    user: process.env.PGUSER ?? "app",
    password: process.env.PGPASSWORD ?? "app",
    database: process.env.PGDATABASE ?? "concurrency",
  },
  redis: {
    host: process.env.REDIS_HOST ?? "localhost",
    port: Number(process.env.REDIS_PORT ?? 6379),
  },
};
