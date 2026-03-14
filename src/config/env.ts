import { config } from "dotenv";

config();

function getEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

export const env = {
  databaseUrl: getEnv("DATABASE_URL"),
  port: Number(process.env.PORT ?? 3000),
  workerPollMs: Number(process.env.WORKER_POLL_MS ?? 5000),
};
