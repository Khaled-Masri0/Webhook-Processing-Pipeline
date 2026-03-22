import { createServer } from "node:http";
import { env } from "../config/env";
import { closeDb } from "../db/client";
import { createApiApp } from "./app";

export function createApiServer() {
  return createServer(createApiApp());
}

const server = createApiServer();

async function shutdown(signal: string): Promise<void> {
  console.log(`Received ${signal}, shutting down API server...`);
  server.close(async () => {
    await closeDb();
    process.exit(0);
  });
}

if (require.main === module) {
  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));

  server.listen(env.port, () => {
    console.log(`API listening on http://localhost:${env.port}`);
  });
}
