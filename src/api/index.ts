import { createServer } from "node:http";
import { pathToFileURL } from "node:url";
import { env } from "../config/env.js";
import { closeDb } from "../db/client.js";
import { createApiApp } from "./app.js";

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

const isDirectExecution =
  process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isDirectExecution) {
  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));

  server.listen(env.port, () => {
    console.log(`API listening on http://localhost:${env.port}`);
  });
}
