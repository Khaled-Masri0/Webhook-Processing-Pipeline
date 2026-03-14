import { createServer } from "node:http";
import { env } from "../config/env";
import { closeDb, prisma } from "../db/client";

const server = createServer(async (req, res) => {
  if (req.method === "GET" && req.url === "/health") {
    try {
      await prisma.$queryRaw`SELECT 1`;
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ status: "ok", database: "up" }));
      return;
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown database error";
      res.writeHead(503, { "content-type": "application/json" });
      res.end(JSON.stringify({ status: "degraded", database: "down", error: message }));
      return;
    }
  }

  res.writeHead(404, { "content-type": "application/json" });
  res.end(JSON.stringify({ error: "Not found" }));
});

async function shutdown(signal: string): Promise<void> {
  console.log(`Received ${signal}, shutting down API server...`);
  server.close(async () => {
    await closeDb();
    process.exit(0);
  });
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

server.listen(env.port, () => {
  console.log(`API listening on http://localhost:${env.port}`);
});
