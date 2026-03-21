import { createServer, IncomingMessage, ServerResponse } from "node:http";
import { env } from "../config/env";
import { closeDb, prisma } from "../db/client";
import { prismaPipelineStore } from "../db/pipeline-store";
import { createPipelineService } from "../services/pipeline-service";
import { sendError, sendJson, readJsonBody, parseResourceId } from "../utils/http";
import { parsePipelineInput } from "../utils/pipeline-validation";

const pipelineService = createPipelineService(prismaPipelineStore);

export async function handleRequest(request: IncomingMessage, response: ServerResponse): Promise<void> {
  try {
    const requestUrl = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);
    const { method = "GET" } = request;

    if (method === "GET" && requestUrl.pathname === "/health") {
      await prisma.$queryRaw`SELECT 1`;
      sendJson(response, 200, { data: { status: "ok", database: "up" } });
      return;
    }

    if (requestUrl.pathname === "/pipelines") {
      if (method === "GET") {
        const pipelines = await pipelineService.listPipelines();
        sendJson(response, 200, { data: pipelines });
        return;
      }

      if (method === "POST") {
        const payload = await readJsonBody(request);
        const pipeline = await pipelineService.createPipeline(parsePipelineInput(payload));
        sendJson(response, 201, { data: pipeline });
        return;
      }
    }

    const pipelineId = parseResourceId(requestUrl.pathname, "/pipelines");
    if (pipelineId) {
      if (method === "GET") {
        const pipeline = await pipelineService.getPipeline(pipelineId);
        sendJson(response, 200, { data: pipeline });
        return;
      }

      if (method === "PUT") {
        const payload = await readJsonBody(request);
        const pipeline = await pipelineService.updatePipeline(pipelineId, parsePipelineInput(payload));
        sendJson(response, 200, { data: pipeline });
        return;
      }

      if (method === "DELETE") {
        const pipeline = await pipelineService.deletePipeline(pipelineId);
        sendJson(response, 200, { data: pipeline });
        return;
      }
    }

    sendJson(response, 404, {
      error: {
        code: "NOT_FOUND",
        message: "Route not found.",
      },
    });
  } catch (error) {
    sendError(response, error);
  }
}

export function createApiServer() {
  return createServer((request, response) => {
    void handleRequest(request, response);
  });
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
