import express, { Express } from "express";
import { JobQueryService } from "../services/job-query-service.js";
import { PipelineService } from "../services/pipeline-service.js";
import { WebhookService } from "../services/webhook-service.js";
import { errorHandler, notFoundHandler } from "./middleware/error-handler.js";
import { createHealthRoutes } from "./routes/health-routes.js";
import { createJobRoutes } from "./routes/job-routes.js";
import { createPipelineRoutes } from "./routes/pipeline-routes.js";
import { createWebhookRoutes } from "./routes/webhook-routes.js";

export interface ApiDependencies {
  pipelineService: PipelineService;
  webhookService: WebhookService;
  jobQueryService: JobQueryService;
  healthcheck: () => Promise<void>;
}

export function createApiApp(dependencies: ApiDependencies): Express {
  const app = express();

  app.use(express.json({ limit: "1mb", type: "*/*" }));

  app.use("/health", createHealthRoutes(dependencies));
  app.use("/pipelines", createPipelineRoutes(dependencies));
  app.use("/jobs", createJobRoutes(dependencies));
  app.use(createWebhookRoutes(dependencies));

  app.use(notFoundHandler);
  app.use(errorHandler);

  return app;
}
