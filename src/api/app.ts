import express, { Express } from "express";
import { prisma } from "../db/client";
import { prismaJobStore } from "../db/job-store";
import { prismaPipelineStore } from "../db/pipeline-store";
import { createJobQueryService, JobQueryService } from "../services/job-query-service";
import { createPipelineService, PipelineService } from "../services/pipeline-service";
import { createWebhookService, WebhookService } from "../services/webhook-service";
import { errorHandler, notFoundHandler } from "./middleware/error-handler";
import { createHealthRoutes } from "./routes/health-routes";
import { createJobRoutes } from "./routes/job-routes";
import { createPipelineRoutes } from "./routes/pipeline-routes";
import { createWebhookRoutes } from "./routes/webhook-routes";

const pipelineService = createPipelineService(prismaPipelineStore);
const webhookService = createWebhookService(prismaPipelineStore, prismaJobStore);
const jobQueryService = createJobQueryService(prismaJobStore);

export interface ApiDependencies {
  pipelineService: PipelineService;
  webhookService: WebhookService;
  jobQueryService: JobQueryService;
  healthcheck: () => Promise<void>;
}

const defaultApiDependencies: ApiDependencies = {
  pipelineService,
  webhookService,
  jobQueryService,
  healthcheck: async () => {
    await prisma.$queryRaw`SELECT 1`;
  },
};

export function createApiApp(dependencies: ApiDependencies = defaultApiDependencies): Express {
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
