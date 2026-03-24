import { prisma } from "../db/client.js";
import { prismaJobStore } from "../db/job-store.js";
import { prismaPipelineStore } from "../db/pipeline-store.js";
import { createJobQueryService } from "../services/job-query-service.js";
import { createPipelineService } from "../services/pipeline-service.js";
import { createWebhookService } from "../services/webhook-service.js";
import { ApiDependencies } from "./app.js";

export function createDefaultApiDependencies(): ApiDependencies {
  const pipelineService = createPipelineService(prismaPipelineStore);
  const webhookService = createWebhookService(prismaPipelineStore, prismaJobStore);
  const jobQueryService = createJobQueryService(prismaJobStore);

  return {
    pipelineService,
    webhookService,
    jobQueryService,
    healthcheck: async () => {
      await prisma.$queryRaw`SELECT 1`;
    },
  };
}
