import { JobStatus } from "@prisma/client";
import { NotFoundError } from "../utils/errors.js";
import { JsonValue } from "../utils/json.js";

export interface WebhookPipeline {
  id: string;
  sourcePath: string;
  active: boolean;
}

export interface EnqueueJobInput {
  pipelineId: string;
  payload: JsonValue;
}

export interface QueuedJob {
  id: string;
  pipelineId: string;
  status: JobStatus;
  retryCount: number;
  maxRetries: number;
  nextRunAt: Date;
  createdAt: Date;
}

export interface WebhookPipelineStore {
  findBySourcePath(sourcePath: string): Promise<WebhookPipeline | null>;
}

export interface JobStore {
  create(input: EnqueueJobInput): Promise<QueuedJob>;
}

export interface WebhookService {
  enqueueWebhook(sourcePath: string, payload: JsonValue): Promise<QueuedJob>;
}

export function createWebhookService(
  pipelineStore: WebhookPipelineStore,
  jobStore: JobStore,
): WebhookService {
  return {
    async enqueueWebhook(sourcePath: string, payload: JsonValue): Promise<QueuedJob> {
      const pipeline = await pipelineStore.findBySourcePath(sourcePath);

      if (!pipeline || !pipeline.active) {
        throw new NotFoundError(`Webhook source ${sourcePath} was not found.`);
      }

      return jobStore.create({
        pipelineId: pipeline.id,
        payload,
      });
    },
  };
}
