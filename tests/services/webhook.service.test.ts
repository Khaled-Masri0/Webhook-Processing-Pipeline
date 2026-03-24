import test from "node:test";
import assert from "node:assert/strict";
import { JobStatus } from "@prisma/client";
import {
  createWebhookService,
  EnqueueJobInput,
  JobStore,
  QueuedJob,
  WebhookPipeline,
  WebhookPipelineStore,
} from "../../src/services/webhook-service.js";
import { NotFoundError } from "../../src/utils/errors.js";
import { JsonValue } from "../../src/utils/json.js";

class InMemoryWebhookPipelineStore implements WebhookPipelineStore {
  constructor(private readonly pipelines: WebhookPipeline[]) {}

  async findBySourcePath(sourcePath: string): Promise<WebhookPipeline | null> {
    return this.pipelines.find((pipeline) => pipeline.sourcePath === sourcePath) ?? null;
  }
}

class InMemoryJobStore implements JobStore {
  public readonly jobs: QueuedJob[] = [];
  public payloads: JsonValue[] = [];

  async create(input: EnqueueJobInput): Promise<QueuedJob> {
    this.payloads.push(input.payload);

    const job: QueuedJob = {
      id: `job-${this.jobs.length + 1}`,
      pipelineId: input.pipelineId,
      status: JobStatus.PENDING,
      retryCount: 0,
      maxRetries: 5,
      nextRunAt: new Date(),
      createdAt: new Date(),
    };

    this.jobs.push(job);
    return job;
  }
}

test("webhook service enqueues a pending job for an active pipeline", async () => {
  const pipelineStore = new InMemoryWebhookPipelineStore([
    { id: "pipeline-1", sourcePath: "/webhooks/order-events", active: true },
  ]);
  const jobStore = new InMemoryJobStore();
  const service = createWebhookService(pipelineStore, jobStore);

  const queuedJob = await service.enqueueWebhook("/webhooks/order-events", {
    eventId: "evt-123",
    total: 42,
  });

  assert.equal(queuedJob.pipelineId, "pipeline-1");
  assert.equal(queuedJob.status, JobStatus.PENDING);
  assert.deepEqual(jobStore.payloads[0], {
    eventId: "evt-123",
    total: 42,
  });
});

test("webhook service accepts arbitrary JSON payloads", async () => {
  const pipelineStore = new InMemoryWebhookPipelineStore([
    { id: "pipeline-1", sourcePath: "/webhooks/order-events", active: true },
  ]);
  const jobStore = new InMemoryJobStore();
  const service = createWebhookService(pipelineStore, jobStore);

  await service.enqueueWebhook("/webhooks/order-events", ["evt-123", true, 5]);

  assert.deepEqual(jobStore.payloads[0], ["evt-123", true, 5]);
});

test("webhook service rejects unknown or inactive source paths", async () => {
  const service = createWebhookService(
    new InMemoryWebhookPipelineStore([
      { id: "pipeline-1", sourcePath: "/webhooks/inactive", active: false },
    ]),
    new InMemoryJobStore(),
  );

  await assert.rejects(
    () => service.enqueueWebhook("/webhooks/missing", { hello: "world" }),
    NotFoundError,
  );
  await assert.rejects(
    () => service.enqueueWebhook("/webhooks/inactive", { hello: "world" }),
    NotFoundError,
  );
});
