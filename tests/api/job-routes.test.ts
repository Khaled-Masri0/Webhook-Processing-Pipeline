import assert from "node:assert/strict";
import { createServer, Server } from "node:http";
import { AddressInfo } from "node:net";
import test from "node:test";
import { DeliveryStatus, JobStatus } from "@prisma/client";
import { ApiDependencies, createApiApp } from "../../src/api/app.js";
import {
  DeliveryAttemptDetails,
  JobDetails,
  JobHistoryQuery,
  JobQueryService,
  JobSummary,
  PaginatedResult,
} from "../../src/services/job-query-service.js";
import { PipelineService } from "../../src/services/pipeline-service.js";
import { QueuedJob, WebhookService } from "../../src/services/webhook-service.js";
import { NotFoundError } from "../../src/utils/errors.js";

function createPipelineServiceStub(): PipelineService {
  return {
    async listPipelines() {
      throw new Error("Not implemented in this test.");
    },
    async getPipeline() {
      throw new Error("Not implemented in this test.");
    },
    async createPipeline() {
      throw new Error("Not implemented in this test.");
    },
    async updatePipeline() {
      throw new Error("Not implemented in this test.");
    },
    async deletePipeline() {
      throw new Error("Not implemented in this test.");
    },
  };
}

function createWebhookServiceStub(): WebhookService {
  return {
    async enqueueWebhook(): Promise<QueuedJob> {
      throw new Error("Not implemented in this test.");
    },
  };
}

async function withApiServer(
  dependencies: ApiDependencies,
  run: (baseUrl: string) => Promise<void>,
): Promise<void> {
  const server = createServer(createApiApp(dependencies));

  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });

  const address = server.address() as AddressInfo;
  const baseUrl = `http://127.0.0.1:${address.port}`;

  try {
    await run(baseUrl);
  } finally {
    await closeServer(server);
  }
}

async function closeServer(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });
}

function buildJobSummary(overrides: Partial<JobSummary> = {}): JobSummary {
  return {
    id: "job-123",
    pipelineId: "pipeline-123",
    pipelineName: "Sales Lead Pipeline",
    status: JobStatus.COMPLETED,
    retryCount: 1,
    maxRetries: 5,
    nextRunAt: new Date("2026-03-24T10:00:00.000Z"),
    lockedAt: null,
    processedAt: new Date("2026-03-24T10:01:00.000Z"),
    lastError: null,
    createdAt: new Date("2026-03-24T09:59:00.000Z"),
    updatedAt: new Date("2026-03-24T10:01:00.000Z"),
    ...overrides,
  };
}

function buildJobDetails(overrides: Partial<JobDetails> = {}): JobDetails {
  return {
    ...buildJobSummary(),
    payload: { leadId: "lead-123" },
    result: { leadId: "lead-123", priority: "high" },
    pipelineSourcePath: "/webhooks/sales-leads",
    actionType: "ENRICH",
    pipelineActive: true,
    ...overrides,
  };
}

function buildDeliveryAttemptDetails(
  overrides: Partial<DeliveryAttemptDetails> = {},
): DeliveryAttemptDetails {
  return {
    id: "attempt-123",
    jobId: "job-123",
    subscriberId: "subscriber-123",
    subscriberUrl: "https://example.com/hooks/sales",
    attemptNumber: 2,
    status: DeliveryStatus.PENDING,
    nextRunAt: new Date("2026-03-24T10:02:00.000Z"),
    lockedAt: null,
    responseCode: 503,
    error: "Subscriber responded with HTTP 503.",
    deliveredAt: null,
    createdAt: new Date("2026-03-24T10:01:00.000Z"),
    ...overrides,
  };
}

function buildPaginatedResult<T>(items: T[]): PaginatedResult<T> {
  return {
    items,
    page: 1,
    pageSize: 20,
    totalItems: items.length,
    totalPages: items.length === 0 ? 0 : 1,
  };
}

test("job routes return a paginated job list with filters", async () => {
  const observedQueries: JobHistoryQuery[] = [];
  const service: JobQueryService = {
    async getJob() {
      throw new Error("Not used in this test.");
    },
    async listJobs(query) {
      observedQueries.push(query);
      return {
        items: [buildJobSummary()],
        page: query.page,
        pageSize: query.pageSize,
        totalItems: 7,
        totalPages: 2,
      };
    },
    async listJobDeliveries() {
      throw new Error("Not used in this test.");
    },
  };

  await withApiServer(
    {
      pipelineService: createPipelineServiceStub(),
      webhookService: createWebhookServiceStub(),
      jobQueryService: service,
      healthcheck: async () => undefined,
    },
    async (baseUrl) => {
      const response = await fetch(
        `${baseUrl}/jobs?pipelineId=pipeline-123&status=COMPLETED&createdAfter=2026-03-24T00:00:00.000Z&createdBefore=2026-03-25T00:00:00.000Z&page=2&pageSize=5`,
      );
      const body = await response.json();

      assert.equal(response.status, 200);
      assert.equal(observedQueries.length, 1);
      assert.equal(observedQueries[0]?.pipelineId, "pipeline-123");
      assert.equal(observedQueries[0]?.status, JobStatus.COMPLETED);
      assert.equal(observedQueries[0]?.page, 2);
      assert.equal(observedQueries[0]?.pageSize, 5);
      assert.equal(observedQueries[0]?.createdAfter?.toISOString(), "2026-03-24T00:00:00.000Z");
      assert.equal(observedQueries[0]?.createdBefore?.toISOString(), "2026-03-25T00:00:00.000Z");
      assert.equal(body.data[0].id, "job-123");
      assert.equal(body.meta.totalItems, 7);
      assert.equal(body.meta.totalPages, 2);
    },
  );
});

test("job routes return job details by id", async () => {
  const service: JobQueryService = {
    async getJob(jobId) {
      assert.equal(jobId, "job-123");
      return buildJobDetails();
    },
    async listJobs() {
      throw new Error("Not used in this test.");
    },
    async listJobDeliveries() {
      throw new Error("Not used in this test.");
    },
  };

  await withApiServer(
    {
      pipelineService: createPipelineServiceStub(),
      webhookService: createWebhookServiceStub(),
      jobQueryService: service,
      healthcheck: async () => undefined,
    },
    async (baseUrl) => {
      const response = await fetch(`${baseUrl}/jobs/job-123`);
      const body = await response.json();

      assert.equal(response.status, 200);
      assert.equal(body.data.id, "job-123");
      assert.equal(body.data.pipelineSourcePath, "/webhooks/sales-leads");
      assert.deepEqual(body.data.payload, { leadId: "lead-123" });
    },
  );
});

test("job routes return delivery history by job id", async () => {
  const service: JobQueryService = {
    async getJob() {
      throw new Error("Not used in this test.");
    },
    async listJobs() {
      throw new Error("Not used in this test.");
    },
    async listJobDeliveries(jobId, query) {
      assert.equal(jobId, "job-123");
      assert.equal(query.status, DeliveryStatus.FAILED);
      assert.equal(query.page, 1);
      assert.equal(query.pageSize, 10);
      return {
        items: [buildDeliveryAttemptDetails({ status: DeliveryStatus.FAILED })],
        page: query.page,
        pageSize: query.pageSize,
        totalItems: 1,
        totalPages: 1,
      };
    },
  };

  await withApiServer(
    {
      pipelineService: createPipelineServiceStub(),
      webhookService: createWebhookServiceStub(),
      jobQueryService: service,
      healthcheck: async () => undefined,
    },
    async (baseUrl) => {
      const response = await fetch(`${baseUrl}/jobs/job-123/deliveries?status=FAILED&page=1&pageSize=10`);
      const body = await response.json();

      assert.equal(response.status, 200);
      assert.equal(body.data[0].subscriberUrl, "https://example.com/hooks/sales");
      assert.equal(body.meta.totalItems, 1);
    },
  );
});

test("job routes reject invalid job filters", async () => {
  const service: JobQueryService = {
    async getJob() {
      throw new Error("Not used in this test.");
    },
    async listJobs() {
      throw new Error("Should not be called for invalid query.");
    },
    async listJobDeliveries() {
      throw new Error("Not used in this test.");
    },
  };

  await withApiServer(
    {
      pipelineService: createPipelineServiceStub(),
      webhookService: createWebhookServiceStub(),
      jobQueryService: service,
      healthcheck: async () => undefined,
    },
    async (baseUrl) => {
      const response = await fetch(`${baseUrl}/jobs?status=DONE&page=0`);
      const body = await response.json();

      assert.equal(response.status, 400);
      assert.equal(body.error.code, "VALIDATION_ERROR");
    },
  );
});

test("job routes reject createdAfter values later than createdBefore", async () => {
  const service: JobQueryService = {
    async getJob() {
      throw new Error("Not used in this test.");
    },
    async listJobs() {
      throw new Error("Should not be called for an invalid date range.");
    },
    async listJobDeliveries() {
      throw new Error("Not used in this test.");
    },
  };

  await withApiServer(
    {
      pipelineService: createPipelineServiceStub(),
      webhookService: createWebhookServiceStub(),
      jobQueryService: service,
      healthcheck: async () => undefined,
    },
    async (baseUrl) => {
      const response = await fetch(
        `${baseUrl}/jobs?createdAfter=2026-03-25T00:00:00.000Z&createdBefore=2026-03-24T00:00:00.000Z`,
      );
      const body = await response.json();

      assert.equal(response.status, 400);
      assert.equal(
        body.error.message,
        "createdAfter must be less than or equal to createdBefore.",
      );
    },
  );
});

test("job routes reject duplicate query parameters", async () => {
  const service: JobQueryService = {
    async getJob() {
      throw new Error("Not used in this test.");
    },
    async listJobs() {
      throw new Error("Should not be called for duplicate query parameters.");
    },
    async listJobDeliveries() {
      throw new Error("Not used in this test.");
    },
  };

  await withApiServer(
    {
      pipelineService: createPipelineServiceStub(),
      webhookService: createWebhookServiceStub(),
      jobQueryService: service,
      healthcheck: async () => undefined,
    },
    async (baseUrl) => {
      const response = await fetch(`${baseUrl}/jobs?status=COMPLETED&status=FAILED`);
      const body = await response.json();

      assert.equal(response.status, 400);
      assert.equal(body.error.message, "status must be provided only once.");
    },
  );
});

test("job routes reject invalid createdAfter values", async () => {
  const service: JobQueryService = {
    async getJob() {
      throw new Error("Not used in this test.");
    },
    async listJobs() {
      throw new Error("Should not be called for an invalid createdAfter value.");
    },
    async listJobDeliveries() {
      throw new Error("Not used in this test.");
    },
  };

  await withApiServer(
    {
      pipelineService: createPipelineServiceStub(),
      webhookService: createWebhookServiceStub(),
      jobQueryService: service,
      healthcheck: async () => undefined,
    },
    async (baseUrl) => {
      const response = await fetch(`${baseUrl}/jobs?createdAfter=not-a-date`);
      const body = await response.json();

      assert.equal(response.status, 400);
      assert.equal(body.error.message, "createdAfter must be a valid ISO-8601 date string.");
    },
  );
});

test("job routes reject invalid createdBefore values", async () => {
  const service: JobQueryService = {
    async getJob() {
      throw new Error("Not used in this test.");
    },
    async listJobs() {
      throw new Error("Should not be called for an invalid createdBefore value.");
    },
    async listJobDeliveries() {
      throw new Error("Not used in this test.");
    },
  };

  await withApiServer(
    {
      pipelineService: createPipelineServiceStub(),
      webhookService: createWebhookServiceStub(),
      jobQueryService: service,
      healthcheck: async () => undefined,
    },
    async (baseUrl) => {
      const response = await fetch(`${baseUrl}/jobs?createdBefore=not-a-date`);
      const body = await response.json();

      assert.equal(response.status, 400);
      assert.equal(body.error.message, "createdBefore must be a valid ISO-8601 date string.");
    },
  );
});

test("job routes reject duplicate createdAfter query parameters", async () => {
  const service: JobQueryService = {
    async getJob() {
      throw new Error("Not used in this test.");
    },
    async listJobs() {
      throw new Error("Should not be called for duplicate createdAfter values.");
    },
    async listJobDeliveries() {
      throw new Error("Not used in this test.");
    },
  };

  await withApiServer(
    {
      pipelineService: createPipelineServiceStub(),
      webhookService: createWebhookServiceStub(),
      jobQueryService: service,
      healthcheck: async () => undefined,
    },
    async (baseUrl) => {
      const response = await fetch(
        `${baseUrl}/jobs?createdAfter=2026-03-24T00:00:00.000Z&createdAfter=2026-03-25T00:00:00.000Z`,
      );
      const body = await response.json();

      assert.equal(response.status, 400);
      assert.equal(body.error.message, "createdAfter must be provided only once.");
    },
  );
});

test("job routes reject duplicate createdBefore query parameters", async () => {
  const service: JobQueryService = {
    async getJob() {
      throw new Error("Not used in this test.");
    },
    async listJobs() {
      throw new Error("Should not be called for duplicate createdBefore values.");
    },
    async listJobDeliveries() {
      throw new Error("Not used in this test.");
    },
  };

  await withApiServer(
    {
      pipelineService: createPipelineServiceStub(),
      webhookService: createWebhookServiceStub(),
      jobQueryService: service,
      healthcheck: async () => undefined,
    },
    async (baseUrl) => {
      const response = await fetch(
        `${baseUrl}/jobs?createdBefore=2026-03-24T00:00:00.000Z&createdBefore=2026-03-25T00:00:00.000Z`,
      );
      const body = await response.json();

      assert.equal(response.status, 400);
      assert.equal(body.error.message, "createdBefore must be provided only once.");
    },
  );
});

test("job routes reject page sizes above the configured maximum", async () => {
  const service: JobQueryService = {
    async getJob() {
      throw new Error("Not used in this test.");
    },
    async listJobs() {
      throw new Error("Should not be called for page sizes above the maximum.");
    },
    async listJobDeliveries() {
      throw new Error("Not used in this test.");
    },
  };

  await withApiServer(
    {
      pipelineService: createPipelineServiceStub(),
      webhookService: createWebhookServiceStub(),
      jobQueryService: service,
      healthcheck: async () => undefined,
    },
    async (baseUrl) => {
      const response = await fetch(`${baseUrl}/jobs?pageSize=101`);
      const body = await response.json();

      assert.equal(response.status, 400);
      assert.equal(body.error.message, "pageSize must be less than or equal to 100.");
    },
  );
});

test("job routes surface missing jobs as 404", async () => {
  const service: JobQueryService = {
    async getJob() {
      throw new NotFoundError("Job job-404 was not found.");
    },
    async listJobs() {
      throw new Error("Not used in this test.");
    },
    async listJobDeliveries() {
      throw new Error("Not used in this test.");
    },
  };

  await withApiServer(
    {
      pipelineService: createPipelineServiceStub(),
      webhookService: createWebhookServiceStub(),
      jobQueryService: service,
      healthcheck: async () => undefined,
    },
    async (baseUrl) => {
      const response = await fetch(`${baseUrl}/jobs/job-404`);
      const body = await response.json();

      assert.equal(response.status, 404);
      assert.equal(body.error.message, "Job job-404 was not found.");
    },
  );
});

test("job routes surface missing jobs as 404 for delivery history too", async () => {
  const service: JobQueryService = {
    async getJob() {
      throw new Error("Not used in this test.");
    },
    async listJobs() {
      throw new Error("Not used in this test.");
    },
    async listJobDeliveries() {
      throw new NotFoundError("Job job-404 was not found.");
    },
  };

  await withApiServer(
    {
      pipelineService: createPipelineServiceStub(),
      webhookService: createWebhookServiceStub(),
      jobQueryService: service,
      healthcheck: async () => undefined,
    },
    async (baseUrl) => {
      const response = await fetch(`${baseUrl}/jobs/job-404/deliveries`);
      const body = await response.json();

      assert.equal(response.status, 404);
      assert.equal(body.error.message, "Job job-404 was not found.");
    },
  );
});

test("job routes use default pagination metadata when the result set is empty", async () => {
  const service: JobQueryService = {
    async getJob() {
      throw new Error("Not used in this test.");
    },
    async listJobs() {
      return buildPaginatedResult([]);
    },
    async listJobDeliveries() {
      throw new Error("Not used in this test.");
    },
  };

  await withApiServer(
    {
      pipelineService: createPipelineServiceStub(),
      webhookService: createWebhookServiceStub(),
      jobQueryService: service,
      healthcheck: async () => undefined,
    },
    async (baseUrl) => {
      const response = await fetch(`${baseUrl}/jobs`);
      const body = await response.json();

      assert.equal(response.status, 200);
      assert.deepEqual(body.data, []);
      assert.equal(body.meta.page, 1);
      assert.equal(body.meta.pageSize, 20);
      assert.equal(body.meta.totalItems, 0);
      assert.equal(body.meta.totalPages, 0);
    },
  );
});
