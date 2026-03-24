import assert from "node:assert/strict";
import { createServer, Server } from "node:http";
import { AddressInfo } from "node:net";
import test from "node:test";
import { createApiApp, ApiDependencies } from "../../src/api/app.js";
import { JobQueryService } from "../../src/services/job-query-service.js";
import { PipelineService } from "../../src/services/pipeline-service.js";
import { QueuedJob } from "../../src/services/webhook-service.js";
import { NotFoundError } from "../../src/utils/errors.js";
import { JsonValue } from "../../src/utils/json.js";

function createPipelineServiceStub(): PipelineService {
  return {
    async listPipelines() {
      throw new Error("Unexpected call in this test.");
    },
    async getPipeline() {
      throw new Error("Unexpected call in this test.");
    },
    async createPipeline() {
      throw new Error("Unexpected call in this test.");
    },
    async updatePipeline() {
      throw new Error("Unexpected call in this test.");
    },
    async deletePipeline() {
      throw new Error("Unexpected call in this test.");
    },
  };
}

function createJobQueryServiceStub(): JobQueryService {
  return {
    async getJob() {
      throw new Error("Unexpected call in this test.");
    },
    async listJobs() {
      throw new Error("Unexpected call in this test.");
    },
    async listJobDeliveries() {
      throw new Error("Unexpected call in this test.");
    },
  };
}

function buildQueuedJob(): QueuedJob {
  const now = new Date("2026-03-21T12:00:00.000Z");

  return {
    id: "job-123",
    pipelineId: "pipeline-123",
    status: "PENDING",
    retryCount: 0,
    maxRetries: 5,
    nextRunAt: now,
    createdAt: now,
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

test("webhook route enqueues jobs and returns 202", async () => {
  const observedPayloads: JsonValue[] = [];

  await withApiServer(
    {
      pipelineService: createPipelineServiceStub(),
      jobQueryService: createJobQueryServiceStub(),
      webhookService: {
        async enqueueWebhook(sourcePath: string, payload: JsonValue): Promise<QueuedJob> {
          assert.equal(sourcePath, "/webhooks/order-events");
          observedPayloads.push(payload);
          return buildQueuedJob();
        },
      },
      healthcheck: async () => undefined,
    },
    async (baseUrl) => {
      const response = await fetch(`${baseUrl}/webhooks/order-events`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ eventId: "evt-123", total: 149, source: "web-form" }),
      });

      const body = await response.text();

      assert.equal(response.status, 202);
      assert.deepEqual(observedPayloads[0], {
        eventId: "evt-123",
        total: 149,
        source: "web-form",
      });
      assert.match(body, /job-123/);
    },
  );
});

test("webhook route returns 404 for unknown source paths", async () => {
  await withApiServer(
    {
      pipelineService: createPipelineServiceStub(),
      jobQueryService: createJobQueryServiceStub(),
      webhookService: {
        async enqueueWebhook(): Promise<QueuedJob> {
          throw new NotFoundError("Webhook source /webhooks/missing was not found.");
        },
      },
      healthcheck: async () => undefined,
    },
    async (baseUrl) => {
      const response = await fetch(`${baseUrl}/webhooks/missing`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ event: "missing" }),
      });

      const body = await response.text();

      assert.equal(response.status, 404);
      assert.match(body, /Webhook source \/webhooks\/missing was not found/);
    },
  );
});

test("webhook route rejects empty request bodies", async () => {
  await withApiServer(
    {
      pipelineService: createPipelineServiceStub(),
      jobQueryService: createJobQueryServiceStub(),
      webhookService: {
        async enqueueWebhook(): Promise<QueuedJob> {
          throw new Error("Should not be called for empty body.");
        },
      },
      healthcheck: async () => undefined,
    },
    async (baseUrl) => {
      const response = await fetch(`${baseUrl}/webhooks/order-events`, {
        method: "POST",
      });

      const body = await response.text();

      assert.equal(response.status, 400);
      assert.match(body, /Request body is required/);
    },
  );
});
