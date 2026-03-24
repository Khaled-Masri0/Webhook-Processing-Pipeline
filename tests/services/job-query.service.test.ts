import assert from "node:assert/strict";
import test from "node:test";
import { DeliveryStatus, JobStatus } from "@prisma/client";
import {
  createJobQueryService,
  DeliveryAttemptDetails,
  DeliveryHistoryQuery,
  JobDetails,
  JobHistoryQuery,
  JobQueryStore,
  JobSummary,
  PaginatedResult,
} from "../../src/services/job-query-service.js";
import { NotFoundError } from "../../src/utils/errors.js";

class InMemoryJobQueryStore implements JobQueryStore {
  constructor(
    private readonly job: JobDetails | null,
    private readonly jobs: PaginatedResult<JobSummary>,
    private readonly deliveries: PaginatedResult<DeliveryAttemptDetails>,
  ) {}

  observedJobQuery: JobHistoryQuery | null = null;
  observedDeliveryQuery: DeliveryHistoryQuery | null = null;

  async findJobById(): Promise<JobDetails | null> {
    return this.job;
  }

  async listJobs(query: JobHistoryQuery): Promise<PaginatedResult<JobSummary>> {
    this.observedJobQuery = query;
    return this.jobs;
  }

  async listDeliveryAttempts(
    _jobId: string,
    query: DeliveryHistoryQuery,
  ): Promise<PaginatedResult<DeliveryAttemptDetails>> {
    this.observedDeliveryQuery = query;
    return this.deliveries;
  }
}

function buildJobSummary(): JobSummary {
  return {
    id: "job-123",
    pipelineId: "pipeline-123",
    pipelineName: "Order Events Pipeline",
    status: JobStatus.COMPLETED,
    retryCount: 0,
    maxRetries: 5,
    nextRunAt: new Date("2026-03-24T10:00:00.000Z"),
    lockedAt: null,
    processedAt: new Date("2026-03-24T10:01:00.000Z"),
    lastError: null,
    createdAt: new Date("2026-03-24T09:59:00.000Z"),
    updatedAt: new Date("2026-03-24T10:01:00.000Z"),
  };
}

function buildJobDetails(): JobDetails {
  return {
    ...buildJobSummary(),
    payload: { eventId: "evt-123", total: 149 },
    result: { eventId: "evt-123", priority: "high" },
    pipelineSourcePath: "/webhooks/order-events",
    actionType: "ENRICH",
    pipelineActive: true,
  };
}

function buildDeliveryAttemptDetails(): DeliveryAttemptDetails {
  return {
    id: "attempt-123",
    jobId: "job-123",
    subscriberId: "subscriber-123",
    subscriberUrl: "https://example.com/hooks/orders",
    attemptNumber: 2,
    status: DeliveryStatus.FAILED,
    nextRunAt: new Date("2026-03-24T10:02:00.000Z"),
    lockedAt: null,
    responseCode: 503,
    error: "Subscriber responded with HTTP 503.",
    deliveredAt: null,
    createdAt: new Date("2026-03-24T10:01:00.000Z"),
  };
}

test("job query service returns a job by id", async () => {
  const store = new InMemoryJobQueryStore(
    buildJobDetails(),
    {
      items: [buildJobSummary()],
      page: 1,
      pageSize: 20,
      totalItems: 1,
      totalPages: 1,
    },
    {
      items: [buildDeliveryAttemptDetails()],
      page: 1,
      pageSize: 20,
      totalItems: 1,
      totalPages: 1,
    },
  );
  const service = createJobQueryService(store);

  const job = await service.getJob("job-123");

  assert.equal(job.id, "job-123");
  assert.equal(job.pipelineSourcePath, "/webhooks/order-events");
});

test("job query service throws when a job is missing", async () => {
  const store = new InMemoryJobQueryStore(
    null,
    {
      items: [],
      page: 1,
      pageSize: 20,
      totalItems: 0,
      totalPages: 0,
    },
    {
      items: [],
      page: 1,
      pageSize: 20,
      totalItems: 0,
      totalPages: 0,
    },
  );
  const service = createJobQueryService(store);

  await assert.rejects(() => service.getJob("job-404"), NotFoundError);
});

test("job query service lists jobs with filters", async () => {
  const store = new InMemoryJobQueryStore(
    buildJobDetails(),
    {
      items: [buildJobSummary()],
      page: 2,
      pageSize: 5,
      totalItems: 7,
      totalPages: 2,
    },
    {
      items: [buildDeliveryAttemptDetails()],
      page: 1,
      pageSize: 20,
      totalItems: 1,
      totalPages: 1,
    },
  );
  const service = createJobQueryService(store);
  const query: JobHistoryQuery = {
    pipelineId: "pipeline-123",
    status: JobStatus.COMPLETED,
    createdAfter: new Date("2026-03-24T00:00:00.000Z"),
    createdBefore: new Date("2026-03-25T00:00:00.000Z"),
    page: 2,
    pageSize: 5,
  };

  const jobs = await service.listJobs(query);

  assert.equal(jobs.totalItems, 7);
  assert.deepEqual(store.observedJobQuery, query);
});

test("job query service lists deliveries for an existing job", async () => {
  const store = new InMemoryJobQueryStore(
    buildJobDetails(),
    {
      items: [buildJobSummary()],
      page: 1,
      pageSize: 20,
      totalItems: 1,
      totalPages: 1,
    },
    {
      items: [buildDeliveryAttemptDetails()],
      page: 1,
      pageSize: 10,
      totalItems: 1,
      totalPages: 1,
    },
  );
  const service = createJobQueryService(store);
  const query: DeliveryHistoryQuery = {
    status: DeliveryStatus.FAILED,
    page: 1,
    pageSize: 10,
  };

  const deliveries = await service.listJobDeliveries("job-123", query);

  assert.equal(deliveries.items[0]?.subscriberId, "subscriber-123");
  assert.deepEqual(store.observedDeliveryQuery, query);
});
