import assert from "node:assert/strict";
import test from "node:test";
import { JobStatus } from "@prisma/client";
import {
  createJobProcessingService,
  ClaimedJob,
  JobProcessingStore,
  ReadyJob,
} from "../../src/services/job-processing-service.js";
import { JsonValue } from "../../src/utils/json.js";

interface StoredJob extends ReadyJob {
  status: JobStatus;
  lockedAt: Date | null;
  result: JsonValue | null;
  processedAt: Date | null;
  lastError: string | null;
}

class InMemoryJobProcessingStore implements JobProcessingStore {
  private readonly contentionBudget = new Map<string, number>();

  constructor(private readonly jobs: StoredJob[]) {}

  simulateContention(jobId: string, failures = 1): void {
    this.contentionBudget.set(jobId, failures);
  }

  async findNextReadyJob(now: Date): Promise<ReadyJob | null> {
    const nextJob = this.jobs
      .filter((job) => job.status === JobStatus.PENDING && job.lockedAt === null)
      .filter((job) => job.nextRunAt.getTime() <= now.getTime())
      .sort((left, right) => {
        if (left.nextRunAt.getTime() !== right.nextRunAt.getTime()) {
          return left.nextRunAt.getTime() - right.nextRunAt.getTime();
        }

        return left.createdAt.getTime() - right.createdAt.getTime();
      })[0];

    if (!nextJob) {
      return null;
    }

    return {
      id: nextJob.id,
      pipelineId: nextJob.pipelineId,
      payload: nextJob.payload,
      actionType: nextJob.actionType,
      actionConfig: nextJob.actionConfig,
      retryCount: nextJob.retryCount,
      maxRetries: nextJob.maxRetries,
      nextRunAt: nextJob.nextRunAt,
      createdAt: nextJob.createdAt,
    };
  }

  async markJobProcessing(jobId: string, lockedAt: Date): Promise<boolean> {
    const pendingContention = this.contentionBudget.get(jobId) ?? 0;

    if (pendingContention > 0) {
      this.contentionBudget.set(jobId, pendingContention - 1);

      const contendedJob = this.jobs.find((job) => job.id === jobId);
      if (contendedJob) {
        contendedJob.status = JobStatus.PROCESSING;
        contendedJob.lockedAt = lockedAt;
      }

      return false;
    }

    const job = this.jobs.find((entry) => entry.id === jobId);

    if (!job || job.status !== JobStatus.PENDING || job.lockedAt !== null) {
      return false;
    }

    if (job.nextRunAt.getTime() > lockedAt.getTime()) {
      return false;
    }

    job.status = JobStatus.PROCESSING;
    job.lockedAt = lockedAt;
    return true;
  }

  async markJobCompleted(jobId: string, result: JsonValue | null, processedAt: Date): Promise<boolean> {
    const job = this.jobs.find((entry) => entry.id === jobId);

    if (!job || job.status !== JobStatus.PROCESSING) {
      return false;
    }

    job.status = JobStatus.COMPLETED;
    job.result = result;
    job.processedAt = processedAt;
    job.lastError = null;
    job.lockedAt = null;
    return true;
  }

  async markJobFailed(jobId: string, lastError: string, processedAt: Date): Promise<boolean> {
    const job = this.jobs.find((entry) => entry.id === jobId);

    if (!job || job.status !== JobStatus.PROCESSING) {
      return false;
    }

    job.status = JobStatus.FAILED;
    job.result = null;
    job.processedAt = processedAt;
    job.lastError = lastError;
    job.lockedAt = null;
    return true;
  }

  async rescheduleJob(
    jobId: string,
    lastError: string,
    nextRunAt: Date,
    retryCount: number,
  ): Promise<boolean> {
    const job = this.jobs.find((entry) => entry.id === jobId);

    if (!job || job.status !== JobStatus.PROCESSING) {
      return false;
    }

    job.status = JobStatus.PENDING;
    job.result = null;
    job.retryCount = retryCount;
    job.nextRunAt = nextRunAt;
    job.processedAt = null;
    job.lastError = lastError;
    job.lockedAt = null;
    return true;
  }

  getJob(jobId: string): StoredJob | undefined {
    return this.jobs.find((job) => job.id === jobId);
  }
}

function buildStoredJob(overrides: Partial<StoredJob> = {}): StoredJob {
  return {
    id: "job-1",
    pipelineId: "pipeline-1",
    payload: { leadId: "lead-123" },
    actionType: "TRANSFORM",
    actionConfig: {
      fields: {
        leadId: "leadId",
      },
    },
    status: JobStatus.PENDING,
    retryCount: 0,
    maxRetries: 5,
    nextRunAt: new Date("2026-03-22T10:00:00.000Z"),
    createdAt: new Date("2026-03-22T09:59:00.000Z"),
    lockedAt: null,
    result: null,
    processedAt: null,
    lastError: null,
    ...overrides,
  };
}

function assertClaimedJob(job: ClaimedJob | null): ClaimedJob {
  if (!job) {
    throw new Error("Expected a claimed job.");
  }

  return job;
}

test("job processing service claims the next ready pending job", async () => {
  const now = new Date("2026-03-22T10:05:00.000Z");
  const store = new InMemoryJobProcessingStore([
    buildStoredJob(),
    buildStoredJob({
      id: "job-2",
      nextRunAt: new Date("2026-03-22T11:00:00.000Z"),
      createdAt: new Date("2026-03-22T10:01:00.000Z"),
    }),
  ]);
  const service = createJobProcessingService(store);

  const claimedJob = await service.claimNextReadyJob(now);

  assert.equal(claimedJob?.id, "job-1");
  assert.equal(claimedJob?.status, JobStatus.PROCESSING);
  assert.equal(claimedJob?.lockedAt.getTime(), now.getTime());
  assert.equal(store.getJob("job-1")?.status, JobStatus.PROCESSING);
});

test("job processing service returns null when no job is ready", async () => {
  const now = new Date("2026-03-22T10:05:00.000Z");
  const store = new InMemoryJobProcessingStore([
    buildStoredJob({
      id: "job-2",
      nextRunAt: new Date("2026-03-22T11:00:00.000Z"),
    }),
  ]);
  const service = createJobProcessingService(store);

  const claimedJob = await service.claimNextReadyJob(now);

  assert.equal(claimedJob, null);
  assert.equal(store.getJob("job-2")?.status, JobStatus.PENDING);
});

test("job processing service skips contended jobs and claims the next ready one", async () => {
  const now = new Date("2026-03-22T10:05:00.000Z");
  const store = new InMemoryJobProcessingStore([
    buildStoredJob({ id: "job-1" }),
    buildStoredJob({
      id: "job-2",
      createdAt: new Date("2026-03-22T10:00:00.000Z"),
    }),
  ]);
  store.simulateContention("job-1");
  const service = createJobProcessingService(store);

  const claimedJob = await service.claimNextReadyJob(now);

  assert.equal(claimedJob?.id, "job-2");
  assert.equal(store.getJob("job-1")?.status, JobStatus.PROCESSING);
  assert.equal(store.getJob("job-2")?.status, JobStatus.PROCESSING);
});

test("job processing service completes a claimed job and stores the action result", async () => {
  const claimTime = new Date("2026-03-22T10:05:00.000Z");
  const processedAt = new Date("2026-03-22T10:05:05.000Z");
  const store = new InMemoryJobProcessingStore([
    buildStoredJob({
      actionType: "TRANSFORM",
      actionConfig: {
        fields: {
          leadId: "lead.id",
          email: "contact.email",
        },
      },
      payload: {
        lead: { id: "lead-123" },
        contact: { email: "owner@example.com" },
      },
    }),
  ]);
  const service = createJobProcessingService(store);

  const claimedJob = assertClaimedJob(await service.claimNextReadyJob(claimTime));
  const processedJob = await service.processClaimedJob(claimedJob, processedAt);

  assert.equal(processedJob.status, JobStatus.COMPLETED);
  assert.equal(processedJob.actionStatus, "SUCCESS");
  assert.deepEqual(processedJob.result, {
    leadId: "lead-123",
    email: "owner@example.com",
  });
  assert.equal(store.getJob("job-1")?.status, JobStatus.COMPLETED);
  assert.deepEqual(store.getJob("job-1")?.result, {
    leadId: "lead-123",
    email: "owner@example.com",
  });
  assert.equal(store.getJob("job-1")?.processedAt?.getTime(), processedAt.getTime());
  assert.equal(store.getJob("job-1")?.lastError, null);
  assert.equal(store.getJob("job-1")?.lockedAt, null);
});

test("job processing service treats filtered-out jobs as completed with a null result", async () => {
  const claimTime = new Date("2026-03-22T10:05:00.000Z");
  const processedAt = new Date("2026-03-22T10:05:05.000Z");
  const store = new InMemoryJobProcessingStore([
    buildStoredJob({
      actionType: "FILTER",
      actionConfig: {
        conditions: [{ path: "amount", operator: "gt", value: 100 }],
      },
      payload: {
        amount: 42,
      },
    }),
  ]);
  const service = createJobProcessingService(store);

  const claimedJob = assertClaimedJob(await service.claimNextReadyJob(claimTime));
  const processedJob = await service.processClaimedJob(claimedJob, processedAt);

  assert.equal(processedJob.status, JobStatus.COMPLETED);
  assert.equal(processedJob.actionStatus, "FILTERED_OUT");
  assert.equal(processedJob.result, null);
  assert.equal(store.getJob("job-1")?.status, JobStatus.COMPLETED);
  assert.equal(store.getJob("job-1")?.result, null);
  assert.equal(store.getJob("job-1")?.processedAt?.getTime(), processedAt.getTime());
  assert.equal(store.getJob("job-1")?.lockedAt, null);
});

test("job processing service reschedules a claimed job when an unexpected error occurs", async () => {
  const claimTime = new Date("2026-03-22T10:05:00.000Z");
  const processedAt = new Date("2026-03-22T10:05:05.000Z");
  const actionConfig = {
    get fields(): never {
      throw new Error("Temporary processing error.");
    },
  };
  const store = new InMemoryJobProcessingStore([
    buildStoredJob({
      actionType: "TRANSFORM",
      actionConfig: actionConfig as unknown as JsonValue,
      payload: {
        lead: { id: "lead-123" },
      },
    }),
  ]);
  const service = createJobProcessingService(store);

  const claimedJob = assertClaimedJob(await service.claimNextReadyJob(claimTime));
  const processedJob = await service.processClaimedJob(claimedJob, processedAt);

  assert.equal(processedJob.status, "RETRY_SCHEDULED");
  assert.equal(processedJob.retryCount, 1);
  assert.equal(
    processedJob.nextRunAt.getTime(),
    new Date("2026-03-22T10:06:05.000Z").getTime(),
  );
  assert.match(processedJob.lastError, /Temporary processing error/);
  assert.equal(store.getJob("job-1")?.status, JobStatus.PENDING);
  assert.equal(store.getJob("job-1")?.retryCount, 1);
  assert.equal(
    store.getJob("job-1")?.nextRunAt.getTime(),
    new Date("2026-03-22T10:06:05.000Z").getTime(),
  );
  assert.equal(store.getJob("job-1")?.processedAt, null);
  assert.match(store.getJob("job-1")?.lastError ?? "", /Temporary processing error/);
  assert.equal(store.getJob("job-1")?.lockedAt, null);
});

test("job processing service fails a claimed job when retries are exhausted", async () => {
  const claimTime = new Date("2026-03-22T10:05:00.000Z");
  const processedAt = new Date("2026-03-22T10:05:05.000Z");
  const actionConfig = {
    get fields(): never {
      throw new Error("Temporary processing error.");
    },
  };
  const store = new InMemoryJobProcessingStore([
    buildStoredJob({
      actionType: "TRANSFORM",
      actionConfig: actionConfig as unknown as JsonValue,
      payload: {
        lead: { id: "lead-123" },
      },
      retryCount: 2,
      maxRetries: 2,
    }),
  ]);
  const service = createJobProcessingService(store);

  const claimedJob = assertClaimedJob(await service.claimNextReadyJob(claimTime));
  const processedJob = await service.processClaimedJob(claimedJob, processedAt);

  assert.equal(processedJob.status, JobStatus.FAILED);
  assert.match(processedJob.lastError, /Temporary processing error/);
  assert.equal(store.getJob("job-1")?.status, JobStatus.FAILED);
  assert.equal(store.getJob("job-1")?.retryCount, 2);
  assert.equal(store.getJob("job-1")?.processedAt?.getTime(), processedAt.getTime());
  assert.match(store.getJob("job-1")?.lastError ?? "", /Temporary processing error/);
  assert.equal(store.getJob("job-1")?.lockedAt, null);
});

test("job processing service fails validation errors without retrying", async () => {
  const claimTime = new Date("2026-03-22T10:05:00.000Z");
  const processedAt = new Date("2026-03-22T10:05:05.000Z");
  const store = new InMemoryJobProcessingStore([
    buildStoredJob({
      actionType: "TRANSFORM",
      actionConfig: {
        fields: {
          leadId: "lead.id",
        },
      },
      payload: 42,
    }),
  ]);
  const service = createJobProcessingService(store);

  const claimedJob = assertClaimedJob(await service.claimNextReadyJob(claimTime));
  const processedJob = await service.processClaimedJob(claimedJob, processedAt);

  assert.equal(processedJob.status, JobStatus.FAILED);
  assert.match(processedJob.lastError, /TRANSFORM payload must be a JSON object/);
  assert.equal(store.getJob("job-1")?.status, JobStatus.FAILED);
  assert.equal(store.getJob("job-1")?.retryCount, 0);
  assert.equal(store.getJob("job-1")?.result, null);
  assert.equal(store.getJob("job-1")?.processedAt?.getTime(), processedAt.getTime());
  assert.match(store.getJob("job-1")?.lastError ?? "", /TRANSFORM payload must be a JSON object/);
  assert.equal(store.getJob("job-1")?.lockedAt, null);
});
