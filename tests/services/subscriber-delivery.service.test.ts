import assert from "node:assert/strict";
import test from "node:test";
import { DeliveryStatus } from "@prisma/client";
import { CompletedJob } from "../../src/services/job-processing-service.js";
import {
  createSubscriberDeliveryService,
  DeliveryAttemptInput,
  DeliveryAttemptStore,
  DeliveryHttpClient,
  DeliveryHttpResponse,
  DeliverySubscriber,
  DeliverySubscriberStore,
  PendingDeliveryAttempt,
  shouldDeliverProcessedJob,
} from "../../src/services/subscriber-delivery-service.js";
import { JsonValue } from "../../src/utils/json.js";

interface StoredSubscriber extends DeliverySubscriber {
  pipelineId: string;
  active: boolean;
}

interface StoredJobResult {
  id: string;
  pipelineId: string;
  payload: JsonValue | null;
}

interface StoredAttempt extends DeliveryAttemptInput {
  id: string;
  nextRunAt: Date;
  lockedAt: Date | null;
  createdAt: Date;
}

class InMemorySubscriberStore implements DeliverySubscriberStore {
  constructor(private readonly subscribers: StoredSubscriber[]) {}

  async listActiveSubscribers(pipelineId: string): Promise<DeliverySubscriber[]> {
    return this.subscribers
      .filter((subscriber) => subscriber.pipelineId === pipelineId && subscriber.active)
      .map((subscriber) => ({
        id: subscriber.id,
        url: subscriber.url,
      }));
  }
}

class InMemoryDeliveryAttemptStore implements DeliveryAttemptStore {
  private nextId = 1;

  constructor(
    private readonly jobs: StoredJobResult[],
    private readonly subscribers: StoredSubscriber[],
    private readonly attempts: StoredAttempt[] = [],
  ) {
    this.nextId = this.attempts.length + 1;
  }

  async createDeliveryAttempt(input: DeliveryAttemptInput): Promise<void> {
    this.attempts.push({
      id: `attempt-${this.nextId}`,
      jobId: input.jobId,
      subscriberId: input.subscriberId,
      attemptNumber: input.attemptNumber,
      status: input.status,
      nextRunAt: input.nextRunAt ?? new Date(`2026-03-23T10:${String(this.nextId).padStart(2, "0")}:00.000Z`),
      lockedAt: null,
      responseCode: input.responseCode,
      error: input.error,
      deliveredAt: input.deliveredAt,
      createdAt: new Date(`2026-03-23T09:${String(this.nextId).padStart(2, "0")}:00.000Z`),
    });
    this.nextId += 1;
  }

  async findNextPendingDeliveryAttempt(now: Date): Promise<PendingDeliveryAttempt | null> {
    const nextAttempt = this.attempts
      .filter((attempt) => attempt.status === DeliveryStatus.PENDING && attempt.lockedAt === null)
      .filter((attempt) => attempt.nextRunAt.getTime() <= now.getTime())
      .sort((left, right) => {
        if (left.nextRunAt.getTime() !== right.nextRunAt.getTime()) {
          return left.nextRunAt.getTime() - right.nextRunAt.getTime();
        }

        return left.createdAt.getTime() - right.createdAt.getTime();
      })[0];

    if (!nextAttempt) {
      return null;
    }

    const subscriber = this.subscribers.find((entry) => entry.id === nextAttempt.subscriberId);
    const job = this.jobs.find((entry) => entry.id === nextAttempt.jobId);

    if (!subscriber || !job) {
      throw new Error("Pending attempt is missing linked job or subscriber.");
    }

    return {
      id: nextAttempt.id,
      jobId: nextAttempt.jobId,
      pipelineId: job.pipelineId,
      subscriberId: nextAttempt.subscriberId,
      subscriberUrl: subscriber.url,
      subscriberActive: subscriber.active,
      attemptNumber: nextAttempt.attemptNumber,
      payload: job.payload,
      nextRunAt: nextAttempt.nextRunAt,
      createdAt: nextAttempt.createdAt,
    };
  }

  async markDeliveryAttemptLocked(attemptId: string, lockedAt: Date): Promise<boolean> {
    const attempt = this.attempts.find((entry) => entry.id === attemptId);

    if (
      !attempt ||
      attempt.status !== DeliveryStatus.PENDING ||
      attempt.lockedAt !== null ||
      attempt.nextRunAt.getTime() > lockedAt.getTime()
    ) {
      return false;
    }

    attempt.lockedAt = lockedAt;
    return true;
  }

  async markDeliveryAttemptSucceeded(
    attemptId: string,
    responseCode: number,
    deliveredAt: Date,
  ): Promise<boolean> {
    const attempt = this.attempts.find((entry) => entry.id === attemptId);

    if (!attempt || attempt.status !== DeliveryStatus.PENDING) {
      return false;
    }

    attempt.status = DeliveryStatus.SUCCESS;
    attempt.responseCode = responseCode;
    attempt.error = undefined;
    attempt.deliveredAt = deliveredAt;
    attempt.lockedAt = null;
    return true;
  }

  async markDeliveryAttemptFailed(
    attemptId: string,
    error: string,
    responseCode?: number,
  ): Promise<boolean> {
    const attempt = this.attempts.find((entry) => entry.id === attemptId);

    if (!attempt || attempt.status !== DeliveryStatus.PENDING) {
      return false;
    }

    attempt.status = DeliveryStatus.FAILED;
    attempt.responseCode = responseCode;
    attempt.error = error;
    attempt.deliveredAt = undefined;
    attempt.lockedAt = null;
    return true;
  }

  listAttempts(): StoredAttempt[] {
    return [...this.attempts];
  }

  getAttempt(id: string): StoredAttempt | undefined {
    return this.attempts.find((attempt) => attempt.id === id);
  }
}

class FakeDeliveryHttpClient implements DeliveryHttpClient {
  readonly deliveries: Array<{ url: string; payload: JsonValue }> = [];
  private readonly responses = new Map<string, DeliveryHttpResponse | Error>();

  setResponse(url: string, response: DeliveryHttpResponse | Error): void {
    this.responses.set(url, response);
  }

  async postJson(url: string, payload: JsonValue): Promise<DeliveryHttpResponse> {
    this.deliveries.push({ url, payload });

    const response = this.responses.get(url) ?? { statusCode: 200 };

    if (response instanceof Error) {
      throw response;
    }

    return response;
  }
}

function buildSubscriber(overrides: Partial<StoredSubscriber> = {}): StoredSubscriber {
  return {
    id: "subscriber-1",
    pipelineId: "pipeline-1",
    url: "https://example.com/hooks/1",
    active: true,
    ...overrides,
  };
}

function buildJobResult(overrides: Partial<StoredJobResult> = {}): StoredJobResult {
  return {
    id: "job-1",
    pipelineId: "pipeline-1",
    payload: {
      leadId: "lead-123",
    },
    ...overrides,
  };
}

function buildStoredAttempt(overrides: Partial<StoredAttempt> = {}): StoredAttempt {
  return {
    id: "attempt-1",
    jobId: "job-1",
    subscriberId: "subscriber-1",
    attemptNumber: 2,
    status: DeliveryStatus.PENDING,
    nextRunAt: new Date("2026-03-23T10:00:00.000Z"),
    lockedAt: null,
    responseCode: undefined,
    error: undefined,
    deliveredAt: undefined,
    createdAt: new Date("2026-03-23T09:00:00.000Z"),
    ...overrides,
  };
}

function buildCompletedJob(overrides: Partial<CompletedJob> = {}): CompletedJob {
  return {
    jobId: "job-1",
    pipelineId: "pipeline-1",
    status: "COMPLETED",
    actionStatus: "SUCCESS",
    result: {
      leadId: "lead-123",
    },
    processedAt: new Date("2026-03-23T10:05:00.000Z"),
    ...overrides,
  };
}

test("subscriber delivery service delivers to active subscribers and records successful attempts", async () => {
  const subscriberStore = new InMemorySubscriberStore([
    buildSubscriber(),
    buildSubscriber({
      id: "subscriber-2",
      url: "https://example.com/hooks/2",
    }),
  ]);
  const attemptStore = new InMemoryDeliveryAttemptStore([buildJobResult()], [
    buildSubscriber(),
    buildSubscriber({
      id: "subscriber-2",
      url: "https://example.com/hooks/2",
    }),
  ]);
  const httpClient = new FakeDeliveryHttpClient();
  const service = createSubscriberDeliveryService(subscriberStore, attemptStore, httpClient);
  const deliveredAt = new Date("2026-03-23T10:06:00.000Z");

  const summary = await service.deliverJobResult(
    {
      jobId: "job-1",
      pipelineId: "pipeline-1",
      payload: {
        leadId: "lead-123",
      },
    },
    deliveredAt,
  );

  assert.deepEqual(summary, {
    jobId: "job-1",
    pipelineId: "pipeline-1",
    totalSubscribers: 2,
    deliveredCount: 2,
    failedCount: 0,
  });
  assert.equal(attemptStore.listAttempts().length, 2);
  assert.deepEqual(
    attemptStore.listAttempts().map((attempt) => ({
      subscriberId: attempt.subscriberId,
      attemptNumber: attempt.attemptNumber,
      status: attempt.status,
      responseCode: attempt.responseCode,
      deliveredAt: attempt.deliveredAt,
    })),
    [
      {
        subscriberId: "subscriber-1",
        attemptNumber: 1,
        status: DeliveryStatus.SUCCESS,
        responseCode: 200,
        deliveredAt,
      },
      {
        subscriberId: "subscriber-2",
        attemptNumber: 1,
        status: DeliveryStatus.SUCCESS,
        responseCode: 200,
        deliveredAt,
      },
    ],
  );
});

test("subscriber delivery service records failed attempts and schedules retries", async () => {
  const subscribers = [
    buildSubscriber(),
    buildSubscriber({
      id: "subscriber-2",
      url: "https://example.com/hooks/2",
    }),
    buildSubscriber({
      id: "subscriber-3",
      url: "https://example.com/hooks/3",
    }),
  ];
  const subscriberStore = new InMemorySubscriberStore(subscribers);
  const attemptStore = new InMemoryDeliveryAttemptStore([buildJobResult()], subscribers);
  const httpClient = new FakeDeliveryHttpClient();
  httpClient.setResponse("https://example.com/hooks/2", { statusCode: 503 });
  httpClient.setResponse("https://example.com/hooks/3", new Error("Connection reset."));
  const service = createSubscriberDeliveryService(subscriberStore, attemptStore, httpClient);
  const now = new Date("2026-03-23T10:06:00.000Z");

  const summary = await service.deliverJobResult(
    {
      jobId: "job-1",
      pipelineId: "pipeline-1",
      payload: {
        leadId: "lead-123",
      },
    },
    now,
  );

  assert.deepEqual(summary, {
    jobId: "job-1",
    pipelineId: "pipeline-1",
    totalSubscribers: 3,
    deliveredCount: 1,
    failedCount: 2,
  });
  assert.equal(attemptStore.listAttempts().length, 5);
  assert.deepEqual(
    attemptStore.listAttempts().map((attempt) => ({
      subscriberId: attempt.subscriberId,
      attemptNumber: attempt.attemptNumber,
      status: attempt.status,
      nextRunAt: attempt.nextRunAt,
      responseCode: attempt.responseCode,
      error: attempt.error,
    })),
    [
      {
        subscriberId: "subscriber-1",
        attemptNumber: 1,
        status: DeliveryStatus.SUCCESS,
        nextRunAt: attemptStore.listAttempts()[0]?.nextRunAt,
        responseCode: 200,
        error: undefined,
      },
      {
        subscriberId: "subscriber-2",
        attemptNumber: 1,
        status: DeliveryStatus.FAILED,
        nextRunAt: attemptStore.listAttempts()[1]?.nextRunAt,
        responseCode: 503,
        error: "Subscriber responded with HTTP 503.",
      },
      {
        subscriberId: "subscriber-2",
        attemptNumber: 2,
        status: DeliveryStatus.PENDING,
        nextRunAt: new Date("2026-03-23T10:07:00.000Z"),
        responseCode: undefined,
        error: undefined,
      },
      {
        subscriberId: "subscriber-3",
        attemptNumber: 1,
        status: DeliveryStatus.FAILED,
        nextRunAt: attemptStore.listAttempts()[3]?.nextRunAt,
        responseCode: undefined,
        error: "Connection reset.",
      },
      {
        subscriberId: "subscriber-3",
        attemptNumber: 2,
        status: DeliveryStatus.PENDING,
        nextRunAt: new Date("2026-03-23T10:07:00.000Z"),
        responseCode: undefined,
        error: undefined,
      },
    ],
  );
});

test("subscriber delivery service skips inactive subscribers", async () => {
  const subscribers = [
    buildSubscriber(),
    buildSubscriber({
      id: "subscriber-2",
      url: "https://example.com/hooks/2",
      active: false,
    }),
  ];
  const subscriberStore = new InMemorySubscriberStore(subscribers);
  const attemptStore = new InMemoryDeliveryAttemptStore([buildJobResult()], subscribers);
  const httpClient = new FakeDeliveryHttpClient();
  const service = createSubscriberDeliveryService(subscriberStore, attemptStore, httpClient);

  const summary = await service.deliverJobResult({
    jobId: "job-1",
    pipelineId: "pipeline-1",
    payload: {
      leadId: "lead-123",
    },
  });

  assert.deepEqual(summary, {
    jobId: "job-1",
    pipelineId: "pipeline-1",
    totalSubscribers: 1,
    deliveredCount: 1,
    failedCount: 0,
  });
  assert.equal(attemptStore.listAttempts().length, 1);
  assert.equal(attemptStore.listAttempts()[0]?.subscriberId, "subscriber-1");
});

test("subscriber delivery service marks a retry attempt successful", async () => {
  const subscribers = [buildSubscriber()];
  const attemptStore = new InMemoryDeliveryAttemptStore(
    [buildJobResult()],
    subscribers,
    [buildStoredAttempt()],
  );
  const httpClient = new FakeDeliveryHttpClient();
  const service = createSubscriberDeliveryService(
    new InMemorySubscriberStore(subscribers),
    attemptStore,
    httpClient,
  );
  const now = new Date("2026-03-23T10:06:00.000Z");

  const result = await service.processNextDeliveryRetry(now);

  assert.deepEqual(result, {
    attemptId: "attempt-1",
    jobId: "job-1",
    pipelineId: "pipeline-1",
    subscriberId: "subscriber-1",
    attemptNumber: 2,
    status: "SUCCESS",
    deliveredAt: now,
  });
  assert.equal(attemptStore.getAttempt("attempt-1")?.status, DeliveryStatus.SUCCESS);
  assert.equal(attemptStore.getAttempt("attempt-1")?.responseCode, 200);
  assert.equal(attemptStore.getAttempt("attempt-1")?.deliveredAt?.getTime(), now.getTime());
  assert.equal(attemptStore.getAttempt("attempt-1")?.lockedAt, null);
});

test("subscriber delivery service reschedules a retry attempt after failure", async () => {
  const subscribers = [buildSubscriber()];
  const attemptStore = new InMemoryDeliveryAttemptStore(
    [buildJobResult()],
    subscribers,
    [buildStoredAttempt()],
  );
  const httpClient = new FakeDeliveryHttpClient();
  httpClient.setResponse("https://example.com/hooks/1", { statusCode: 503 });
  const service = createSubscriberDeliveryService(
    new InMemorySubscriberStore(subscribers),
    attemptStore,
    httpClient,
  );
  const now = new Date("2026-03-23T10:06:00.000Z");

  const result = await service.processNextDeliveryRetry(now);

  assert.deepEqual(result, {
    attemptId: "attempt-1",
    jobId: "job-1",
    pipelineId: "pipeline-1",
    subscriberId: "subscriber-1",
    attemptNumber: 2,
    status: "RETRY_SCHEDULED",
    lastError: "Subscriber responded with HTTP 503.",
    nextAttemptNumber: 3,
    nextRunAt: new Date("2026-03-23T10:07:00.000Z"),
  });
  assert.equal(attemptStore.getAttempt("attempt-1")?.status, DeliveryStatus.FAILED);
  assert.equal(attemptStore.listAttempts().length, 2);
  assert.deepEqual(
    attemptStore.listAttempts()[1] && {
      subscriberId: attemptStore.listAttempts()[1]?.subscriberId,
      attemptNumber: attemptStore.listAttempts()[1]?.attemptNumber,
      status: attemptStore.listAttempts()[1]?.status,
      nextRunAt: attemptStore.listAttempts()[1]?.nextRunAt,
    },
    {
      subscriberId: "subscriber-1",
      attemptNumber: 3,
      status: DeliveryStatus.PENDING,
      nextRunAt: new Date("2026-03-23T10:07:00.000Z"),
    },
  );
});

test("subscriber delivery service fails permanently after the fifth attempt", async () => {
  const subscribers = [buildSubscriber()];
  const attemptStore = new InMemoryDeliveryAttemptStore(
    [buildJobResult()],
    subscribers,
    [
      buildStoredAttempt({
        attemptNumber: 5,
      }),
    ],
  );
  const httpClient = new FakeDeliveryHttpClient();
  httpClient.setResponse("https://example.com/hooks/1", new Error("Connection reset."));
  const service = createSubscriberDeliveryService(
    new InMemorySubscriberStore(subscribers),
    attemptStore,
    httpClient,
  );
  const now = new Date("2026-03-23T10:06:00.000Z");

  const result = await service.processNextDeliveryRetry(now);

  assert.deepEqual(result, {
    attemptId: "attempt-1",
    jobId: "job-1",
    pipelineId: "pipeline-1",
    subscriberId: "subscriber-1",
    attemptNumber: 5,
    status: "FAILED",
    lastError: "Connection reset.",
  });
  assert.equal(attemptStore.getAttempt("attempt-1")?.status, DeliveryStatus.FAILED);
  assert.equal(attemptStore.listAttempts().length, 1);
});

test("subscriber delivery helper skips filtered-out completed jobs", () => {
  const filteredOutJob = buildCompletedJob({
    actionStatus: "FILTERED_OUT",
    result: null,
  });
  const successfulJob = buildCompletedJob();

  assert.equal(shouldDeliverProcessedJob(filteredOutJob), false);
  assert.equal(shouldDeliverProcessedJob(successfulJob), true);
});
