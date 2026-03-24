import { DeliveryStatus } from "@prisma/client";
import { CompletedJob } from "./job-processing-service.js";
import { JsonValue } from "../utils/json.js";

export interface DeliverySubscriber {
  id: string;
  url: string;
}

export interface DeliveryAttemptInput {
  jobId: string;
  subscriberId: string;
  attemptNumber: number;
  status: DeliveryStatus;
  nextRunAt?: Date;
  responseCode?: number;
  error?: string;
  deliveredAt?: Date;
}

export interface PendingDeliveryAttempt {
  id: string;
  jobId: string;
  pipelineId: string;
  subscriberId: string;
  subscriberUrl: string;
  subscriberActive: boolean;
  attemptNumber: number;
  payload: JsonValue | null;
  nextRunAt: Date;
  createdAt: Date;
}

export interface DeliverySubscriberStore {
  listActiveSubscribers(pipelineId: string): Promise<DeliverySubscriber[]>;
}

export interface DeliveryAttemptStore {
  createDeliveryAttempt(input: DeliveryAttemptInput): Promise<void>;
  findNextPendingDeliveryAttempt(now: Date): Promise<PendingDeliveryAttempt | null>;
  markDeliveryAttemptLocked(attemptId: string, lockedAt: Date): Promise<boolean>;
  markDeliveryAttemptSucceeded(
    attemptId: string,
    responseCode: number,
    deliveredAt: Date,
  ): Promise<boolean>;
  markDeliveryAttemptFailed(
    attemptId: string,
    error: string,
    responseCode?: number,
  ): Promise<boolean>;
}

export interface DeliveryHttpResponse {
  statusCode: number;
}

export interface DeliveryHttpClient {
  postJson(url: string, payload: JsonValue): Promise<DeliveryHttpResponse>;
}

export interface DeliveryResultInput {
  jobId: string;
  pipelineId: string;
  payload: JsonValue;
}

export interface DeliverySummary {
  jobId: string;
  pipelineId: string;
  totalSubscribers: number;
  deliveredCount: number;
  failedCount: number;
}

export interface RetriedDeliverySuccess {
  attemptId: string;
  jobId: string;
  pipelineId: string;
  subscriberId: string;
  attemptNumber: number;
  status: "SUCCESS";
  deliveredAt: Date;
}

export interface RetriedDeliveryScheduled {
  attemptId: string;
  jobId: string;
  pipelineId: string;
  subscriberId: string;
  attemptNumber: number;
  status: "RETRY_SCHEDULED";
  lastError: string;
  nextAttemptNumber: number;
  nextRunAt: Date;
}

export interface RetriedDeliveryFailed {
  attemptId: string;
  jobId: string;
  pipelineId: string;
  subscriberId: string;
  attemptNumber: number;
  status: "FAILED";
  lastError: string;
}

export type DeliveryRetryResult =
  | RetriedDeliverySuccess
  | RetriedDeliveryScheduled
  | RetriedDeliveryFailed;

export interface SubscriberDeliveryService {
  deliverJobResult(input: DeliveryResultInput, now?: Date): Promise<DeliverySummary>;
  processNextDeliveryRetry(now?: Date): Promise<DeliveryRetryResult | null>;
}

export type DeliverableCompletedJob = CompletedJob & {
  actionStatus: "SUCCESS";
  result: JsonValue;
};

const FIRST_DELIVERY_ATTEMPT = 1;
const DELIVERY_RETRY_DELAY_MS = 60_000;
const MAX_DELIVERY_ATTEMPTS = 5;
const MAX_DELIVERY_CLAIM_ATTEMPTS = 5;

const defaultDeliveryHttpClient: DeliveryHttpClient = {
  async postJson(url: string, payload: JsonValue): Promise<DeliveryHttpResponse> {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    return {
      statusCode: response.status,
    };
  },
};

export function createSubscriberDeliveryService(
  subscriberStore: DeliverySubscriberStore,
  attemptStore: DeliveryAttemptStore,
  httpClient: DeliveryHttpClient = defaultDeliveryHttpClient,
): SubscriberDeliveryService {
  return {
    async deliverJobResult(input: DeliveryResultInput, now = new Date()): Promise<DeliverySummary> {
      const subscribers = await subscriberStore.listActiveSubscribers(input.pipelineId);
      let deliveredCount = 0;
      let failedCount = 0;

      for (const subscriber of subscribers) {
        const deliveryOutcome = await sendDelivery({
          jobId: input.jobId,
          pipelineId: input.pipelineId,
          subscriber,
          payload: input.payload,
          attemptNumber: FIRST_DELIVERY_ATTEMPT,
          now,
          attemptStore,
          httpClient,
        });

        if (deliveryOutcome === "SUCCESS") {
          deliveredCount += 1;
        } else {
          failedCount += 1;
        }
      }

      return {
        jobId: input.jobId,
        pipelineId: input.pipelineId,
        totalSubscribers: subscribers.length,
        deliveredCount,
        failedCount,
      };
    },

    async processNextDeliveryRetry(now = new Date()): Promise<DeliveryRetryResult | null> {
      const pendingAttempt = await claimNextPendingDeliveryAttempt(attemptStore, now);

      if (!pendingAttempt) {
        return null;
      }

      if (!pendingAttempt.subscriberActive) {
        const lastError = `Subscriber ${pendingAttempt.subscriberId} is inactive.`;
        const markedFailed = await attemptStore.markDeliveryAttemptFailed(
          pendingAttempt.id,
          lastError,
        );

        if (!markedFailed) {
          throw new Error(`Delivery attempt ${pendingAttempt.id} could not be marked failed.`);
        }

        return {
          attemptId: pendingAttempt.id,
          jobId: pendingAttempt.jobId,
          pipelineId: pendingAttempt.pipelineId,
          subscriberId: pendingAttempt.subscriberId,
          attemptNumber: pendingAttempt.attemptNumber,
          status: "FAILED",
          lastError,
        };
      }

      if (pendingAttempt.payload === null) {
        const lastError = `Delivery payload missing for job ${pendingAttempt.jobId}.`;
        const markedFailed = await attemptStore.markDeliveryAttemptFailed(
          pendingAttempt.id,
          lastError,
        );

        if (!markedFailed) {
          throw new Error(`Delivery attempt ${pendingAttempt.id} could not be marked failed.`);
        }

        return {
          attemptId: pendingAttempt.id,
          jobId: pendingAttempt.jobId,
          pipelineId: pendingAttempt.pipelineId,
          subscriberId: pendingAttempt.subscriberId,
          attemptNumber: pendingAttempt.attemptNumber,
          status: "FAILED",
          lastError,
        };
      }

      const result = await deliverRetryAttempt(pendingAttempt, now, attemptStore, httpClient);

      return result;
    },
  };
}

export function shouldDeliverProcessedJob(job: CompletedJob): job is DeliverableCompletedJob {
  return job.actionStatus === "SUCCESS" && job.result !== null;
}

async function claimNextPendingDeliveryAttempt(
  attemptStore: DeliveryAttemptStore,
  now: Date,
): Promise<PendingDeliveryAttempt | null> {
  for (let attempt = 0; attempt < MAX_DELIVERY_CLAIM_ATTEMPTS; attempt += 1) {
    const pendingAttempt = await attemptStore.findNextPendingDeliveryAttempt(now);

    if (!pendingAttempt) {
      return null;
    }

    const locked = await attemptStore.markDeliveryAttemptLocked(pendingAttempt.id, now);

    if (locked) {
      return pendingAttempt;
    }
  }

  return null;
}

async function deliverRetryAttempt(
  attempt: PendingDeliveryAttempt & { payload: JsonValue },
  now: Date,
  attemptStore: DeliveryAttemptStore,
  httpClient: DeliveryHttpClient,
): Promise<DeliveryRetryResult> {
  try {
    const response = await httpClient.postJson(attempt.subscriberUrl, attempt.payload);

    if (isSuccessResponse(response.statusCode)) {
      const markedSucceeded = await attemptStore.markDeliveryAttemptSucceeded(
        attempt.id,
        response.statusCode,
        now,
      );

      if (!markedSucceeded) {
        throw new Error(`Delivery attempt ${attempt.id} could not be marked successful.`);
      }

      return {
        attemptId: attempt.id,
        jobId: attempt.jobId,
        pipelineId: attempt.pipelineId,
        subscriberId: attempt.subscriberId,
        attemptNumber: attempt.attemptNumber,
        status: "SUCCESS",
        deliveredAt: now,
      };
    }

    return handleRetriedDeliveryFailure(
      attempt,
      `Subscriber responded with HTTP ${response.statusCode}.`,
      now,
      attemptStore,
      response.statusCode,
    );
  } catch (error) {
    return handleRetriedDeliveryFailure(
      attempt,
      toErrorMessage(error),
      now,
      attemptStore,
    );
  }
}

async function handleRetriedDeliveryFailure(
  attempt: PendingDeliveryAttempt,
  lastError: string,
  now: Date,
  attemptStore: DeliveryAttemptStore,
  responseCode?: number,
): Promise<DeliveryRetryResult> {
  const markedFailed = await attemptStore.markDeliveryAttemptFailed(
    attempt.id,
    lastError,
    responseCode,
  );

  if (!markedFailed) {
    throw new Error(`Delivery attempt ${attempt.id} could not be marked failed.`);
  }

  if (attempt.attemptNumber < MAX_DELIVERY_ATTEMPTS) {
    const nextAttemptNumber = attempt.attemptNumber + 1;
    const nextRunAt = new Date(now.getTime() + DELIVERY_RETRY_DELAY_MS);

    await attemptStore.createDeliveryAttempt({
      jobId: attempt.jobId,
      subscriberId: attempt.subscriberId,
      attemptNumber: nextAttemptNumber,
      status: DeliveryStatus.PENDING,
      nextRunAt,
    });

    return {
      attemptId: attempt.id,
      jobId: attempt.jobId,
      pipelineId: attempt.pipelineId,
      subscriberId: attempt.subscriberId,
      attemptNumber: attempt.attemptNumber,
      status: "RETRY_SCHEDULED",
      lastError,
      nextAttemptNumber,
      nextRunAt,
    };
  }

  return {
    attemptId: attempt.id,
    jobId: attempt.jobId,
    pipelineId: attempt.pipelineId,
    subscriberId: attempt.subscriberId,
    attemptNumber: attempt.attemptNumber,
    status: "FAILED",
    lastError,
  };
}

async function sendDelivery(input: {
  jobId: string;
  pipelineId: string;
  subscriber: DeliverySubscriber;
  payload: JsonValue;
  attemptNumber: number;
  now: Date;
  attemptStore: DeliveryAttemptStore;
  httpClient: DeliveryHttpClient;
}): Promise<"SUCCESS" | "FAILED"> {
  try {
    const response = await input.httpClient.postJson(input.subscriber.url, input.payload);

    if (isSuccessResponse(response.statusCode)) {
      await input.attemptStore.createDeliveryAttempt({
        jobId: input.jobId,
        subscriberId: input.subscriber.id,
        attemptNumber: input.attemptNumber,
        status: DeliveryStatus.SUCCESS,
        responseCode: response.statusCode,
        deliveredAt: input.now,
      });

      return "SUCCESS";
    }

    await recordInitialFailedDelivery(
      {
        jobId: input.jobId,
        subscriberId: input.subscriber.id,
        attemptNumber: input.attemptNumber,
      },
      `Subscriber responded with HTTP ${response.statusCode}.`,
      input.now,
      input.attemptStore,
      response.statusCode,
    );

    return "FAILED";
  } catch (error) {
    await recordInitialFailedDelivery(
      {
        jobId: input.jobId,
        subscriberId: input.subscriber.id,
        attemptNumber: input.attemptNumber,
      },
      toErrorMessage(error),
      input.now,
      input.attemptStore,
    );

    return "FAILED";
  }
}

async function recordInitialFailedDelivery(
  input: {
    jobId: string;
    subscriberId: string;
    attemptNumber: number;
  },
  lastError: string,
  now: Date,
  attemptStore: DeliveryAttemptStore,
  responseCode?: number,
): Promise<void> {
  await attemptStore.createDeliveryAttempt({
    jobId: input.jobId,
    subscriberId: input.subscriberId,
    attemptNumber: input.attemptNumber,
    status: DeliveryStatus.FAILED,
    responseCode,
    error: lastError,
  });

  if (input.attemptNumber < MAX_DELIVERY_ATTEMPTS) {
    await attemptStore.createDeliveryAttempt({
      jobId: input.jobId,
      subscriberId: input.subscriberId,
      attemptNumber: input.attemptNumber + 1,
      status: DeliveryStatus.PENDING,
      nextRunAt: new Date(now.getTime() + DELIVERY_RETRY_DELAY_MS),
    });
  }
}

function isSuccessResponse(statusCode: number): boolean {
  return statusCode >= 200 && statusCode < 300;
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }

  return "Unknown delivery error";
}
