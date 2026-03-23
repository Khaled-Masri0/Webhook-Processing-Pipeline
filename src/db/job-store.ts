import { DeliveryStatus, JobStatus, Prisma } from "@prisma/client";
import { prisma } from "./client";
import {
  DeliveryAttemptInput,
  DeliveryAttemptStore,
  PendingDeliveryAttempt,
} from "../services/subscriber-delivery-service";
import { EnqueueJobInput, JobStore, QueuedJob } from "../services/webhook-service";
import { JobProcessingStore, ReadyJob } from "../services/job-processing-service";
import { JsonValue } from "../utils/json";

class PrismaJobStore implements JobStore, JobProcessingStore, DeliveryAttemptStore {
  async create(input: EnqueueJobInput): Promise<QueuedJob> {
    const job = await prisma.job.create({
      data: {
        pipelineId: input.pipelineId,
        payload: input.payload as Prisma.InputJsonValue,
      },
    });

    return {
      ...mapQueuedJob(job),
    };
  }

  async findNextReadyJob(now: Date): Promise<ReadyJob | null> {
    const job = await prisma.job.findFirst({
      where: {
        status: JobStatus.PENDING,
        lockedAt: null,
        nextRunAt: {
          lte: now,
        },
      },
      orderBy: [{ nextRunAt: "asc" }, { createdAt: "asc" }],
      include: {
        pipeline: {
          select: {
            actionType: true,
            actionConfig: true,
          },
        },
      },
    });

    if (!job) {
      return null;
    }

    return {
      id: job.id,
      pipelineId: job.pipelineId,
      payload: job.payload as JsonValue,
      actionType: job.pipeline.actionType,
      actionConfig: job.pipeline.actionConfig as JsonValue,
      retryCount: job.retryCount,
      maxRetries: job.maxRetries,
      nextRunAt: job.nextRunAt,
      createdAt: job.createdAt,
    };
  }

  async markJobProcessing(jobId: string, lockedAt: Date): Promise<boolean> {
    const result = await prisma.job.updateMany({
      where: {
        id: jobId,
        status: JobStatus.PENDING,
        lockedAt: null,
        nextRunAt: {
          lte: lockedAt,
        },
      },
      data: {
        status: JobStatus.PROCESSING,
        lockedAt,
      },
    });

    return result.count === 1;
  }

  async markJobCompleted(jobId: string, result: JsonValue | null, processedAt: Date): Promise<boolean> {
    const update = await prisma.job.updateMany({
      where: {
        id: jobId,
        status: JobStatus.PROCESSING,
      },
      data: {
        status: JobStatus.COMPLETED,
        result: result === null ? Prisma.JsonNull : (result as Prisma.InputJsonValue),
        processedAt,
        lockedAt: null,
        lastError: null,
      },
    });

    return update.count === 1;
  }

  async markJobFailed(jobId: string, lastError: string, processedAt: Date): Promise<boolean> {
    const update = await prisma.job.updateMany({
      where: {
        id: jobId,
        status: JobStatus.PROCESSING,
      },
      data: {
        status: JobStatus.FAILED,
        result: Prisma.JsonNull,
        processedAt,
        lockedAt: null,
        lastError,
      },
    });

    return update.count === 1;
  }

  async rescheduleJob(
    jobId: string,
    lastError: string,
    nextRunAt: Date,
    retryCount: number,
  ): Promise<boolean> {
    const update = await prisma.job.updateMany({
      where: {
        id: jobId,
        status: JobStatus.PROCESSING,
      },
      data: {
        status: JobStatus.PENDING,
        result: Prisma.JsonNull,
        retryCount,
        nextRunAt,
        processedAt: null,
        lockedAt: null,
        lastError,
      },
    });

    return update.count === 1;
  }

  async createDeliveryAttempt(input: DeliveryAttemptInput): Promise<void> {
    await prisma.deliveryAttempt.create({
      data: {
        jobId: input.jobId,
        subscriberId: input.subscriberId,
        attemptNumber: input.attemptNumber,
        status: input.status,
        nextRunAt: input.nextRunAt,
        responseCode: input.responseCode,
        error: input.error,
        deliveredAt: input.deliveredAt,
      },
    });
  }

  async findNextPendingDeliveryAttempt(now: Date): Promise<PendingDeliveryAttempt | null> {
    const attempt = await prisma.deliveryAttempt.findFirst({
      where: {
        status: DeliveryStatus.PENDING,
        lockedAt: null,
        nextRunAt: {
          lte: now,
        },
      },
      orderBy: [{ nextRunAt: "asc" }, { createdAt: "asc" }],
      include: {
        subscriber: {
          select: {
            url: true,
            active: true,
          },
        },
        job: {
          select: {
            pipelineId: true,
            result: true,
          },
        },
      },
    });

    if (!attempt) {
      return null;
    }

    return {
      id: attempt.id,
      jobId: attempt.jobId,
      pipelineId: attempt.job.pipelineId,
      subscriberId: attempt.subscriberId,
      subscriberUrl: attempt.subscriber.url,
      subscriberActive: attempt.subscriber.active,
      attemptNumber: attempt.attemptNumber,
      payload: attempt.job.result as JsonValue | null,
      nextRunAt: attempt.nextRunAt,
      createdAt: attempt.createdAt,
    };
  }

  async markDeliveryAttemptLocked(attemptId: string, lockedAt: Date): Promise<boolean> {
    const update = await prisma.deliveryAttempt.updateMany({
      where: {
        id: attemptId,
        status: DeliveryStatus.PENDING,
        lockedAt: null,
        nextRunAt: {
          lte: lockedAt,
        },
      },
      data: {
        lockedAt,
      },
    });

    return update.count === 1;
  }

  async markDeliveryAttemptSucceeded(
    attemptId: string,
    responseCode: number,
    deliveredAt: Date,
  ): Promise<boolean> {
    const update = await prisma.deliveryAttempt.updateMany({
      where: {
        id: attemptId,
        status: DeliveryStatus.PENDING,
      },
      data: {
        status: DeliveryStatus.SUCCESS,
        responseCode,
        error: null,
        deliveredAt,
        lockedAt: null,
      },
    });

    return update.count === 1;
  }

  async markDeliveryAttemptFailed(
    attemptId: string,
    error: string,
    responseCode?: number,
  ): Promise<boolean> {
    const update = await prisma.deliveryAttempt.updateMany({
      where: {
        id: attemptId,
        status: DeliveryStatus.PENDING,
      },
      data: {
        status: DeliveryStatus.FAILED,
        responseCode,
        error,
        deliveredAt: null,
        lockedAt: null,
      },
    });

    return update.count === 1;
  }
}

export const prismaJobStore = new PrismaJobStore();

function mapQueuedJob(job: {
  id: string;
  pipelineId: string;
  status: JobStatus;
  retryCount: number;
  maxRetries: number;
  nextRunAt: Date;
  createdAt: Date;
}): QueuedJob {
  return {
    id: job.id,
    pipelineId: job.pipelineId,
    status: job.status,
    retryCount: job.retryCount,
    maxRetries: job.maxRetries,
    nextRunAt: job.nextRunAt,
    createdAt: job.createdAt,
  };
}
