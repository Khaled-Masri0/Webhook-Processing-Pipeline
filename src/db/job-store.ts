import { JobStatus, Prisma } from "@prisma/client";
import { prisma } from "./client";
import { EnqueueJobInput, JobStore, QueuedJob } from "../services/webhook-service";
import { JobProcessingStore, ReadyJob } from "../services/job-processing-service";
import { JsonValue } from "../utils/json";

class PrismaJobStore implements JobStore, JobProcessingStore {
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
