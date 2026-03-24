import { ActionType, DeliveryStatus, JobStatus, Prisma } from "@prisma/client";
import { prisma } from "./client.js";
import {
  DeliveryAttemptDetails,
  DeliveryHistoryQuery,
  JobDetails,
  JobHistoryQuery,
  JobQueryStore,
  JobSummary,
  PaginatedResult,
} from "../services/job-query-service.js";
import {
  DeliveryAttemptInput,
  DeliveryAttemptStore,
  PendingDeliveryAttempt,
} from "../services/subscriber-delivery-service.js";
import { EnqueueJobInput, JobStore, QueuedJob } from "../services/webhook-service.js";
import { JobProcessingStore, ReadyJob } from "../services/job-processing-service.js";
import { JsonValue } from "../utils/json.js";

const jobListInclude = {
  pipeline: {
    select: {
      name: true,
      sourcePath: true,
      actionType: true,
      active: true,
    },
  },
} satisfies Prisma.JobInclude;

const deliveryAttemptInclude = {
  subscriber: {
    select: {
      url: true,
    },
  },
} satisfies Prisma.DeliveryAttemptInclude;

class PrismaJobStore implements JobStore, JobProcessingStore, DeliveryAttemptStore, JobQueryStore {
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

  async findJobById(id: string): Promise<JobDetails | null> {
    const job = await prisma.job.findUnique({
      where: { id },
      include: jobListInclude,
    });

    return job ? mapJobDetails(job) : null;
  }

  async listJobs(query: JobHistoryQuery): Promise<PaginatedResult<JobSummary>> {
    const where = buildJobHistoryWhere(query);
    const skip = (query.page - 1) * query.pageSize;

    const [items, totalItems] = await prisma.$transaction([
      prisma.job.findMany({
        where,
        include: jobListInclude,
        orderBy: {
          createdAt: "desc",
        },
        skip,
        take: query.pageSize,
      }),
      prisma.job.count({ where }),
    ]);

    return buildPaginatedResult(
      items.map(mapJobSummary),
      query.page,
      query.pageSize,
      totalItems,
    );
  }

  async listDeliveryAttempts(
    jobId: string,
    query: DeliveryHistoryQuery,
  ): Promise<PaginatedResult<DeliveryAttemptDetails>> {
    const where: Prisma.DeliveryAttemptWhereInput = {
      jobId,
      ...(query.status ? { status: query.status } : {}),
    };
    const skip = (query.page - 1) * query.pageSize;

    const [items, totalItems] = await prisma.$transaction([
      prisma.deliveryAttempt.findMany({
        where,
        include: deliveryAttemptInclude,
        orderBy: [{ createdAt: "asc" }, { attemptNumber: "asc" }],
        skip,
        take: query.pageSize,
      }),
      prisma.deliveryAttempt.count({ where }),
    ]);

    return buildPaginatedResult(
      items.map(mapDeliveryAttemptDetails),
      query.page,
      query.pageSize,
      totalItems,
    );
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

function buildJobHistoryWhere(query: JobHistoryQuery): Prisma.JobWhereInput {
  return {
    ...(query.pipelineId ? { pipelineId: query.pipelineId } : {}),
    ...(query.status ? { status: query.status } : {}),
    ...buildCreatedAtRange(query.createdAfter, query.createdBefore),
  };
}

function buildCreatedAtRange(
  createdAfter?: Date,
  createdBefore?: Date,
): Prisma.JobWhereInput {
  if (!createdAfter && !createdBefore) {
    return {};
  }

  return {
    createdAt: {
      ...(createdAfter ? { gte: createdAfter } : {}),
      ...(createdBefore ? { lte: createdBefore } : {}),
    },
  };
}

function buildPaginatedResult<T>(
  items: T[],
  page: number,
  pageSize: number,
  totalItems: number,
): PaginatedResult<T> {
  const totalPages = totalItems === 0 ? 0 : Math.ceil(totalItems / pageSize);

  return {
    items,
    page,
    pageSize,
    totalItems,
    totalPages,
  };
}

function mapJobSummary(job: {
  id: string;
  pipelineId: string;
  status: JobStatus;
  retryCount: number;
  maxRetries: number;
  nextRunAt: Date;
  lockedAt: Date | null;
  processedAt: Date | null;
  lastError: string | null;
  createdAt: Date;
  updatedAt: Date;
  pipeline: {
    name: string;
  };
}): JobSummary {
  return {
    id: job.id,
    pipelineId: job.pipelineId,
    pipelineName: job.pipeline.name,
    status: job.status,
    retryCount: job.retryCount,
    maxRetries: job.maxRetries,
    nextRunAt: job.nextRunAt,
    lockedAt: job.lockedAt,
    processedAt: job.processedAt,
    lastError: job.lastError,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
  };
}

function mapJobDetails(job: {
  id: string;
  pipelineId: string;
  payload: Prisma.JsonValue;
  result: Prisma.JsonValue | null;
  status: JobStatus;
  retryCount: number;
  maxRetries: number;
  nextRunAt: Date;
  lockedAt: Date | null;
  processedAt: Date | null;
  lastError: string | null;
  createdAt: Date;
  updatedAt: Date;
  pipeline: {
    name: string;
    sourcePath: string;
    actionType: ActionType;
    active: boolean;
  };
}): JobDetails {
  return {
    ...mapJobSummary(job),
    payload: job.payload as JsonValue,
    result: job.result as JsonValue | null,
    pipelineSourcePath: job.pipeline.sourcePath,
    actionType: job.pipeline.actionType,
    pipelineActive: job.pipeline.active,
  };
}

function mapDeliveryAttemptDetails(attempt: {
  id: string;
  jobId: string;
  subscriberId: string;
  attemptNumber: number;
  status: DeliveryStatus;
  nextRunAt: Date;
  lockedAt: Date | null;
  responseCode: number | null;
  error: string | null;
  deliveredAt: Date | null;
  createdAt: Date;
  subscriber: {
    url: string;
  };
}): DeliveryAttemptDetails {
  return {
    id: attempt.id,
    jobId: attempt.jobId,
    subscriberId: attempt.subscriberId,
    subscriberUrl: attempt.subscriber.url,
    attemptNumber: attempt.attemptNumber,
    status: attempt.status,
    nextRunAt: attempt.nextRunAt,
    lockedAt: attempt.lockedAt,
    responseCode: attempt.responseCode,
    error: attempt.error,
    deliveredAt: attempt.deliveredAt,
    createdAt: attempt.createdAt,
  };
}
