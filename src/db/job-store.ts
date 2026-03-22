import { Prisma } from "@prisma/client";
import { prisma } from "./client";
import { EnqueueJobInput, JobStore, QueuedJob } from "../services/webhook-service";

class PrismaJobStore implements JobStore {
  async create(input: EnqueueJobInput): Promise<QueuedJob> {
    const job = await prisma.job.create({
      data: {
        pipelineId: input.pipelineId,
        payload: input.payload as Prisma.InputJsonValue,
      },
    });

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
}

export const prismaJobStore = new PrismaJobStore();
