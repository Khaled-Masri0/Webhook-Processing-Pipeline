import { Pipeline as PrismaPipeline, Prisma, Subscriber as PrismaSubscriber } from "@prisma/client";
import { prisma } from "./client";
import { ConflictError } from "../utils/errors";
import { Pipeline, PipelineInput, PipelineStore } from "../services/pipeline-service";
import { WebhookPipeline, WebhookPipelineStore } from "../services/webhook-service";

type PipelineRecord = PrismaPipeline & { subscribers: PrismaSubscriber[] };

const pipelineInclude = {
  subscribers: {
    orderBy: {
      createdAt: "asc",
    },
  },
} satisfies Prisma.PipelineInclude;

class PrismaPipelineStore implements PipelineStore, WebhookPipelineStore {
  async list(): Promise<Pipeline[]> {
    const pipelines = await prisma.pipeline.findMany({
      include: pipelineInclude,
      orderBy: {
        createdAt: "desc",
      },
    });

    return pipelines.map(mapPipelineRecord);
  }

  async findById(id: string): Promise<Pipeline | null> {
    const pipeline = await prisma.pipeline.findUnique({
      where: { id },
      include: pipelineInclude,
    });

    return pipeline ? mapPipelineRecord(pipeline) : null;
  }

  async findBySourcePath(sourcePath: string): Promise<WebhookPipeline | null> {
    const pipeline = await prisma.pipeline.findUnique({
      where: { sourcePath },
    });

    if (!pipeline) {
      return null;
    }

    return {
      id: pipeline.id,
      sourcePath: pipeline.sourcePath,
      active: pipeline.active,
    };
  }

  async create(input: PipelineInput): Promise<Pipeline> {
    try {
      const pipeline = await prisma.pipeline.create({
        data: buildPipelineWriteInput(input),
        include: pipelineInclude,
      });

      return mapPipelineRecord(pipeline);
    } catch (error) {
      throw mapPersistenceError(error);
    }
  }

  async update(id: string, input: PipelineInput): Promise<Pipeline | null> {
    const existingPipeline = await prisma.pipeline.findUnique({ where: { id } });
    if (!existingPipeline) {
      return null;
    }

    try {
      const pipeline = await prisma.pipeline.update({
        where: { id },
        data: {
          ...buildPipelineWriteInput(input),
          subscribers: {
            deleteMany: {},
            create: input.subscribers.map((subscriber) => ({
              url: subscriber.url,
              active: subscriber.active,
            })),
          },
        },
        include: pipelineInclude,
      });

      return mapPipelineRecord(pipeline);
    } catch (error) {
      throw mapPersistenceError(error);
    }
  }

  async delete(id: string): Promise<Pipeline | null> {
    const existingPipeline = await prisma.pipeline.findUnique({
      where: { id },
      include: pipelineInclude,
    });

    if (!existingPipeline) {
      return null;
    }

    await prisma.pipeline.delete({ where: { id } });
    return mapPipelineRecord(existingPipeline);
  }
}

export const prismaPipelineStore = new PrismaPipelineStore();

function buildPipelineWriteInput(input: PipelineInput): Prisma.PipelineCreateInput {
  return {
    name: input.name,
    sourcePath: input.sourcePath,
    actionType: input.actionType,
    actionConfig: input.actionConfig as Prisma.InputJsonValue,
    active: input.active,
    subscribers: {
      create: input.subscribers.map((subscriber) => ({
        url: subscriber.url,
        active: subscriber.active,
      })),
    },
  };
}

function mapPipelineRecord(record: PipelineRecord): Pipeline {
  return {
    id: record.id,
    name: record.name,
    sourcePath: record.sourcePath,
    actionType: record.actionType,
    actionConfig: isPlainObject(record.actionConfig) ? record.actionConfig : {},
    active: record.active,
    subscribers: record.subscribers.map((subscriber) => ({
      id: subscriber.id,
      url: subscriber.url,
      active: subscriber.active,
      createdAt: subscriber.createdAt,
      updatedAt: subscriber.updatedAt,
    })),
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
  };
}

function mapPersistenceError(error: unknown): Error {
  if (isPrismaError(error, "P2002")) {
    return new ConflictError("sourcePath must be unique.");
  }

  if (error instanceof Error) {
    return error;
  }

  return new Error("Unexpected persistence error.");
}

function isPrismaError(error: unknown, code: string): error is { code: string } {
  return typeof error === "object" && error !== null && "code" in error && error.code === code;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
