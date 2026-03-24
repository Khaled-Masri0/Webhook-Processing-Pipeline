import { ActionType } from "@prisma/client";
import { NotFoundError } from "../utils/errors.js";

export interface PipelineSubscriberInput {
  url: string;
  active: boolean;
}

export interface PipelineInput {
  name: string;
  sourcePath: string;
  actionType: ActionType;
  actionConfig: Record<string, unknown>;
  active: boolean;
  subscribers: PipelineSubscriberInput[];
}

export interface PipelineSubscriber {
  id: string;
  url: string;
  active: boolean;
  createdAt: Date;
  updatedAt: Date;
}

export interface Pipeline {
  id: string;
  name: string;
  sourcePath: string;
  actionType: ActionType;
  actionConfig: Record<string, unknown>;
  active: boolean;
  subscribers: PipelineSubscriber[];
  createdAt: Date;
  updatedAt: Date;
}

export interface PipelineStore {
  list(): Promise<Pipeline[]>;
  findById(id: string): Promise<Pipeline | null>;
  create(input: PipelineInput): Promise<Pipeline>;
  update(id: string, input: PipelineInput): Promise<Pipeline | null>;
  delete(id: string): Promise<Pipeline | null>;
}

export interface PipelineService {
  listPipelines(): Promise<Pipeline[]>;
  getPipeline(id: string): Promise<Pipeline>;
  createPipeline(input: PipelineInput): Promise<Pipeline>;
  updatePipeline(id: string, input: PipelineInput): Promise<Pipeline>;
  deletePipeline(id: string): Promise<Pipeline>;
}

export function createPipelineService(store: PipelineStore): PipelineService {
  return {
    async listPipelines(): Promise<Pipeline[]> {
      return store.list();
    },

    async getPipeline(id: string): Promise<Pipeline> {
      const pipeline = await store.findById(id);
      if (!pipeline) {
        throw new NotFoundError(`Pipeline ${id} was not found.`);
      }

      return pipeline;
    },

    async createPipeline(input: PipelineInput): Promise<Pipeline> {
      return store.create(input);
    },

    async updatePipeline(id: string, input: PipelineInput): Promise<Pipeline> {
      const pipeline = await store.update(id, input);
      if (!pipeline) {
        throw new NotFoundError(`Pipeline ${id} was not found.`);
      }

      return pipeline;
    },

    async deletePipeline(id: string): Promise<Pipeline> {
      const pipeline = await store.delete(id);
      if (!pipeline) {
        throw new NotFoundError(`Pipeline ${id} was not found.`);
      }

      return pipeline;
    },
  };
}
