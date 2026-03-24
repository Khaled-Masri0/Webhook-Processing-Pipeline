import { ActionType, DeliveryStatus, JobStatus } from "@prisma/client";
import { NotFoundError } from "../utils/errors.js";
import { JsonValue } from "../utils/json.js";

export interface JobSummary {
  id: string;
  pipelineId: string;
  pipelineName: string;
  status: JobStatus;
  retryCount: number;
  maxRetries: number;
  nextRunAt: Date;
  lockedAt: Date | null;
  processedAt: Date | null;
  lastError: string | null;
  createdAt: Date;
  updatedAt: Date;
}

export interface JobDetails extends JobSummary {
  payload: JsonValue;
  result: JsonValue | null;
  pipelineSourcePath: string;
  actionType: ActionType;
  pipelineActive: boolean;
}

export interface DeliveryAttemptDetails {
  id: string;
  jobId: string;
  subscriberId: string;
  subscriberUrl: string;
  attemptNumber: number;
  status: DeliveryStatus;
  nextRunAt: Date;
  lockedAt: Date | null;
  responseCode: number | null;
  error: string | null;
  deliveredAt: Date | null;
  createdAt: Date;
}

export interface PaginationInput {
  page: number;
  pageSize: number;
}

export interface PaginatedResult<T> {
  items: T[];
  page: number;
  pageSize: number;
  totalItems: number;
  totalPages: number;
}

export interface JobHistoryQuery extends PaginationInput {
  pipelineId?: string;
  status?: JobStatus;
  createdAfter?: Date;
  createdBefore?: Date;
}

export interface DeliveryHistoryQuery extends PaginationInput {
  status?: DeliveryStatus;
}

export interface JobQueryStore {
  findJobById(id: string): Promise<JobDetails | null>;
  listJobs(query: JobHistoryQuery): Promise<PaginatedResult<JobSummary>>;
  listDeliveryAttempts(
    jobId: string,
    query: DeliveryHistoryQuery,
  ): Promise<PaginatedResult<DeliveryAttemptDetails>>;
}

export interface JobQueryService {
  getJob(id: string): Promise<JobDetails>;
  listJobs(query: JobHistoryQuery): Promise<PaginatedResult<JobSummary>>;
  listJobDeliveries(
    jobId: string,
    query: DeliveryHistoryQuery,
  ): Promise<PaginatedResult<DeliveryAttemptDetails>>;
}

export function createJobQueryService(store: JobQueryStore): JobQueryService {
  return {
    async getJob(id: string): Promise<JobDetails> {
      const job = await store.findJobById(id);

      if (!job) {
        throw new NotFoundError(`Job ${id} was not found.`);
      }

      return job;
    },

    async listJobs(query: JobHistoryQuery): Promise<PaginatedResult<JobSummary>> {
      return store.listJobs(query);
    },

    async listJobDeliveries(
      jobId: string,
      query: DeliveryHistoryQuery,
    ): Promise<PaginatedResult<DeliveryAttemptDetails>> {
      const job = await store.findJobById(jobId);

      if (!job) {
        throw new NotFoundError(`Job ${jobId} was not found.`);
      }

      return store.listDeliveryAttempts(jobId, query);
    },
  };
}
