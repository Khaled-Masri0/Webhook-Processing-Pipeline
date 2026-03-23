import { ActionType, JobStatus } from "@prisma/client";
import { executePipelineAction, PipelineActionExecutionResult } from "./pipeline-action-service";
import { ValidationError } from "../utils/errors";
import { JsonValue } from "../utils/json";

export interface ReadyJob {
  id: string;
  pipelineId: string;
  payload: JsonValue;
  actionType: ActionType;
  actionConfig: JsonValue;
  retryCount: number;
  maxRetries: number;
  nextRunAt: Date;
  createdAt: Date;
}

export interface ClaimedJob extends ReadyJob {
  status: JobStatus;
  lockedAt: Date;
}

export interface JobProcessingStore {
  findNextReadyJob(now: Date): Promise<ReadyJob | null>;
  markJobProcessing(jobId: string, lockedAt: Date): Promise<boolean>;
  markJobCompleted(jobId: string, result: JsonValue | null, processedAt: Date): Promise<boolean>;
  markJobFailed(jobId: string, lastError: string, processedAt: Date): Promise<boolean>;
  rescheduleJob(jobId: string, lastError: string, nextRunAt: Date, retryCount: number): Promise<boolean>;
}

export interface JobProcessingService {
  claimNextReadyJob(now?: Date): Promise<ClaimedJob | null>;
  processClaimedJob(job: ClaimedJob, now?: Date): Promise<ProcessedJob>;
}

const MAX_CLAIM_ATTEMPTS = 5;

export interface CompletedJob {
  jobId: string;
  pipelineId: string;
  status: "COMPLETED";
  actionStatus: PipelineActionExecutionResult["status"];
  result: JsonValue | null;
  processedAt: Date;
}

export interface FailedJob {
  jobId: string;
  pipelineId: string;
  status: "FAILED";
  lastError: string;
  processedAt: Date;
}

export interface RetriedJob {
  jobId: string;
  pipelineId: string;
  status: "RETRY_SCHEDULED";
  lastError: string;
  retryCount: number;
  nextRunAt: Date;
}

export type ProcessedJob = CompletedJob | FailedJob | RetriedJob;

const RETRY_DELAY_MS = 60_000;

export function createJobProcessingService(store: JobProcessingStore): JobProcessingService {
  return {
    async claimNextReadyJob(now = new Date()): Promise<ClaimedJob | null> {
      for (let attempt = 0; attempt < MAX_CLAIM_ATTEMPTS; attempt += 1) {
        const readyJob = await store.findNextReadyJob(now);

        if (!readyJob) {
          return null;
        }

        const claimed = await store.markJobProcessing(readyJob.id, now);

        if (claimed) {
          return {
            ...readyJob,
            status: JobStatus.PROCESSING,
            lockedAt: now,
          };
        }
      }

      return null;
    },

    async processClaimedJob(job: ClaimedJob, now = new Date()): Promise<ProcessedJob> {
      let executionResult: PipelineActionExecutionResult;

      try {
        executionResult = executePipelineAction({
          actionType: job.actionType,
          actionConfig: job.actionConfig,
          payload: job.payload,
        });
      } catch (error) {
        const lastError = toErrorMessage(error);

        if (!(error instanceof ValidationError) && job.retryCount < job.maxRetries) {
          const retryCount = job.retryCount + 1;
          const nextRunAt = new Date(now.getTime() + RETRY_DELAY_MS);
          const rescheduled = await store.rescheduleJob(job.id, lastError, nextRunAt, retryCount);

          if (!rescheduled) {
            throw new Error(`Job ${job.id} could not be rescheduled.`);
          }

          return {
            jobId: job.id,
            pipelineId: job.pipelineId,
            status: "RETRY_SCHEDULED",
            lastError,
            retryCount,
            nextRunAt,
          };
        }

        const markedFailed = await store.markJobFailed(job.id, lastError, now);

        if (!markedFailed) {
          throw new Error(`Job ${job.id} could not be marked failed.`);
        }

        return {
          jobId: job.id,
          pipelineId: job.pipelineId,
          status: "FAILED",
          lastError,
          processedAt: now,
        };
      }

      const markedCompleted = await store.markJobCompleted(job.id, executionResult.result, now);

      if (!markedCompleted) {
        throw new Error(`Job ${job.id} could not be marked completed.`);
      }

      return {
        jobId: job.id,
        pipelineId: job.pipelineId,
        status: "COMPLETED",
        actionStatus: executionResult.status,
        result: executionResult.result,
        processedAt: now,
      };
    },
  };
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }

  return "Unknown processing error";
}
