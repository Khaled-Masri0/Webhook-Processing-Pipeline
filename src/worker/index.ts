import { env } from "../config/env";
import { closeDb } from "../db/client";
import { prismaJobStore } from "../db/job-store";
import { createJobProcessingService } from "../services/job-processing-service";

let timer: NodeJS.Timeout | null = null;
const jobProcessingService = createJobProcessingService(prismaJobStore);

async function runTick(): Promise<void> {
  try {
    const claimedJob = await jobProcessingService.claimNextReadyJob();

    if (claimedJob) {
      console.log(`Worker claimed job ${claimedJob.id} for pipeline ${claimedJob.pipelineId}.`);
      const processedJob = await jobProcessingService.processClaimedJob(claimedJob);

      if (processedJob.status === "COMPLETED") {
        console.log(
          `Worker completed job ${processedJob.jobId} for pipeline ${processedJob.pipelineId} with action status ${processedJob.actionStatus}.`,
        );
      } else if (processedJob.status === "RETRY_SCHEDULED") {
        console.warn(
          `Worker rescheduled job ${processedJob.jobId} for pipeline ${processedJob.pipelineId} as retry ${processedJob.retryCount} at ${processedJob.nextRunAt.toISOString()}: ${processedJob.lastError}`,
        );
      } else {
        console.error(
          `Worker failed job ${processedJob.jobId} for pipeline ${processedJob.pipelineId}: ${processedJob.lastError}`,
        );
      }
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown worker error";
    console.error(`Worker tick failed: ${message}`);
  }
}

async function startWorker(): Promise<void> {
  await runTick();
  timer = setInterval(() => {
    void runTick();
  }, env.workerPollMs);

  console.log(`Worker running with poll interval ${env.workerPollMs}ms`);
}

async function shutdown(signal: string): Promise<void> {
  console.log(`Received ${signal}, shutting down worker...`);
  if (timer) {
    clearInterval(timer);
  }
  await closeDb();
  process.exit(0);
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

void startWorker();
