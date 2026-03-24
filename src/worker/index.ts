import { env } from "../config/env.js";
import { closeDb } from "../db/client.js";
import { prismaJobStore } from "../db/job-store.js";
import { prismaPipelineStore } from "../db/pipeline-store.js";
import { createJobProcessingService } from "../services/job-processing-service.js";
import {
  createSubscriberDeliveryService,
  shouldDeliverProcessedJob,
} from "../services/subscriber-delivery-service.js";

let timer: NodeJS.Timeout | null = null;
const jobProcessingService = createJobProcessingService(prismaJobStore);
const subscriberDeliveryService = createSubscriberDeliveryService(prismaPipelineStore, prismaJobStore);

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

        if (shouldDeliverProcessedJob(processedJob)) {
          const deliverySummary = await subscriberDeliveryService.deliverJobResult({
            jobId: processedJob.jobId,
            pipelineId: processedJob.pipelineId,
            payload: processedJob.result,
          });

          console.log(
            `Worker delivered job ${deliverySummary.jobId} to ${deliverySummary.totalSubscribers} subscribers (${deliverySummary.deliveredCount} succeeded, ${deliverySummary.failedCount} failed).`,
          );
        } else {
          console.log(`Worker skipped subscriber delivery for job ${processedJob.jobId}.`);
        }
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

    const deliveryRetry = await subscriberDeliveryService.processNextDeliveryRetry();

    if (deliveryRetry?.status === "SUCCESS") {
      console.log(
        `Worker delivered retry attempt ${deliveryRetry.attemptNumber} for job ${deliveryRetry.jobId} to subscriber ${deliveryRetry.subscriberId}.`,
      );
    } else if (deliveryRetry?.status === "RETRY_SCHEDULED") {
      console.warn(
        `Worker rescheduled delivery retry after attempt ${deliveryRetry.attemptNumber} for job ${deliveryRetry.jobId} to subscriber ${deliveryRetry.subscriberId} at ${deliveryRetry.nextRunAt.toISOString()}: ${deliveryRetry.lastError}`,
      );
    } else if (deliveryRetry?.status === "FAILED") {
      console.error(
        `Worker exhausted delivery retries at attempt ${deliveryRetry.attemptNumber} for job ${deliveryRetry.jobId} to subscriber ${deliveryRetry.subscriberId}: ${deliveryRetry.lastError}`,
      );
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
