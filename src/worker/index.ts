import { env } from "../config/env";
import { closeDb, prisma } from "../db/client";

let timer: NodeJS.Timeout | null = null;

async function runTick(): Promise<void> {
  try {
    const pendingJobs = await prisma.job.count({
      where: {
        status: "PENDING",
        nextRunAt: {
          lte: new Date(),
        },
      },
    });

    if (pendingJobs > 0) {
      console.log(`Worker heartbeat: ${pendingJobs} pending jobs ready for processing.`);
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
