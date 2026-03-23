-- AlterTable
ALTER TABLE "delivery_attempts"
ADD COLUMN "nextRunAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN "lockedAt" TIMESTAMP(3);

-- CreateIndex
CREATE INDEX "delivery_attempts_status_nextRunAt_idx" ON "delivery_attempts"("status", "nextRunAt");
