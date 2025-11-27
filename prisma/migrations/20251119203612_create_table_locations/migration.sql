-- CreateTable
CREATE TABLE "location_buckets" (
    "id" SERIAL NOT NULL,
    "batch_id" TEXT NOT NULL,
    "start_time" TIMESTAMP(3) NOT NULL,
    "end_time" TIMESTAMP(3) NOT NULL,
    "count" INTEGER NOT NULL,
    "data" JSONB NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "location_buckets_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "location_buckets_batch_id_start_time_idx" ON "location_buckets"("batch_id", "start_time");
