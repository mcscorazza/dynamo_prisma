import { PrismaClient } from "@prisma/client";
import type { LocationBucketData } from "../types/index.js";

export class PostgresRepository {
  private prisma: PrismaClient;

  constructor(prismaClient: PrismaClient) {
    this.prisma = prismaClient;
  }

  async saveBuckets(buckets: LocationBucketData[]): Promise<void> {
    if (buckets.length === 0) return;

    await this.prisma.locationBucket.createMany({
      data: buckets,
      skipDuplicates: true,
    });
  }
}
