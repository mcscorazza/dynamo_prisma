import "dotenv/config";
import { PrismaClient } from "@prisma/client";
import { DynamoRepository } from "./repositories/DynamoRepository.js";
import { PostgresRepository } from "./repositories/PostgresRepository.js";
import { EtlService } from "./services/EtlService.js";
import { BatchController } from "./controllers/BatchController.js";
import { S3Repository } from "./repositories/S3Repository.js";

async function main() {
  const prisma = new PrismaClient();
  try {
    const dynamoRepo = new DynamoRepository();
    const pgRepo = new PostgresRepository(prisma);
    const s3Repo = new S3Repository();
    const etlService = new EtlService(dynamoRepo, pgRepo, s3Repo);
    const controller = new BatchController(etlService);
    const batchIdParaProcessar = "e25d9c5e-a796-4933-9b1a-33fb36fe7f41";
    await controller.handle(batchIdParaProcessar);
  } catch (err) {
    console.error("Erro fatal:", err);
  } finally {
    await prisma.$disconnect();
  }
}

main();
