import "dotenv/config";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";
import type { QueryCommandOutput } from "@aws-sdk/lib-dynamodb";
import type { RawBatchItem } from "../types/index.js";

export class DynamoRepository {
  private docClient: DynamoDBDocumentClient;
  private tableName: string;

  constructor() {
    const client = new DynamoDBClient({
      region: process.env.AWS_REGION || "sa-east-1",
    });
    this.docClient = DynamoDBDocumentClient.from(client);
    this.tableName = process.env.DYNAMO_TABLE_NAME || "svx_batches";
  }

  async getItemsByBatchId(batchId: string): Promise<RawBatchItem[]> {
    const allItems: RawBatchItem[] = [];
    let lastKey: Record<string, any> | undefined = undefined;
    let pageCount = 0;
    console.log(`[DynamoRepository] Buscando itens para batch: ${batchId}...`);

    do {
      const command = new QueryCommand({
        TableName: this.tableName,
        IndexName: "gsi_locations_sensors", // Seu GSI novo
        KeyConditionExpression: "batch_id = :bid",
        ExpressionAttributeValues: { ":bid": batchId },
        ExclusiveStartKey: lastKey,
      });

      const response: QueryCommandOutput = await this.docClient.send(command);

      if (response.Items) {
        allItems.push(...(response.Items as RawBatchItem[]));
        pageCount++;
      }

      lastKey = response.LastEvaluatedKey;

      if (lastKey) {
        process.stdout.write(`.`);
      }
    } while (lastKey);

    console.log(
      `\n[DynamoRepository] Busca finalizada. ${pageCount} páginas lidas. Total: ${allItems.length} itens.`
    );

    return allItems;
  }
}
