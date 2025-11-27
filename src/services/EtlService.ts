import type { LocationBucketData, RawBatchItem } from "../types/index.js";
import Papa from "papaparse";
import { DynamoRepository } from "../repositories/DynamoRepository.js";
import { PostgresRepository } from "../repositories/PostgresRepository.js";
import { S3Repository } from "../repositories/S3Repository.js";

interface CsvRowWithLoc {
  timestamp: number;
  lat: number;
  lon: number;
  [sensorId: string]: number | string | null;
}

export class EtlService {
  constructor(
    private dynamoRepo: DynamoRepository,
    private pgRepo: PostgresRepository,
    private s3Repo: S3Repository
  ) {}

  async execute(batchId: string) {
    console.log(`[Service] Iniciando processamento do Batch: ${batchId}`);

    const rawItems = await this.dynamoRepo.getItemsByBatchId(batchId);
    console.log(`[Service] Encontrados ${rawItems.length} itens no Dynamo.`);

    if (rawItems.length === 0) return;

    const buckets = this.bucketizeData(rawItems);

    if (buckets.length > 0) {
      console.log(`[Service] Gerados ${buckets.length} buckets.`);
      await this.pgRepo.saveBuckets(buckets);
      console.log(`[Service] ✅ Sucesso! Buckets salvos no Postgres.`);
    } else {
      console.log(
        "[Service] ⚠️ Nenhum dado válido encontrado para bucketização."
      );
    }
    if (rawItems.length > 0) {
      await this.processSensorCsv(batchId, rawItems);
    }
  }

  private async processSensorCsv(batchId: string, items: RawBatchItem[]) {
    console.log(`[Service] Iniciando processamento CSV para ${batchId}...`);
    const rowsMap = new Map<number, CsvRowWithLoc>();
    const allSensorIds = new Set<string>();

    for (const item of items) {
      if (!item.sensors || !Array.isArray(item.sensors)) continue;

      let currentLat = 0;
      let currentLon = 0;
      if (Array.isArray(item.position) && item.position.length >= 2) {
        currentLat = item.position[0] || 0;
        currentLon = item.position[1] || 0;
      }

      for (const sensor of item.sensors) {
        const sensorValues = sensor.value || sensor.values;
        const sensorId = sensor.id;
        const sensorBaseTime = Number(sensor.timestamp) * 1000;
        if (
          !sensorId ||
          !sensorBaseTime ||
          !Array.isArray(sensorValues) ||
          sensorValues.length === 0
        ) {
          continue;
        }
        allSensorIds.add(sensorId);

        const totalSamples = sensorValues.length;
        const durationMs = 1000;
        const step = durationMs / totalSamples;

        sensorValues.forEach((val: any, index: number) => {
          const offset = Math.round(index * step);
          const sampleTimestamp = sensorBaseTime + offset;
          if (!rowsMap.has(sampleTimestamp)) {
            rowsMap.set(sampleTimestamp, {
              timestamp: sampleTimestamp,
              lat: currentLat,
              lon: currentLon,
            });
          }
          const row = rowsMap.get(sampleTimestamp)!;
          row[sensorId] = Number(val);
        });
      }
    }
    const sortedRows = Array.from(rowsMap.values()).sort(
      (a, b) => a.timestamp - b.timestamp
    );
    if (sortedRows.length === 0) {
      console.log("[Service] Nenhum dado de sensor encontrado para CSV.");
      return;
    }
    console.log(`[Service] Total de linhas geradas: ${sortedRows.length}`);
    if (sortedRows.length > 0) {
      const firstRow = sortedRows[0];
      const lastRow = sortedRows[sortedRows.length - 1];
      if (firstRow && lastRow) {
        console.log(
          `[Service] Início: ${new Date(firstRow.timestamp).toISOString()}`
        );
        console.log(
          `[Service] Fim:    ${new Date(lastRow.timestamp).toISOString()}`
        );
      }
    }
    const csv = Papa.unparse(sortedRows, {
      columns: ["timestamp", "lat", "lon", ...Array.from(allSensorIds)],
      header: true,
    });
    const fileName = `sensors_${batchId}_${Date.now()}.csv`;
    await this.s3Repo.uploadCsv(fileName, csv);
  }

  private bucketizeData(items: RawBatchItem[]): LocationBucketData[] {
    const bucketsMap = new Map<string, any>();

    for (const item of items) {
      const rawTs = item.timestamp;

      if (!rawTs || rawTs === 0) continue;

      const dateObj = new Date(rawTs);

      let lat = 0;
      let lon = 0;

      if (Array.isArray(item.position) && item.position.length >= 2) {
        lat = item.position[0] || 0;
        lon = item.position[1] || 0;
      } else {
        continue;
      }

      const bucketKeyTime = new Date(dateObj);
      bucketKeyTime.setMinutes(0, 0, 0);
      const key = bucketKeyTime.toISOString();

      if (!bucketsMap.has(key)) {
        bucketsMap.set(key, {
          batchId: item.batch_id,
          startTime: bucketKeyTime,
          endTime: dateObj,
          points: [],
        });
      }

      const currentBucket = bucketsMap.get(key);

      currentBucket.points.push({
        ts: dateObj.toISOString(),
        lat: lat,
        lon: lon,
      });

      if (dateObj > currentBucket.endTime) {
        currentBucket.endTime = dateObj;
      }
    }

    return Array.from(bucketsMap.values()).map((b) => ({
      batchId: b.batchId,
      startTime: b.startTime,
      endTime: b.endTime,
      count: b.points.length,
      data: b.points,
    }));
  }
}
