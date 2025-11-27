import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
} from "@aws-sdk/client-s3";
import { Readable } from "stream";

export class S3Repository {
  private client: S3Client;
  private bucketName: string;

  constructor() {
    this.client = new S3Client({
      region: process.env.AWS_REGION || "us-east-1",
    });
    this.bucketName = process.env.S3_BUCKET_NAME || "seu-bucket-padrao";
  }

  async uploadCsv(fileName: string, csvContent: string): Promise<void> {
    const command = new PutObjectCommand({
      Bucket: this.bucketName,
      Key: `sensores/${fileName}`, // Salva numa pasta organizada
      Body: csvContent,
      ContentType: "text/csv",
    });

    await this.client.send(command);
    console.log(`[S3] Arquivo ${fileName} enviado com sucesso.`);
  }

  async getCsvContent(fileName: string): Promise<string> {
    const command = new GetObjectCommand({
      Bucket: this.bucketName,
      Key: `sensores/${fileName}`,
    });

    const response = await this.client.send(command);
    if (response.Body) {
      return response.Body.transformToString();
    } else {
      throw new Error("Arquivo vazio ou não encontrado no S3");
    }
  }
}
