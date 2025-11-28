import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
} from "@aws-sdk/client-s3";
import { gzipSync } from "zlib";

export class S3Repository {
  private client: S3Client;
  private bucketName: string;

  constructor() {
    this.client = new S3Client({
      region: process.env.AWS_REGION || "sa-east-1",
    });
    this.bucketName = process.env.S3_BUCKET_NAME || "svx-csv";
  }

  async uploadCsv(fileName: string, content: string): Promise<void> {
    const compressedBody = gzipSync(content);
    const command = new PutObjectCommand({
      Bucket: this.bucketName,
      Key: `sensores/${fileName}`,
      Body: compressedBody,
      ContentType: "text/csv",
      ContentEncoding: "gzip",
    });

    await this.client.send(command);
    console.log(
      `[S3] Arquivo ${fileName} enviado (GZIP: ${compressedBody.length} bytes).`
    );
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
