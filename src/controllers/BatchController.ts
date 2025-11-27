import { EtlService } from "../services/EtlService.js";

export class BatchController {
  constructor(private etlService: EtlService) {}

  async handle(batchId: string) {
    try {
      if (!batchId) {
        throw new Error("Batch ID é obrigatório!");
      }

      console.time("Tempo de Execução");
      await this.etlService.execute(batchId);
      console.timeEnd("Tempo de Execução");
      
      console.log("✅ Processo finalizado.");
    } catch (error) {
      console.error("❌ Erro no Controller:", error);
    }
  }
}