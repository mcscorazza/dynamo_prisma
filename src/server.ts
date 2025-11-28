import "dotenv/config";
import Fastify from "fastify";
import cors from "@fastify/cors";
import fastifyStatic from "@fastify/static";
import path from "path";
import { fileURLToPath } from "url";
import { S3Repository } from "./repositories/S3Repository.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const fastify = Fastify({ logger: true });
const s3Repo = new S3Repository();

await fastify.register(cors, {
  origin: true,
});

await fastify.register(fastifyStatic, {
  root: path.join(process.cwd(), "public"),
  prefix: "/",
});

fastify.get("/api/csv", async (request, reply) => {
  const { file } = request.query as { file: string };
  if (!file) {
    return reply
      .status(400)
      .send({ error: "Nome do arquivo é obrigatório (?file=...)" });
  }
  try {
    console.log(`Buscando arquivo no S3: ${file}`);
    const csvContent = await s3Repo.getCsvContent(file);
    reply.type("text/csv").send(csvContent);
  } catch (err) {
    request.log.error(err);
    return reply
      .status(500)
      .send({ error: "Erro ao buscar arquivo no S3. Verifique o nome." });
  }
});

const start = async () => {
  try {
    const port = 3000;
    await fastify.listen({ port, host: "0.0.0.0" });
    console.log(`\n📡 Servidor rodando em: http://localhost:${port}`);
    console.log(
      `👉 Para testar: http://localhost:${port}/api/csv?file=NOME_DO_SEU_ARQUIVO.csv\n`
    );
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
