// power-msk v2 — Koa middleware for a resilient producer
import { DefaultState, Middleware } from "koa";
import { Kafka, KafkaConfig, CompressionTypes, Message } from "kafkajs";
import { ResilientProducer } from "../producer/resilientProducer";

declare module "koa" {
  interface DefaultContext {
    kafkaClient?: {
      sendMessages: <T>(events: T[], topic: string) => Promise<void>;
      isHealthy: () => boolean;
      isReady: () => boolean;
    };
  }
}

export type KoaKafkaOptions =
  | { kafka: Kafka }
  | ({ clientId: string; brokers: string[] } & KafkaConfig);

export function getKafkaClientMw<T = DefaultState>(
  opts: KoaKafkaOptions
): Middleware<T> {
  const kafka =
    "kafka" in opts && (opts as any).kafka instanceof Kafka //eslint-disable-line
      ? (opts as { kafka: Kafka }).kafka
      : new Kafka(opts as KafkaConfig);

  // Single resilient producer for the app (no per-request connect/disconnect)
  const producer = new ResilientProducer({
    kafka,
    idempotent: true,
    acks: -1,
    compression: CompressionTypes.GZIP,
    lingerMs: 10,
    maxBatchSize: 1000,
    maxQueueBytes: 10 * 1024 * 1024,
  });

  let started = false;
  async function ensureStarted() {
    if (!started) {
      started = true;
      await producer.start();
      // graceful shutdown (optional)
      const shutdown = async () => {
        await producer.stop();
        process.exit(0);
      };
      process.once("SIGTERM", shutdown);
      process.once("SIGINT", shutdown);
    }
  }

  async function sendMessages<T>(events: T[], topic: string) {
    if (!Array.isArray(events) || events.length === 0) {
      throw new Error("Events are required for broadcasting");
    }
    const messages: Message[] = events.map((item) => ({
      value: Buffer.from(JSON.stringify(item)),
    }));
    await producer.send(topic, messages);
  }

  return async (ctx, next) => {
    await ensureStarted();
    ctx.kafkaClient = {
      sendMessages,
      isHealthy: () => producer.isHealthy(),
      isReady: () => producer.isReady(),
    };
    await next();
  };
}
