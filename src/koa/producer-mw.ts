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

  const providedKafka = (opts as any).kafka;
  const kafka =
    providedKafka && typeof providedKafka.producer === "function"
      ? (providedKafka as Kafka)
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
  function ensureStarted() {
    if (!started) {
      started = true;
      // fire-and-forget; do NOT await (start() runs until stop())
      producer.start().catch(() => {/* swallow; supervisor logs elsewhere */});

      // optional: graceful stop on signals (do NOT exit the process here)
      const shutdown = async () => {
        try { await producer.stop(); } catch {}
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
    ensureStarted();
    ctx.kafkaClient = {
      sendMessages,
      isHealthy: () => producer.isHealthy(),
      isReady: () => producer.isReady(),
    };
    await next();
  };
}
