// src/koa/producerMw.ts
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

// NEW: type augmentation for shutdown on the middleware function
export type KafkMwWithShutdown<T = DefaultState> = Middleware<T> & {
  shutdown: () => Promise<void>;
};

export type KoaKafkaOptions =
  | { kafka: Kafka }
  | ({ clientId: string; brokers: string[] } & KafkaConfig);

export function getKafkaClientMw<T = DefaultState>(
  opts: KoaKafkaOptions
): KafkMwWithShutdown<T> {
  const providedKafka = (opts as any).kafka;
  const kafka =
    providedKafka && typeof providedKafka.producer === "function"
      ? (providedKafka as Kafka)
      : new Kafka(opts as KafkaConfig);

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
  const sig = {
    term: async () => { try { await producer.stop(); } catch {} },
    int: async () => { try { await producer.stop(); } catch {} },
  };
  function ensureStarted() {
    if (!started) {
      started = true;
      producer.start().catch(() => {});
      process.once("SIGTERM", sig.term);
      process.once("SIGINT", sig.int);
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

  const mw: any = async (ctx: any, next: any) => {
    ensureStarted();
    ctx.kafkaClient = {
      sendMessages,
      isHealthy: () => producer.isHealthy(),
      isReady: () => producer.isReady(),
    };
    await next();
  };

  mw.shutdown = async () => {
    if (started) {
      started = false;
      // remove our listeners so multiple tests don’t pile up
      process.removeListener("SIGTERM", sig.term);
      process.removeListener("SIGINT", sig.int);
      await producer.stop();
    }
  };

  return mw as KafkMwWithShutdown<T>;
}
