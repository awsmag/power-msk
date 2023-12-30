import { DefaultState, Middleware } from "koa";
import { CompressionTypes, Kafka, Message, Producer } from "kafkajs";
import { getClient } from "./client";
import config from "./config";

async function sendMesgFn(producer: Producer) {
  return async <T>(events: T[], topic: string) => {
    if (Array.isArray(events) === false || events.length === 0) {
      throw new Error("Events are required for boradcasting");
    }

    const messages: Message[] = events.map((item) => ({
      value: JSON.stringify(item),
    }));

    await producer.connect();

    try {
      await producer.send({
        topic,
        compression: CompressionTypes.GZIP,
        messages,
      });
    } catch (error) {
      return Promise.reject(error);
    } finally {
      producer.disconnect();
    }
  };
}

export function getKafkaClientMw<T = DefaultState>(
  clientId: string = config.clientId,
  brokers: string[] = config.brokers,
  ssl: boolean = true,
): Middleware<T> {
  if (!clientId || !brokers || brokers.length === 0) {
    throw new Error("clientId and broker list are reuired");
  }

  const client: Kafka = getClient(clientId, brokers, ssl);

  const producer = client.producer();

  const sendMessages = sendMesgFn(producer);

  return async (ctx, next) => {
    ctx.kafkaClient = {
      sendMessages,
    };

    await next();
  };
}
