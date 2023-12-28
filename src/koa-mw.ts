import { DefaultContext, DefaultState, Middleware } from "koa";
import { getClient } from "./client";
import config from "./config";
import { Kafka } from "kafkajs";

export function getKafkaClientMw<T = DefaultState>(
  clientId: string = config.clientId,
  brokers: string[] = config.brokers,
  ssl: boolean = true,
): Middleware<T> {
  if (!clientId || !brokers || brokers.length === 0) {
    throw new Error("clientId and broker list are reuired");
  }

  return async (ctx, next) => {
    const client: Kafka =  getClient(clientId, brokers, ssl);
    ctx.kafkaClient = client;

    await next();
  };
}
