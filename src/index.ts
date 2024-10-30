import { Mechanism, SASLOptions } from "kafkajs";
import config from "./config";
import { getClient } from "./client";
import { ConnectionOptions } from "tls";

export * from "./koa-mw";

export function getKafkaClient(
  clientId: string = config.clientId,
  brokers: string[] = config.brokers,
  ssl: boolean | ConnectionOptions = true,
  sasl?: SASLOptions | Mechanism,
) {
  if (!clientId) {
    throw new Error("clientId is required");
  }

  if (!Array.isArray(brokers) || brokers.length === 0) {
    throw new Error("brokers is required");
  }

  return getClient(clientId, brokers, ssl, sasl as any);
}

export * from "kafkajs";
