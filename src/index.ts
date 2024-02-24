import { Kafka as KafkaType, Producer as ProducerType, Consumer as ConsumerType } from "kafkajs";
import config from "./config";
import { getClient } from "./client";

export * from "./koa-mw";

export function getKafkaClient(
  clientId: string = config.clientId,
  brokers: string[] = config.brokers,
  ssl: boolean = true,
) {
  if (!clientId) {
    throw new Error("clientId is required");
  }

  if(!Array.isArray(brokers) || brokers.length === 0) {
    throw new Error("brokers is required");
  }

  return getClient(clientId, brokers, ssl);
}

export type Kafka = KafkaType;

export type Producer = ProducerType;

export type Consumer = ConsumerType;
