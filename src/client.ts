import { Kafka } from "kafkajs";
import config from "./config";

let client: Kafka;

export function connectKafka(
  clientId: string,
  brokers: string[],
  ssl: boolean = true,
) {
  if (!clientId) {
    throw new Error("clientId is required");
  }

  if(!Array.isArray(brokers) || brokers.length === 0) {
    throw new Error("brokers is required");
  }

  client = new Kafka({
    clientId,
    brokers,
    ssl,
  });
}

export function getClient(
  clientId: string = config.clientId,
  brokers: string[] = config.brokers,
  ssl: boolean = true,
) {
  if (!client) {
    connectKafka(clientId, brokers, ssl);
  }
  return client;
}
