import { Kafka } from "kafkajs";

let client: Kafka;

export function connectKafka(clientId: string, brokers: string[], ssl: boolean = true) {
  client = new Kafka({
    clientId,
    brokers,
    ssl
  });
}

export function getClient(clientId: string,brokers: string[], ssl: boolean) {
  if (!client) {
    connectKafka(clientId, brokers, ssl);
  }
  return client;
}


