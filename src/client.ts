import { Kafka, Mechanism, SASLOptions } from "kafkajs";
import config from "./config";
import { ConnectionOptions } from "tls";
import { createMechanism } from "@jm18457/kafkajs-msk-iam-authentication-mechanism";

let client: Kafka;

export function connectKafka(
  clientId: string,
  brokers: string[],
  ssl: boolean | ConnectionOptions = true,
  sasl?: SASLOptions | Mechanism,
) {
  if (!clientId) {
    throw new Error("clientId is required");
  }

  if (!Array.isArray(brokers) || brokers.length === 0) {
    throw new Error("brokers is required");
  }

  client = new Kafka({
    clientId,
    brokers,
    ssl,
    sasl,
  });
}

export function getClient(
  clientId: string = config.clientId as string,
  brokers: string[] = config.brokers as string[],
  ssl: boolean | ConnectionOptions = true,
  sasl?: SASLOptions | Mechanism,
) {
  if (!client) {
    connectKafka(clientId, brokers, ssl, sasl);
  }
  return client;
}

export function getAWSIAMAuthMechanism(region: string) {
  return createMechanism({
    region,
  });
}
