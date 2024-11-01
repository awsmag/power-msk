import { Mechanism, SASLOptions } from "kafkajs";
import config from "./config";
import { getClient } from "./client";
import { ConnectionOptions } from "tls";
import { createMechanism } from "@jm18457/kafkajs-msk-iam-authentication-mechanism";

export * from "./koa-mw";

export function getKafkaClient(
  clientId: string = config.clientId as string,
  brokers: string[] = config.brokers as string[],
  ssl: boolean | ConnectionOptions = true,
  sasl?: SASLOptions | Mechanism,
) {
  if (!clientId) {
    throw new Error("clientId is required");
  }

  if (!Array.isArray(brokers) || brokers.length === 0) {
    throw new Error("brokers is required");
  }

  return getClient(clientId, brokers, ssl, sasl);
}

export function getAWSIAMAuthMechanism(region: string) {
  return createMechanism({
    region,
  });
}

export * from "kafkajs";
