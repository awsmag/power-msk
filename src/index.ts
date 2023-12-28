import config from "./config";
import { getClient } from "./client";

export * from "./koa-mw";

export function getKafkaClient(
  clientId: string = config.clientId,
  brokers: string[] = config.brokers,
  ssl: boolean = true,
) {
  if (!clientId || !brokers || brokers.length === 0) {
    throw new Error("clientId and broker list are reuired");
  }

  return getClient(clientId, brokers, ssl);
}
