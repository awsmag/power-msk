import {record, string, optional } from "typescript-json-decoder";

const envDecoder = record({
  KAFKA_CLIENTID: optional(string),
  KAFKA_BROKERS: optional(string)
});

const data = envDecoder(process.env);

const config: Record<string, string | null | Array<string>> = {
  clientId: data.KAFKA_CLIENTID || null,
  brokers: data.KAFKA_BROKERS ?  data.KAFKA_BROKERS.split(",") : []
}

export default Object.freeze(config);