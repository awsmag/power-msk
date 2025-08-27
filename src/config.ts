import { optional, record, string } from "typescript-json-decoder";

const envDecoder = record({
  POWER_MSK_LOGGER_LEVEL: optional(string),
});

const data = envDecoder(process.env);

const config: Record<string, any> = {
  loggerLevel: data.POWER_MSK_LOGGER_LEVEL ?? "error",
};

export default Object.freeze(config);