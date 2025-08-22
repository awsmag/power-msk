import type { Consumer } from "kafkajs";

export type RestartFn = (reason: string, err?: unknown) => Promise<void> | void;

export function attachConsumerResilience(
  consumer: Consumer,
  opts: {
    onNonRetriable?: RestartFn; // called when KafkaJS won’t auto-restart (STOP or CRASH with restart=false)
    onCrashed?: RestartFn; // called on any crash (observability hook)
  } = {},
) {

  let healthy = false;
  let ready = false;

  consumer.on(consumer.events.CONNECT, () => {
    healthy = true;
    ready = true;
  });

  consumer.on(consumer.events.DISCONNECT, () => {
    healthy = false;
    ready = false;
  });

  consumer.on(consumer.events.STOP, () => {
    healthy = false;
    ready = false;
    opts.onNonRetriable?.("STOP");
  });

  consumer.on(consumer.events.CRASH, (e) => {
    const { error, restart } = e?.payload || {};
    opts.onCrashed?.("CRASH", error);
    if (!restart) {
      healthy = false;
      ready = false;
      opts.onNonRetriable?.("CRASH_NON_RETRIABLE", error);
    }
  });

  return {
    getHealthy: () => healthy,
    getReady: () => ready,
  };
}
