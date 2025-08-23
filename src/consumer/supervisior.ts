import {
  Consumer,
  ConsumerRunConfig,
  EachBatchPayload,
  Kafka,
  EachMessagePayload,
} from "kafkajs";
import { attachConsumerResilience } from "./consumerResilience";

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
function backoffMs(base: number, attempt: number, cap = 30_000) {
  const exp = Math.min(cap, base * 2 ** attempt);
  return Math.floor(Math.random() * exp); // full jitter
}

export interface SupervisorOpts {
  kafka: Kafka;
  groupId: string;
  topics: string[];
  fromBeginning?: boolean;
  sessionTimeout?: number;
  rebalanceTimeout?: number;
  baseRetryMs?: number;

  // choose ONE
  eachBatch?: (ctx: EachBatchPayload) => Promise<void>;
  eachMessage?: (ctx: EachMessagePayload) => Promise<void>;
  runConfig?: Omit<
    Parameters<Consumer["run"]>[0],
    "eachBatch" | "eachMessage"
  > &
    ConsumerRunConfig;
}

type IOpts = Required<
  Omit<SupervisorOpts, "runConfig" | "eachBatch" | "eachMessage">
> & {
  runConfig?: SupervisorOpts["runConfig"];
  eachBatch?: SupervisorOpts["eachBatch"];
  eachMessage?: SupervisorOpts["eachMessage"];
  onCrashed?: () => Promise<void> | void;
};

export class ConsumerSupervisor {
  private stopping = false;
  private healthy = false;
  private ready = false;
  private opts: IOpts;

  constructor(opts: SupervisorOpts) {
    if (!opts.eachBatch && !opts.eachMessage) {
      throw new Error("Provide either eachBatch or eachMessage.");
    }
    if (opts.eachBatch && opts.eachMessage) {
      throw new Error("Provide only one of eachBatch or eachMessage.");
    }
    this.opts = {
      ...opts,
      baseRetryMs: opts.baseRetryMs ?? 1000,
    } as IOpts;
  }

  isHealthy() {
    return this.healthy;
  }
  isReady() {
    return this.ready;
  }
  async stop() {
    this.stopping = true;
  }

  async startForever() {
    const { baseRetryMs } = this.opts;
    let attempt = 0;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      if (this.stopping) break;
      try {
        await this.oneLifecycle();
        if (this.stopping) break;
        attempt++;
      } catch {
        attempt++;
      }
      const delay = backoffMs(baseRetryMs, attempt);
      await sleep(delay);
    }
    this.healthy = false;
    this.ready = false;
  }

  private async oneLifecycle() {
    const {
      kafka,
      groupId,
      topics,
      fromBeginning,
      sessionTimeout,
      rebalanceTimeout,
      runConfig,
      eachBatch,
      eachMessage,
    } = this.opts;

    let consumer = kafka.consumer({
      groupId,
      sessionTimeout,
      rebalanceTimeout,
    });

    async function recreate() {
      await consumer.disconnect();
      consumer = kafka.consumer({ groupId, sessionTimeout, rebalanceTimeout });
      attach(); // reattach listeners after recreation
      await consumer.connect();
      for (const topic of topics) {
        await consumer.subscribe({ topic, fromBeginning: !!fromBeginning });
      }
      await run(); // resume run loop
    }

    function attach() {
      attachConsumerResilience(consumer, {
        onNonRetriable: async (reason, err) => {
          console.log(reason, err);
          await recreate();
        },
        onCrashed: this.opts.onCrashed ? this.opts.onCrashed : () => {},
      });
    }

    async function run() {
      if (eachBatch) {
        await consumer.run({
          ...runConfig,
          autoCommit: runConfig?.autoCommit ?? false,
          eachBatchAutoResolve: runConfig?.eachBatchAutoResolve ?? false,
          eachBatch: async (ctx) => {
            try {
              await eachBatch(ctx);
              await ctx.commitOffsetsIfNecessary?.();
            } catch (err) {
              // pause the failing partition briefly to avoid hot loops
              const sel = [
                {
                  topic: ctx.batch.topic,
                  partitions: [ctx.batch.partition] as number[],
                },
              ];
              ctx.pause();
              setTimeout(() => {
                consumer.resume(sel);
              }, 5000);
              throw err;
            }
          },
        });
      } else if (eachMessage) {
        await consumer.run({
          ...runConfig,
          autoCommit: runConfig?.autoCommit ?? false,
          eachMessage: async (ctx) => {
            const { topic, partition, message, heartbeat, pause } = ctx;
            try {
              await eachMessage({
                topic,
                partition,
                message,
                heartbeat,
                pause,
              });
            } catch (err) {
              const sel = [{ topic, partitions: [partition] as number[] }];
              pause();
              setTimeout(() => {
                consumer.resume(sel);
              }, 5000);
              throw err;
            }
          },
        });
      }
    }

    attach();
    await consumer.connect();
    for (const topic of topics) {
      await consumer.subscribe({ topic, fromBeginning: !!fromBeginning });
    }

    this.healthy = true;
    this.ready = true;

    await run();

    await consumer.disconnect();
    this.healthy = false;
    this.ready = false;
  }
}
