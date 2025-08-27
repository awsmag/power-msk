import {
  Consumer,
  ConsumerConfig,
  ConsumerRunConfig,
  EachBatchPayload,
  EachMessagePayload,
  Kafka,
} from "kafkajs";
import getLogger, { Logger } from "pino";

import config from "../config";

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
function backoffMs(base: number, attempt: number, cap = 30_000) {
  const exp = Math.min(cap, base * 2 ** attempt);
  return Math.floor(Math.random() * exp); // full jitter
}

function nextOffset(offset: string): string {
  return (BigInt(offset) + 1n).toString();
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

  consumerConfig?: ConsumerConfig;

  onCrashed?: () => Promise<void> | void;

  logger?: Logger;
}

type IOpts = Required<
  Omit<SupervisorOpts, "runConfig" | "eachBatch" | "eachMessage" | "onCrashed">
> & {
  runConfig?: SupervisorOpts["runConfig"];
  eachBatch?: SupervisorOpts["eachBatch"];
  eachMessage?: SupervisorOpts["eachMessage"];
  onCrashed?: SupervisorOpts["onCrashed"];
  consumerConfig?: SupervisorOpts["consumerConfig"];
};

export class ConsumerSupervisor {
  private stopping = false;
  private healthy = false;
  private ready = false;
  private lifecycleInFlight = false;
  private attempt = 0;
  private opts: IOpts;
  private log: Logger;

  constructor(opts: SupervisorOpts) {
    if (!opts.eachBatch && !opts.eachMessage) {
      throw new Error("Provide either eachBatch or eachMessage.");
    }

    if (opts.eachBatch && opts.eachMessage) {
      throw new Error("Provide only one of eachBatch or eachMessage.");
    }

    this.opts = { ...(opts as IOpts), baseRetryMs: opts.baseRetryMs ?? 1000 };

    this.log =
      opts.logger ??
      getLogger({
        name: "POWER_MSK_LOGGER",
        level: config.loggerLevel,
      });
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
    if (this.lifecycleInFlight) {
      return;
    }
    const base = this.opts.baseRetryMs;

    // eslint-disable-next-line no-constant-condition
    while (true) {
      if (this.stopping) {
        break;
      }

      try {
        this.lifecycleInFlight = true;
        const outcome = await this.oneLifecycle(); // returns "ok" | "fatal"
        this.lifecycleInFlight = false;

        if (this.stopping) {
          break;
        }

        if (outcome === "ok") {
          // normal stop — don’t restart
          break;
        } else {
          // fatal crash: backoff and recreate
          this.attempt++;
          const d = backoffMs(base, this.attempt);
          this.log.warn(
            `[POWER_MSK_CONSUMER_SUPERVISOR] lifecycle ended after fatal crash; restarting in ${d}ms`,
          );
          await sleep(d);
        }
      } catch (err) {
        this.lifecycleInFlight = false;
        this.attempt++;
        const d = backoffMs(base, this.attempt);
        this.log.error(
          { error: err },
          "[POWER_MSK_CONSUMER_SUPERVISOR] lifecycle error; restarting",
        );
        await sleep(d);
      }
    }
    this.healthy = false;
    this.ready = false;
    this.log.info("[POWER_MSK_CONSUMER_SUPERVISOR] stopped");
  }

  /**
   * Start one consumer instance.
   * Returns:
   *  - "ok"    → graceful stop requested
   *  - "fatal" → unrecoverable crash (restart lifecycle)
   */
  private async oneLifecycle(): Promise<"ok" | "fatal"> {
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
      onCrashed,
      consumerConfig,
    } = this.opts;

    if (!topics || topics.length === 0) {
      throw new Error("ConsumerSupervisor: 'topics' must be a non-empty array");
    }

    const consumer = kafka.consumer({
      ...consumerConfig,
      groupId,
      sessionTimeout: sessionTimeout ?? 30_000,
      rebalanceTimeout: rebalanceTimeout ?? 60_000,
    });
    this.log.debug("[POWER_MSK_CONSUMER_SUPERVISOR] consumer created");

    let fatalCrash = false;

    const crashedHook =
      onCrashed ??
      (() => {
        /* no-op */
      });

    // ---- events (log-only) ----
    consumer.on(consumer.events.CONNECT, () =>
      this.log.debug("[POWER_MSK_CONSUMER_SUPERVISOR] CONNECT"),
    );
    consumer.on(consumer.events.DISCONNECT, (e) =>
      this.log.warn(
        { payload: e?.payload },
        "[POWER_MSK_CONSUMER_SUPERVISOR] Disconnected",
      ),
    );
    consumer.on(consumer.events.STOP, () =>
      this.log.warn("[POWER_MSK_CONSUMER_SUPERVISOR] STOP"),
    );
    consumer.on(consumer.events.GROUP_JOIN, (e) => {
      const ma: any = e?.payload?.memberAssignment;
      let assignments: Array<{ topic: string; partitions: number[] }> = [];
      if (Array.isArray(ma)) {
        assignments = ma.map((x: any) => ({
          topic: String(x?.topic),
          partitions: Array.isArray(x?.partitions) ? x.partitions : [],
        }));
      } else if (ma && typeof ma === "object") {
        assignments = Object.entries(ma).map(([topic, parts]) => ({
          topic,
          partitions: Array.isArray(parts) ? (parts as number[]) : [],
        }));
      }
      this.log.debug(
        {
          generationId: (e?.payload as any)?.generationId,
          isLeader: (e?.payload as any)?.isLeader,
          assigned: assignments.map(
            (a) => `${a.topic}[${a.partitions.length}]`,
          ),
        },
        "[POWER_MSK_CONSUMER_SUPERVISOR] GROUP_JOIN",
      );
    });
    consumer.on(consumer.events.CRASH, (e) => {
      const { error, restart } = (e?.payload ?? {}) as any;
      this.log.error(
        {
          restart,
          name: error?.name,
          msg: error?.message,
        },
        "[POWER_MSK_CONSUMER_SUPERVISOR] CRASH",
      );
      try {
        crashedHook();
      } catch {}
      // KafkaJS will auto-restart if restart===true. If false, we mark fatal and let the outer loop recreate.
      if (restart === false) fatalCrash = true;
    });

    // Connect & subscribe once
    this.log.debug("[POWER_MSK_CONSUMER_SUPERVISOR] connect...");
    await consumer.connect().catch((e) => {
      this.log.error(
        { error: e },
        "[POWER_MSK_CONSUMER_SUPERVISOR] connect failed",
      );
      throw e;
    });
    for (const topic of topics) {
      this.log.debug(
        { topic, fromBeginning: !!fromBeginning },
        "[POWER_MSK_CONSUMER_SUPERVISOR] subscribe",
      );
      await consumer
        .subscribe({ topic, fromBeginning: !!fromBeginning })
        .catch((e) => {
          this.log.error(
            { topic, error: e },
            "[POWER_MSK_CONSUMER_SUPERVISOR] subscribe failed",
          );
          throw e;
        });
    }

    this.healthy = true;
    this.ready = true;
    this.attempt = 0;

    // Start the runner ONCE; it returns immediately and keeps running in background.
    this.log.debug("[POWER_MSK_CONSUMER_SUPERVISOR] run starting...");
    if (eachBatch) {
      await consumer.run({
        ...runConfig,
        autoCommit: runConfig?.autoCommit ?? false,
        eachBatchAutoResolve: runConfig?.eachBatchAutoResolve ?? false,
        eachBatch: async (ctx) => {
          try {
            await eachBatch(ctx);
            await ctx.commitOffsetsIfNecessary?.();
          } catch (error) {
            this.log.error(
              {
                topic: ctx.batch.topic,
                partition: ctx.batch.partition,
                error,
              },
              "[POWER_MSK_CONSUMER_SUPERVISOR] eachBatch error; pausing 5s",
            );
            const resume = ctx.pause();
            setTimeout(() => {
              try {
                resume();
              } catch {}
            }, 5000);
            throw error;
          }
        },
      });
    } else if (eachMessage) {
      this.log.debug("[POWER_MSK_CONSUMER_SUPERVISOR] starting consumer");
      await consumer.run({
        ...runConfig,
        autoCommit: runConfig?.autoCommit ?? true,
        eachMessage: async (ctx) => {
          try {
            await eachMessage(ctx);
            const auto = this.opts.runConfig?.autoCommit;
            if (auto === false || auto == null) {
              await consumer.commitOffsets([
                {
                  topic: ctx.topic,
                  partition: ctx.partition,
                  offset: nextOffset(ctx.message.offset),
                },
              ]);
            }
          } catch (error) {
            this.log.error(
              { error },
              "[POWER_MSK_CONSUMER_SUPERVISOR] eachMessage error",
            );
            const maybeResume = (ctx as any).pause?.();
            if (typeof maybeResume === "function") {
              // type where pause() returns a resume()
              setTimeout(() => {
                try {
                  maybeResume();
                } catch {}
              }, 5000);
            } else {
              const sel = [
                { topic: ctx.topic, partitions: [ctx.partition] as number[] },
              ];
              setTimeout(() => {
                try {
                  consumer.resume(sel);
                } catch {}
              }, 5000);
            }
          }
        },
      });
    }

    // Hold the lifecycle alive until:
    //  - supervisor.stop() is called, or
    //  - a fatal crash was seen.
    while (!this.stopping && !fatalCrash) {
      await sleep(1000);
    }

    // Graceful teardown or hand off to outer loop
    try {
      if (this.stopping) {
        await consumer.stop().catch(() => {});
      }
    } finally {
      try {
        await consumer.disconnect();
      } catch (err) {
        this.log.error(
          {
            error: err,
          },
          "[POWER_MSK_CONSUMER_SUPERVISOR] Error while disconnecting",
        );
      }
      this.healthy = false;
      this.ready = false;
      this.log.debug("[POWER_MSK_CONSUMER_SUPERVISOR] disconnected");
    }

    return this.stopping ? "ok" : fatalCrash ? "fatal" : "ok";
  }
}
