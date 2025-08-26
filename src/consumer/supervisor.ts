// @ts-nocheck
import {
  Consumer,
  ConsumerRunConfig,
  EachBatchPayload,
  EachMessagePayload,
  Kafka,
} from "kafkajs";
// NOTE: leave external wrappers disabled during stabilization
// import { attachConsumerResilience } from "./consumerResilience";

function sleep(ms: number) { return new Promise((r) => setTimeout(r, ms)); }
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

  runConfig?: Omit<Parameters<Consumer["run"]>[0], "eachBatch" | "eachMessage"> & ConsumerRunConfig;

  onCrashed?: () => Promise<void> | void; // optional
}

type IOpts = Required<
  Omit<SupervisorOpts, "runConfig" | "eachBatch" | "eachMessage" | "onCrashed">
> & {
  runConfig?: SupervisorOpts["runConfig"];
  eachBatch?: SupervisorOpts["eachBatch"];
  eachMessage?: SupervisorOpts["eachMessage"];
  onCrashed?: SupervisorOpts["onCrashed"];
};

export class ConsumerSupervisor {
  private stopping = false;
  private healthy = false;
  private ready = false;
  private lifecycleInFlight = false;
  private attempt = 0;
  private opts: IOpts;

  constructor(opts: SupervisorOpts) {
    if (!opts.eachBatch && !opts.eachMessage) throw new Error("Provide either eachBatch or eachMessage.");
    if (opts.eachBatch && opts.eachMessage) throw new Error("Provide only one of eachBatch or eachMessage.");
    this.opts = { ...(opts as IOpts), baseRetryMs: opts.baseRetryMs ?? 1000 };
  }

  isHealthy() { return this.healthy; }
  isReady() { return this.ready; }
  async stop() { this.stopping = true; }

  async startForever() {
    if (this.lifecycleInFlight) return;
    const base = this.opts.baseRetryMs;

    // eslint-disable-next-line no-constant-condition
    while (true) {
      if (this.stopping) break;
      try {
        this.lifecycleInFlight = true;
        const outcome = await this.oneLifecycle(); // returns "ok" | "fatal"
        this.lifecycleInFlight = false;

        if (this.stopping) break;
        if (outcome === "ok") {
          // normal stop — don’t restart
          break;
        } else {
          // fatal crash: backoff and recreate
          this.attempt++;
          const d = backoffMs(base, this.attempt);
          console.warn(`[sup] lifecycle ended after fatal crash; restarting in ${d}ms`);
          await sleep(d);
        }
      } catch (err) {
        this.lifecycleInFlight = false;
        this.attempt++;
        const d = backoffMs(base, this.attempt);
        console.error("[sup] lifecycle error; restarting", (err as any)?.message ?? err);
        await sleep(d);
      }
    }
    this.healthy = false;
    this.ready = false;
    console.info("[sup] stopped");
  }

  /**
   * Start one consumer instance.
   * Returns:
   *  - "ok"    → graceful stop requested
   *  - "fatal" → unrecoverable crash (restart lifecycle)
   */
  private async oneLifecycle(): Promise<"ok" | "fatal"> {
    const {
      kafka, groupId, topics, fromBeginning,
      sessionTimeout, rebalanceTimeout,
      runConfig, eachBatch, eachMessage, onCrashed,
    } = this.opts;

    if (!topics || topics.length === 0) throw new Error("ConsumerSupervisor: 'topics' must be a non-empty array");

    const consumer = kafka.consumer({
      groupId,
      sessionTimeout: sessionTimeout ?? 30_000,
      rebalanceTimeout: rebalanceTimeout ?? 60_000,
    });
    console.log("[sup] consumer created");

    let fatalCrash = false;

    const crashedHook = onCrashed ?? (() => { /* no-op */ });

    // ---- events (log-only) ----
    consumer.on(consumer.events.CONNECT, () => console.log("[sup] CONNECT"));
    consumer.on(consumer.events.DISCONNECT, (e) => console.warn("[sup] DISCONNECT", e?.payload));
    consumer.on(consumer.events.STOP, () => console.warn("[sup] STOP"));
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
      console.log("[sup] GROUP_JOIN", {
        generationId: (e?.payload as any)?.generationId,
        isLeader: (e?.payload as any)?.isLeader,
        assigned: assignments.map(a => `${a.topic}[${a.partitions.length}]`),
      });
    });
    consumer.on(consumer.events.CRASH, (e) => {
      const { error, restart } = (e?.payload ?? {}) as any;
      console.error("[sup] CRASH", { restart, name: error?.name, msg: error?.message });
      try { crashedHook(); } catch {}
      // KafkaJS will auto-restart if restart===true. If false, we mark fatal and let the outer loop recreate.
      if (restart === false) fatalCrash = true;
    });

    // Connect & subscribe once
    console.log("[sup] connect...");
    await consumer.connect().catch((e) => { console.error("[sup] connect failed", e); throw e; });
    for (const topic of topics) {
      console.log("[sup] subscribe", { topic, fromBeginning: !!fromBeginning });
      await consumer.subscribe({ topic, fromBeginning: !!fromBeginning })
        .catch((e) => { console.error("[sup] subscribe failed", { topic }, e); throw e; });
    }

    this.healthy = true;
    this.ready = true;
    this.attempt = 0;

    // Start the runner ONCE; it returns immediately and keeps running in background.
    console.log("[sup] run starting...");
    if (eachBatch) {
      await consumer.run({
        ...runConfig,
        autoCommit: runConfig?.autoCommit ?? false,
        eachBatchAutoResolve: runConfig?.eachBatchAutoResolve ?? false,
        heartbeatInterval: runConfig?.heartbeatInterval ?? 3000,
        eachBatch: async (ctx) => {
          try {
            await eachBatch(ctx);
            await ctx.commitOffsetsIfNecessary?.();
          } catch (err) {
            const sel = [{ topic: ctx.batch.topic, partitions: [ctx.batch.partition] as number[] }];
            console.error("[sup] eachBatch error; pausing 5s", { topic: ctx.batch.topic, partition: ctx.batch.partition, err });
            ctx.pause(sel); // selector REQUIRED in eachBatch
            setTimeout(() => { try { consumer.resume(sel); } catch {} }, 5000);
            throw err;
          }
        },
      });
    } else if (eachMessage) {
      console.log("[sup] starting consumer");
      await consumer.run({
        ...runConfig,
        autoCommit: runConfig?.autoCommit ?? false,
        heartbeatInterval: runConfig?.heartbeatInterval ?? 3000,
        eachMessage: async (ctx) => {
          try {
            await eachMessage(ctx);
            await ctx.commitOffsetsIfNecessary?.();
          } catch (err) {
            console.error("[sup] eachMessage error", err);
            ctx.pause(); // pauses current partition
            const sel = [{ topic: ctx.topic, partitions: [ctx.partition] as number[] }];
            setTimeout(() => { try { consumer.resume(sel); } catch {} }, 5000);
            throw err;
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
      try { await consumer.disconnect(); } catch {}
      this.healthy = false;
      this.ready = false;
      console.log("[sup] disconnected");
    }

    return this.stopping ? "ok" : (fatalCrash ? "fatal" : "ok");
  }
}
