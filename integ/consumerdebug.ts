// @ts-nocheck

import { Kafka, logLevel } from "kafkajs";
import { ConsumerSupervisor } from "../src";
import * as process from "node:process";

function env(name: string, def?: string) { return process.env[name] ?? def ?? ""; }
function flag(name: string) { return process.argv.includes(`--${name}`); }
function arg(name: string, def?: string) {
  const i = process.argv.findIndex(a => a === `--${name}` || a.startsWith(`--${name}=`));
  if (i < 0) return def;
  const a = process.argv[i];
  if (a.includes("=")) return a.split("=")[1];
  const n = process.argv[i + 1];
  return !n || n.startsWith("--") ? def : n;
}
function pretty(o: any) { try { return JSON.stringify(o, null, 2); } catch { return String(o); } }
function die(msg: string): never { console.error(msg); process.exit(1); }

const topic = arg("topic") || "test-topic";
const groupId = arg("group") || "pmsk-g1";
const fromBeginning = flag("fromBeginning");
const seekEarliest = flag("seekEarliest");
const printLag = flag("printLag");
const resetToEarliest = flag("resetToEarliest");
const debug = flag("debug");

const brokers = env("BROKERS", "localhost:9092").split(",").map(s => s.trim()).filter(Boolean);
const clientId = env("CLIENT_ID", "power-msk-consumer-debug");

(async function main() {
  if (!topic) die("Provide --topic");
  const kafka = new Kafka({
    clientId,
    brokers,
    ssl: false, // set true if your local cluster uses TLS
    // sasl: { mechanism: "scram-sha-512", username: "...", password: "..." },
    logLevel: debug ? logLevel.DEBUG : logLevel.INFO,
  });

  // Optional: reset group offsets to earliest before starting
  if (resetToEarliest) {
    const admin = kafka.admin();
    await admin.connect();
    console.log(`[RESET] group=${groupId} topic=${topic} -> earliest`);
    await admin.resetOffsets({ groupId, topic, earliest: true });
    await admin.disconnect();
    console.log("[RESET] done");
  }

  // Create supervisor
  const sup = new ConsumerSupervisor({
    kafka,
    groupId,
    topics: [topic],
    fromBeginning, // subscription hint
    eachMessage: async ({ topic, partition, message }) => {
      // verbose record log
      const k = message.key?.toString();
      const v = message.value?.toString();
      console.log(`[RECV] ${topic}/${partition} offset=${message.offset} key=${k} value=${v}`);
    },
    runConfig: {
      autoCommit: true,
      // partitionsConsumedConcurrently: 1, // uncomment to simplify concurrency during debug
    },
  });

  // Optional: periodic lag printer
  let lagTimer: NodeJS.Timeout | undefined;
  if (printLag) {
    const admin = kafka.admin();
    await admin.connect();
    const tick = async () => {
      try {
        const topicOffsets = await admin.fetchTopicOffsets(topic);
        const groupOffsets = await admin.fetchOffsets({ groupId, topic });
        const lag = topicOffsets.map(t => {
          const g = groupOffsets.find(x => x.partition === t.partition);
          const committed = g?.offset ? Number(g.offset) : 0;
          const latest = Number(t.high);
          return { partition: t.partition, lag: Math.max(0, latest - committed), latest, committed };
        });
        console.log(`[LAG] ${new Date().toISOString()} ${pretty(lag)}`);
      } catch (e) {
        console.warn("[LAG] error", (e as any)?.message || e);
      }
    };
    await tick();
    lagTimer = setInterval(tick, 10_000);
    // clean up on exit
    const stopLag = async () => { if (lagTimer) clearInterval(lagTimer); try { await admin.disconnect(); } catch {} };
    process.on("SIGINT", stopLag);
    process.on("SIGTERM", stopLag);
  }

  // Start the supervisor
  console.log(`[START] brokers=${brokers.join(",")} topic=${topic} group=${groupId} fromBeginning=${fromBeginning} seekEarliest=${seekEarliest}`);
  await sup.startForever();

  // graceful shutdown
  const stop = async () => {
      await sup.stop(); 
    process.exit(0);
  };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
})().catch((e) => {
  console.error("[FATAL]", e);
  process.exit(1);
});
