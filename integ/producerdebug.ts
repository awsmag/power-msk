// @ts-nocheck
import { Kafka, logLevel } from "kafkajs";
import { ResilientProducer } from "../src/producer/ResilientProducer";

// ---------- Env & defaults ----------
const BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092")
  .split(",")
  .map(s => s.trim())
  .filter(Boolean);

const TOPIC = process.env.TOPIC || "test";
const COUNT = parseInt(process.env.COUNT || "10", 10);
const LINGER_MS = parseInt(process.env.LINGER_MS || "5", 10);
const CREATE_TOPIC = (process.env.CREATE_TOPIC || "true").toLowerCase() !== "false";

async function main() {
  console.log(`[IT] brokers=${BROKERS.join(",")} topic=${TOPIC} count=${COUNT}`);

  const kafka = new Kafka({
    clientId: "power-msk-it-producer",
    brokers: BROKERS,
    logLevel: logLevel.INFO,
  });

  // ---------- (Optional) ensure topic exists ----------
  if (CREATE_TOPIC) {
    const admin = kafka.admin();
    await admin.connect();
    try {
      const topics = await admin.listTopics();
      if (!topics.includes(TOPIC)) {
        console.log(`[IT] creating topic ${TOPIC}`);
        await admin.createTopics({
          topics: [{ topic: TOPIC, numPartitions: 1, replicationFactor: 1 }],
          waitForLeaders: true,
        });
      } else {
        console.log(`[IT] topic ${TOPIC} already exists`);
      }
    } finally {
      await admin.disconnect();
    }
  }

  // ---------- Start resilient producer ----------
  const prod = new ResilientProducer({
    kafka,
    lingerMs: LINGER_MS,
    baseRetryMs: 50,
    idempotent: true,
    allowAutoTopicCreation: false,
    // recreate on any send error by default; keep default or tune:
    // recreateOnError: (err) => true,
  });

  const run = prod.start();

  // Wait a tick so start() connects
  await new Promise(r => setTimeout(r, 50));

  if (!prod.isReady()) {
    // not strictly necessary, just a guardrail
    console.log("[IT] waiting for producer to become ready...");
    let attempts = 0;
    while (!prod.isReady() && attempts < 50) {
      await new Promise(r => setTimeout(r, 20));
      attempts++;
    }
  }

  if (!prod.isReady()) {
    throw new Error("[IT] producer not ready after wait");
  }

  console.log("[IT] producer ready → sending messages...");

  // ---------- Produce messages ----------
  const startTs = Date.now();
  for (let i = 0; i < COUNT; i++) {
    const key = Buffer.from(String(i));
    const value = Buffer.from(
      JSON.stringify({
        i,
        ts: new Date().toISOString(),
        payload: `hello-${i}`,
      })
    );

    await prod.send(TOPIC, [{ key, value }]); // queued; flushed by flushLoop()
  }

  // sleep slightly > linger to ensure flush loop runs at least once
  await new Promise(r => setTimeout(r, LINGER_MS + 20));

  const ms = Date.now() - startTs;
  console.log(`[IT] sent ${COUNT} messages in ~${ms}ms`);

  // ---------- Cleanup ----------
  await prod.stop();
  await run; // wait for start() loop exit
  console.log("[IT] producer stopped, done.");
}

main().catch((err) => {
  console.error("[IT] FAILED", err);
  process.exitCode = 1;
});
