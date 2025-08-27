# power-msk v2

**Resilience-first KafkaJS helpers** for production-grade apps on [KafkaJS](https://kafka.js.org/).

👉 Focused on **robust consumers & producers** + simple **Koa middleware**.

👉 You bring your own Kafka config (brokers, SSL, SASL, etc).

---

## ✨ Features

### Resilient Consumer

* Auto-recreate on fatal errors (`STOP` / `CRASH` with `restart=false`)
* Partition-level isolation via pause/resume
* Full-jitter exponential backoff
* Health & readiness probes

### Resilient Producer

* Auto-reconnect & recreate on send errors
* Batching (`lingerMs`, `maxBatchSize`) for throughput
* Backpressure (`maxQueueBytes`) to prevent OOM
* Idempotence & transactions supported
* Health & readiness probes

### Koa Middleware

* Exposes a shared resilient producer on `ctx.kafkaClient`
* `sendMessages()`, `isHealthy()`, `isReady()` available in requests
* Graceful shutdown hook (`mw.shutdown()`) for clean exits

---

## 📦 Install

```bash
npm install @awsmag/power-msk kafkajs
# or
yarn add @awsmag/power-msk kafkajs
```

---

## 🚀 Quickstart

### 1) Consumer (eachMessage)

```ts
import { Kafka } from "kafkajs";
import { ConsumerSupervisor } from "@awsmag/power-msk";

const kafka = new Kafka({
  clientId: "orders-app",
  brokers: ["b1:9092","b2:9092","b3:9092"],
  ssl: true,
});

const sup = new ConsumerSupervisor({
  kafka,
  groupId: "orders-g1",
  topics: ["orders"],
  eachMessage: async ({ topic, partition, message }) => {
    const payload = JSON.parse(message.value!.toString());
    console.log("Received", payload);
  },
});

await sup.startForever();
```

### 2) Consumer (eachBatch)

```ts
const sup = new ConsumerSupervisor({
  kafka,
  groupId: "orders-g1",
  topics: ["orders"],
  eachBatch: async ({ batch, commitOffsetsIfNecessary }) => {
    for (const m of batch.messages) {
      console.log("Received", m.offset, m.value?.toString());
    }
    await commitOffsetsIfNecessary();
  },
  runConfig: { autoCommit: false, eachBatchAutoResolve: false },
});

await sup.startForever();
```

### 3) Producer

```ts
import { ResilientProducer } from "@awsmag/power-msk";

const producer = new ResilientProducer({
  kafka,
  idempotent: true,
  acks: -1,
  lingerMs: 10,
  maxBatchSize: 500,
  maxQueueBytes: 5 * 1024 * 1024,
});

await producer.start();

await producer.sendOne("orders", {
  key: Buffer.from("o:123"),
  value: Buffer.from(JSON.stringify({ id: 123, status: "created" })),
});
```

### 4) Koa Middleware

```ts
import Koa from "koa";
import Router from "@koa/router";
import { getKafkaClientMw } from "@awsmag/power-msk";

const app = new Koa();
const router = new Router();

const kafkaMw = getKafkaClientMw({
  clientId: "my-app",
  brokers: ["b1:9092", "b2:9092"],
  ssl: true,
});

app.use(kafkaMw);

router.post("/broadcast", async (ctx) => {
  const events = [{ id: 1, msg: "hello" }];
  await ctx.kafkaClient!.sendMessages(events, "my-topic");
  ctx.status = 202;
});

router.get("/healthz", (ctx) => { ctx.status = ctx.kafkaClient!.isHealthy() ? 200 : 500; });
router.get("/readyz",  (ctx) => { ctx.status = ctx.kafkaClient!.isReady() ? 200 : 503; });

app.use(router.routes()).use(router.allowedMethods());
app.listen(3000);

// optional: stop producer cleanly in tests or shutdown scripts
// await kafkaMw.shutdown();
```

---

## 🛠 API

### ConsumerSupervisor

* `startForever()`: run until stopped
* `stop()`: graceful stop
* `isHealthy()` / `isReady()`

### ResilientProducer

* `start()` / `stop()`
* `send(topic, messages)` / `sendOne(topic, message)`
* `withTransaction(fn)`
* `isHealthy()` / `isReady()`

### Koa Middleware

* `getKafkaClientMw(opts)`

  * Attaches `ctx.kafkaClient` with:

    * `sendMessages(events[], topic)`
    * `isHealthy()`
    * `isReady()`
  * Provides `.shutdown()` for clean exits (esp. in tests)

---

## 📚 Detailed Scenarios

Want to understand how the **ConsumerSupervisor** and **ResilientProducer** behave in real-world cases (errors, crashes, rebalances, backpressure)?  

👉 See [SCENARIOS.md](./SCENARIOS.md) for sequence diagrams and lifecycle walkthroughs.

---

## 🔄 Migration (v1 → v2)

* **IAM/MSK helpers** dropped. Bring your own `ssl/sasl`.
* **Producer/Consumer APIs** are now resilience-focused.
* **Koa middleware** switched from per-request producer → shared resilient producer with auto-recreate.

---

Maintained by [AWSMAG](https://awsmag.com) C/O [S25Digital](https://s25.digital).

---
