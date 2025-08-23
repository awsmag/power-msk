# power-msk v2

**Resilience-first KafkaJS helpers** for building production-grade apps on top of [KafkaJS](https://kafka.js.org/).

This package is **focused only on resilient producers and consumers**.  
You bring your own Kafka configuration (brokers, SSL, SASL, etc).

---

## ✨ Features

- **Resilient Consumer**
  - Auto-recreate on `STOP` / non-retriable `CRASH`
  - Partition-level isolation with pause/resume
  - Full-jitter exponential backoff between lifecycles
  - Health & readiness flags for probes

- **Resilient Producer**
  - Auto-reconnect & recreate on send errors
  - Batching (`lingerMs`, `maxBatchSize`) for throughput
  - Backpressure (`maxQueueBytes`) to prevent OOM
  - Idempotence & transaction support
  - Health & readiness flags

- **Koa Middleware**
  - Simple middleware to expose a resilient producer on `ctx.kafkaClient.sendMessages()`
  - Health helpers available on the same context

---

## 📦 Install

```bash
npm install @awsmag/power-msk kafkajs
# or
yarn add @awsmag/power-msk kafkajs
```

---

## 🚀 Quickstart

### 1) Resilient Consumer (eachMessage)

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
  eachMessage: async ({ topic, partition, message, resolveOffset, heartbeat }) => {
    const payload = JSON.parse(message.value!.toString());
    console.log("Received", payload);

    resolveOffset(message.offset);
    if ((Number(message.offset) & 0x3F) === 0) await heartbeat();
  },
});

await sup.startForever();
```

### 2) Resilient Consumer (eachBatch)

```ts
import { ConsumerSupervisor } from "@awsmag/power-msk";

const sup = new ConsumerSupervisor({
  kafka,
  groupId: "orders-g1",
  topics: ["orders"],
  eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {
    for (const m of batch.messages) {
      console.log("Received", m.offset, m.value?.toString());
      resolveOffset(m.offset);
      if ((Number(m.offset) & 0x3F) === 0) await heartbeat();
    }
    await commitOffsetsIfNecessary();
  },
  runConfig: { autoCommit: false, eachBatchAutoResolve: false },
});

await sup.startForever();
```

### 3) Resilient Producer

```ts
import { Kafka } from "kafkajs";
import { ResilientProducer } from "@awsmag/power-msk";

const kafka = new Kafka({ clientId: "orders-app", brokers: ["b1:9092","b2:9092"] });

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

### 4) Koa Middleware (Producer)

```ts
import Koa from "koa";
import Router from "@koa/router";
import { getKafkaClientMw } from "@awsmag/power-msk";

const app = new Koa();
const router = new Router();

app.use(getKafkaClientMw({
  clientId: "my-app",
  brokers: ["b1:9092", "b2:9092"],
  ssl: true,
}));

router.post("/broadcast", async (ctx) => {
  const events = [{ id: 1, msg: "hello" }];
  await ctx.kafkaClient!.sendMessages(events, "my-topic");
  ctx.status = 202;
});

router.get("/healthz", (ctx) => { ctx.status = ctx.kafkaClient!.isHealthy() ? 200 : 500; });
router.get("/readyz",  (ctx) => { ctx.status = ctx.kafkaClient!.isReady() ? 200 : 503; });

app.use(router.routes()).use(router.allowedMethods());
app.listen(3000);
```

---

## 🛠 API Overview

### ConsumerSupervisor
- `new ConsumerSupervisor(opts)`  
- `startForever(): Promise<void>`  
- `stop(): Promise<void>`  
- `isHealthy(): boolean`  
- `isReady(): boolean`

### ResilientProducer
- `new ResilientProducer(opts)`  
- `start(): Promise<void>` / `stop(): Promise<void>`  
- `send(topic, messages)` / `sendOne(topic, message)`  
- `withTransaction(fn)`  
- `isHealthy(): boolean` / `isReady(): boolean`

### createProbeServer
- `createProbeServer({ port, getHealthy, getReady, extra })`  

### getKafkaClientMw
- Middleware attaches `ctx.kafkaClient` with:
  - `sendMessages(events[], topic)`
  - `isHealthy()`
  - `isReady()`

---

## 💡 Best Practices

- **Idempotency**: make consumers idempotent; safe to reprocess.  
- **Manual commits**: use `autoCommit: false`. Commit offsets after success.  
- **Heartbeats**: call in long loops.  
- **Backpressure**: handle `queue full` errors in producer.  
- **Probes**: wire health/readiness into infra.  

---

## 🔄 Migration Guide (v1 → v2)

### What changed?
- v1 shipped with **IAM/MSK auth helpers** (auto token refresh, STS, etc).  
- v2 removes IAM completely — you configure `kafkajs` with your own `ssl` / `sasl` / `oauthBearerProvider`.  
- v2 focuses only on **resilience**: `ResilientProducer`, `ConsumerSupervisor`, probes, and Koa middleware.

### Producer migration

**v1**:
```ts
import { getProducer } from "@awsmag/power-msk";
const producer = await getProducer();
await producer.send({ topic: "orders", messages: [{ value: "event" }] });
```

**v2**:
```ts
import { Kafka } from "kafkajs";
import { ResilientProducer } from "@awsmag/power-msk";

const kafka = new Kafka({ clientId: "app", brokers: ["..."], ssl: true, sasl: { mechanism: "plain", username: "...", password: "..." } });
const producer = new ResilientProducer({ kafka });
await producer.start();
await producer.sendOne("orders", { value: Buffer.from("event") });
```

### Consumer migration

**v1**:
```ts
import { getConsumer } from "@awsmag/power-msk";
const consumer = await getConsumer({ groupId: "g1", topic: "orders" });
await consumer.run({ eachMessage: async ({ message }) => console.log(message.value?.toString()) });
```

**v2**:
```ts
import { Kafka } from "kafkajs";
import { ConsumerSupervisor } from "@awsmag/power-msk";

const kafka = new Kafka({ clientId: "app", brokers: ["..."], ssl: true });
const sup = new ConsumerSupervisor({
  kafka,
  groupId: "g1",
  topics: ["orders"],
  eachMessage: async ({ message }) => console.log(message.value?.toString()),
});
await sup.startForever();
```

### Koa middleware migration

**v1**:  
Connected/disconnected producer per request.  

**v2**:  
Shared resilient producer, auto-recreates on failure.  

```ts
import { getKafkaClientMw } from "@awsmag/power-msk";

app.use(getKafkaClientMw({ clientId: "my-app", brokers: ["b1:9092"] }));

router.post("/broadcast", async (ctx) => {
  await ctx.kafkaClient!.sendMessages([{ msg: "hello" }], "orders");
});
```

The package is developed and maintained by [AWSMAG](https://awsmag.com) C/O [S25Digital](https://s25.digital).
