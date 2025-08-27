import { expect } from "chai";
import Koa from "koa";
import request from "supertest";
import { EventEmitter } from "events";
import { getKafkaClientMw } from "../src/koa/producer-mw";

// ---- Fakes for KafkaJS Producer/Kafka ----
class FakeProducer extends EventEmitter {
  public events = { CONNECT: "connect", DISCONNECT: "disconnect" } as const;
  public connected = false;
  public sends: Array<{ topic: string; messages: any[] }> = [];
  public failNext = 0;

  async connect() {
    this.connected = true;
    setImmediate(() => this.emit(this.events.CONNECT, { payload: {} }));
  }
  async disconnect() {
    this.connected = false;
    setImmediate(() => this.emit(this.events.DISCONNECT, { payload: {} }));
  }
  async send({ topic, messages }: { topic: string; messages: any[] }) {
    if (this.failNext > 0) {
      this.failNext--;
      throw new Error("send failed (injected)");
    }
    this.sends.push({ topic, messages });
  }
  on() { return this; } // allow .on chaining, unused in tests
  transaction() {
    return { send: async (_: any) => {}, commit: async () => {}, abort: async () => {} };
  }
}

class FakeKafka {
  public lastProducer?: FakeProducer;
  producer(_: any) {
    const p = new FakeProducer();
    this.lastProducer = p;
    return p as any;
  }
}

// ---- Small helper ----
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ---- Tests ----
describe("Koa producer middleware", function () {
  this.timeout(15000);

  it("attaches ctx.kafkaClient with methods and starts producer lazily", async () => {
    const fk = new FakeKafka();
    const mw = getKafkaClientMw({ kafka: fk as any }); // capture instance

    const app = new Koa();
    app.use(mw);

    app.use(async (ctx) => {
      ctx.body = {
        healthy: ctx.kafkaClient!.isHealthy(),
        ready: ctx.kafkaClient!.isReady(),
      };
    });

    const srv = app.callback();
    await request(srv).get("/").expect(200);

    await sleep(20);
    expect(fk.lastProducer?.connected).to.equal(true);

    // ✅ stop background loop so Node can exit
    await mw.shutdown();
  });

  it("sendMessages publishes events (JSON) using the resilient producer", async () => {
    const fk = new FakeKafka();
    const mw = getKafkaClientMw({ kafka: fk as any });

    const app = new Koa();
    app.use(mw);
    app.use(async (ctx) => {
      await ctx.kafkaClient!.sendMessages([{ a: 1 }, { b: 2 }], "topicA");
      ctx.status = 204;
    });

    const srv = app.callback();
    await request(srv).post("/produce").send({}).expect(204);
    await sleep(25);

    const p = fk.lastProducer!;
    expect(p.sends.length).to.equal(1);

    await mw.shutdown();
  });

  it("exposes isHealthy/isReady (eventually true once producer connects)", async () => {
    const fk = new FakeKafka();
    const mw = getKafkaClientMw({ kafka: fk as any });

    const app = new Koa();
    app.use(mw);
    app.use(async (ctx) => {
      ctx.body = {
        healthy: ctx.kafkaClient!.isHealthy(),
        ready: ctx.kafkaClient!.isReady(),
      };
    });

    const srv = app.callback();
    await request(srv).get("/").expect(200);
    await sleep(30);
    const res2 = await request(srv).get("/").expect(200);
    expect(res2.body.ready).to.equal(true);

    await mw.shutdown();
  });
});