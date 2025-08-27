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
  this.timeout(15_000);

  it("attaches ctx.kafkaClient with methods and starts producer lazily", async () => {
    const fk = new FakeKafka();

    const app = new Koa();
    app.use(getKafkaClientMw({ kafka: fk as any }));

    app.use(async (ctx) => {
      expect(ctx.kafkaClient).to.exist;
      expect(typeof ctx.kafkaClient!.sendMessages).to.equal("function");
      expect(typeof ctx.kafkaClient!.isHealthy).to.equal("function");
      expect(typeof ctx.kafkaClient!.isReady).to.equal("function");
      ctx.body = {
        healthy: ctx.kafkaClient!.isHealthy(),
        ready: ctx.kafkaClient!.isReady(),
      };
    });

    const srv = app.callback();
    const res = await request(srv).get("/").expect(200);
    expect(res.body).to.have.keys(["healthy", "ready"]);

    // producer should have been created and connected shortly after
    await sleep(20);
    expect(fk.lastProducer?.connected).to.equal(true);
  });

  it("sendMessages publishes events (JSON) using the resilient producer", async () => {
    const fk = new FakeKafka();
    const app = new Koa();
    app.use(getKafkaClientMw({ kafka: fk as any }));

    app.use(async (ctx) => {
      await ctx.kafkaClient!.sendMessages([{ a: 1 }, { b: 2 }], "topicA");
      ctx.status = 204;
    });

    const srv = app.callback();
    await request(srv).post("/produce").send({}).expect(204);

    // allow flush loop to run (linger ~10ms in middleware)
    await sleep(25);

    const p = fk.lastProducer!;
    expect(p.sends.length).to.equal(1);
    expect(p.sends[0].topic).to.equal("topicA");
    const payloads = p.sends[0].messages.map((m) =>
      Buffer.isBuffer(m.value) ? m.value.toString() : String(m.value)
    );
    expect(payloads).to.deep.equal([JSON.stringify({ a: 1 }), JSON.stringify({ b: 2 })]);
  });

  it("sendMessages throws on empty array", async () => {
    const fk = new FakeKafka();
    const app = new Koa();
    app.use(getKafkaClientMw({ kafka: fk as any }));

    app.use(async (ctx) => {
      try {
        await ctx.kafkaClient!.sendMessages([], "x");
        ctx.status = 500;
      } catch (e) {
        ctx.status = 400;
        ctx.body = { err: String((e as Error).message) };
      }
    });

    const srv = app.callback();
    const res = await request(srv).post("/bad").send({}).expect(400);
    expect(String(res.body.err || "")).to.match(/Events are required/i);
  });

  it("exposes isHealthy/isReady (eventually true once producer connects)", async () => {
    const fk = new FakeKafka();
    const app = new Koa();
    app.use(getKafkaClientMw({ kafka: fk as any }));

    app.use(async (ctx) => {
      ctx.body = {
        healthy: ctx.kafkaClient!.isHealthy(),
        ready: ctx.kafkaClient!.isReady(),
      };
    });

    const srv = app.callback();
    // first hit: may be false/false right away
    const first = await request(srv).get("/").expect(200);
    expect(first.body).to.have.keys(["healthy", "ready"]);

    // wait for connect event and check again
    await sleep(30);
    const second = await request(srv).get("/").expect(200);
    expect(second.body.healthy).to.be.a("boolean");
    expect(second.body.ready).to.be.a("boolean");
    // at least ready should be true after start+connect
    expect(second.body.ready).to.equal(true);
  });
});
