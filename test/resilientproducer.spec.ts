import { expect } from "chai";
import { EventEmitter } from "events";
import { ResilientProducer } from "../src/producer/resilientProducer";

// --------------------------
// Small helpers (real timers)
// --------------------------
async function waitUntil(
  cond: () => boolean,
  stepMs = 20,
  timeoutMs = 10_000
) {
  const start = Date.now();
  while (!cond()) {
    await new Promise((r) => setTimeout(r, stepMs));
    if (Date.now() - start > timeoutMs) throw new Error("waitUntil timeout");
  }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// --------------------------
// Fakes (KafkaJS producer + Kafka)
// --------------------------
class FakeProducer extends EventEmitter {
  public events = { CONNECT: "connect", DISCONNECT: "disconnect" } as const;
  public connected = false;
  public sends: Array<{ topic: string; messages: any[]; acks?: number; compression?: number }> = [];
  public failNext = 0;

  async connect() {
    this.connected = true;
    // mimic kafkajs: emit CONNECT on next tick
    setImmediate(() => this.emit(this.events.CONNECT, { payload: {} }));
  }
  async disconnect() {
    this.connected = false;
    setImmediate(() => this.emit(this.events.DISCONNECT, { payload: {} }));
  }

  async send({
    topic,
    messages,
    acks,
    compression,
  }: {
    topic: string;
    messages: any[];
    acks?: number;
    compression?: number;
  }) {
    if (this.failNext > 0) {
      this.failNext--;
      throw new Error("send failed (injected)");
    }
    this.sends.push({ topic, messages, acks, compression });
  }

  transaction() {
    // simple tx object we can spy via monkey-patch in tests
    const tx = {
      sent: [] as any[],
      send: async (args: any) => {
        tx.sent.push(args);
      },
      commit: async () => {},
      abort: async () => {},
    };
    return tx;
  }
}

class FakeKafka {
  public lastProducer?: FakeProducer;
  producer(_cfg: any) {
    const p = new FakeProducer();
    this.lastProducer = p;
    return p as any;
  }
}

// --------------------------
// Tests (no fake timers)
// --------------------------
describe("ResilientProducer (integration-free)", function () {
  // use real timers, just increase timeout a bit
  this.timeout(15_000);

  it("start() flips ready/healthy and stop() shuts down", async () => {
    const fk = new FakeKafka();
    const rp = new ResilientProducer({ kafka: fk as any, lingerMs: 5 });

    const loop = rp.start();
    await waitUntil(() => rp.isReady());

    expect(rp.isReady()).to.equal(true);
    expect(rp.isHealthy()).to.equal(true);
    expect(fk.lastProducer?.connected).to.equal(true);

    await rp.stop();
    await loop;
    await sleep(10); // let DISCONNECT event flush

    expect(rp.isReady()).to.equal(false);
    expect(rp.isHealthy()).to.equal(false);
    expect(fk.lastProducer?.connected).to.equal(false);
  });

  it("send() resolves and batches by topic", async () => {
    const fk = new FakeKafka();
    const rp = new ResilientProducer({ kafka: fk as any, lingerMs: 5 });

    const loop = rp.start();
    await waitUntil(() => rp.isReady());
    const p = fk.lastProducer!;

    const s1 = rp.send("t1", [{ value: Buffer.from("a") }]);
    const s2 = rp.send("t1", [{ value: Buffer.from("b") }]);
    const s3 = rp.send("t2", [{ value: Buffer.from("c") }]);

    await sleep(15); // > lingerMs so flushLoop fires at least once
    await Promise.all([s1, s2, s3]);

    // 2 topics => 2 sends; t1 entries coalesced
    expect(p.sends.length).to.equal(2);
    const t1 = p.sends.find((x) => x.topic === "t1")!;
    const t2 = p.sends.find((x) => x.topic === "t2")!;
    expect(t1.messages.map((m) => m.value.toString())).to.deep.equal(["a", "b"]);
    expect(t2.messages.map((m) => m.value.toString())).to.deep.equal(["c"]);

    await rp.stop();
    await loop;
  });

  it("retries & recreates on send error when recreateOnError returns true", async () => {
    const fk = new FakeKafka();
    const rp = new ResilientProducer({
      kafka: fk as any,
      baseRetryMs: 10, // keep fast
      lingerMs: 1,
      recreateOnError: () => true,
    });

    const loop = rp.start();
    await waitUntil(() => rp.isReady());

    // inject one failure on *current* underlying producer
    fk.lastProducer!.failNext = 1;

    const sendP = rp.send("tx", [{ value: Buffer.from("x") }]);

    // allow: first failure → backoff → (optional) disconnect/recreate → retry → success
    await sleep(300);
    await sendP;

    // Ensure at least one successful send happened on the (possibly recreated) producer
    expect(fk.lastProducer!.sends.some((s) => s.topic === "tx")).to.equal(true);

    await rp.stop();
    await loop;
  });

  it("enforces maxQueueBytes backpressure", async () => {
    const fk = new FakeKafka();
    // tiny queue to force backpressure quickly
    const rp = new ResilientProducer({
      kafka: fk as any,
      lingerMs: 50,
      maxQueueBytes: 4, // ~4 bytes
    });

    const loop = rp.start();
    await waitUntil(() => rp.isReady());

    // first small message should enqueue fine
    await rp.send("t", [{ value: Buffer.from("a") }]);

    // second big message should exceed queue
    let err: any;
    try {
      await rp.send("t", [{ value: Buffer.from("bbbbbbbb") }]);
    } catch (e) {
      err = e;
    }
    expect(String(err?.message || err)).to.match(/queue is full/i);

    // let first message flush to avoid pending promises when stopping
    await sleep(60);

    await rp.stop();
    await loop;
  });

  it("withTransaction() commits on success and aborts on failure", async () => {
    const fk = new FakeKafka();
    const rp = new ResilientProducer({
      kafka: fk as any,
      transactionalId: "tx-1",
      lingerMs: 1,
    });

    const loop = rp.start();
    await waitUntil(() => rp.isReady());

    // success path: observe commit was called
    let committed = false;
    const out = await rp.withTransaction(async (txn: any) => {
      await txn.send({ topic: "t", messages: [{ value: Buffer.from("ok") }] });
      const origCommit = txn.commit;
      txn.commit = async () => {
        committed = true;
        return origCommit();
      };
      return "done";
    });
    expect(out).to.equal("done");
    await sleep(5);
    expect(committed).to.equal(true);

    // failure path: observe abort was called
    let aborted = false;
    try {
      await rp.withTransaction(async (txn: any) => {
        const origAbort = txn.abort;
        txn.abort = async () => {
          aborted = true;
          return origAbort();
        };
        await txn.send({ topic: "t", messages: [{ value: Buffer.from("nope") }] });
        throw new Error("boom");
      });
      expect.fail("should have thrown");
    } catch {
      // expected
    }
    await sleep(5);
    expect(aborted).to.equal(true);

    await rp.stop();
    await loop;
  });
});
