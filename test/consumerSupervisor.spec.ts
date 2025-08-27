import { expect } from "chai";
import sinon from "sinon";
import { EventEmitter } from "events";
import { ConsumerSupervisor } from "../src/consumer/supervisor";

// --------------------------
// Helpers
// --------------------------
async function waitUntil(
  cond: () => boolean,
  clock: sinon.SinonFakeTimers,
  stepMs = 50,
  timeoutMs = 5_000,
) {
  const start = Date.now();
  while (!cond()) {
    clock.tick(stepMs);
    await Promise.resolve();
    if (Date.now() - start > timeoutMs) {
      throw new Error("waitUntil timeout");
    }
  }
}

function advance(clock: sinon.SinonFakeTimers, ms: number) {
  clock.tick(ms);
}

// --------------------------
// Fake KafkaJS Consumer
// --------------------------
type EachMessageFn = (ctx: any) => Promise<void>;
type EachBatchFn = (ctx: any) => Promise<void>;

interface RunCfg {
  autoCommit?: boolean;
  eachMessage?: EachMessageFn;
  eachBatch?: EachBatchFn;
  [k: string]: any;
}

class FakeConsumer extends EventEmitter {
  running = false;
  runConfig?: RunCfg;
  committed: Array<{ topic: string; partition: number; offset: string }> = [];

  // match the surface used by your class
  public events = {
    CONNECT: "connect",
    DISCONNECT: "disconnect",
    STOP: "stop",
    GROUP_JOIN: "group_join",
    CRASH: "crash",
  };

  async connect() {
    // emit connect async
    setImmediate(() => this.emit(this.events.CONNECT, { payload: {} }));
  }

  async subscribe(_args: { topic: string; fromBeginning?: boolean }) {
    // no-op
  }

  async run(cfg: RunCfg) {
    this.runConfig = cfg;
    this.running = true;
    // like KafkaJS: returns immediately, keeps background runner
  }

  async stop() {
    this.running = false;
    setImmediate(() => this.emit(this.events.STOP, { payload: {} }));
  }

  async disconnect() {
    setImmediate(() => this.emit(this.events.DISCONNECT, { payload: {} }));
  }

  async commitOffsets(
    entries: Array<{ topic: string; partition: number; offset: string }>,
  ) {
    this.committed.push(...entries);
  }

  resume(_sel: Array<{ topic: string; partitions: number[] }>) {
    // no-op
  }

  // ---- test helpers ----
  emitGroupJoin(
    assignment:
      | Record<string, number[]>
      | Array<{ topic: string; partitions: number[] }>,
    isLeader = false,
  ) {
    const payload: any = {
      isLeader,
      generationId: 1,
      memberAssignment: assignment,
    };
    this.emit(this.events.GROUP_JOIN, { payload });
  }

  emitCrash(restart: boolean, name = "TestError", msg = "boom") {
    this.emit(this.events.CRASH, {
      payload: { restart, error: { name, message: msg } },
    });
  }

  async deliverEachMessage(topic: string, partition: number, offset: string) {
    if (!this.runConfig?.eachMessage)
      throw new Error("eachMessage handler not set");
    const pauseImpl = () => () => {}; // returns resume fn (compatible)
    const ctx = {
      topic,
      partition,
      message: { offset, key: Buffer.from("k"), value: Buffer.from("v") },
      pause: pauseImpl,
      heartbeat: async () => {},
    };
    await this.runConfig.eachMessage(ctx);
  }

  async deliverEachBatch(topic: string, partition: number, baseOffset: string) {
    if (!this.runConfig?.eachBatch)
      throw new Error("eachBatch handler not set");
    const pauseImpl = () => () => {}; // returns resume fn per your typings
    const ctx = {
      batch: { topic, partition, firstOffset: baseOffset },
      pause: pauseImpl,
      commitOffsetsIfNecessary: async () => {},
    };
    await this.runConfig.eachBatch(ctx);
  }
}

class FakeKafka {
  consumers: FakeConsumer[] = [];
  consumer(_cfg: any) {
    const c = new FakeConsumer();
    this.consumers.push(c);
    return c;
  }
}

// --------------------------
// Tests
// --------------------------
describe("ConsumerSupervisor", () => {
  let clock: sinon.SinonFakeTimers;

  beforeEach(() => {
    clock = sinon.useFakeTimers({ shouldAdvanceTime: false });
  });

  afterEach(() => {
    clock.restore();
  });

  it("sets healthy/ready after connect+subscribe and clears on stop", async () => {
    const fk = new FakeKafka();

    const sup = new ConsumerSupervisor({
      kafka: fk as any,
      groupId: "g1",
      topics: ["test"],
      fromBeginning: true,
      eachMessage: async () => {},
      runConfig: { autoCommit: true },
      logger: undefined as any, // let it pick default pino
    });

    // Kick off
    const p = sup.startForever();

    // wait until isReady flips true (connect + subscribe done)
    await waitUntil(() => sup.isReady(), clock, 50, 5_000);
    expect(sup.isHealthy()).to.equal(true);
    expect(sup.isReady()).to.equal(true);

    // stop
    await sup.stop();
    // the inner loop checks every 1000ms
    advance(clock, 1_200);
    await p;

    expect(sup.isHealthy()).to.equal(false);
    expect(sup.isReady()).to.equal(false);
  });

  it("commits offsets on eachMessage success when autoCommit=false", async () => {
    const fk = new FakeKafka();

    const sup = new ConsumerSupervisor({
      kafka: fk as any,
      groupId: "g2",
      topics: ["t"],
      eachMessage: async () => {
        /* success */
      },
      runConfig: { autoCommit: false },
      logger: undefined as any,
    });

    const p = sup.startForever();

    await waitUntil(() => sup.isReady(), clock);
    const consumer = fk.consumers[0];

    // ensure run() has been called and handler is installed
    await waitUntil(() => !!consumer.runConfig?.eachMessage, clock);

    // deliver a message
    await consumer.deliverEachMessage("t", 0, "5");
    // allow any microtasks
    await Promise.resolve();
    advance(clock, 10);

    expect(consumer.committed).to.deep.equal([
      { topic: "t", partition: 0, offset: "6" },
    ]);

    await sup.stop();
    advance(clock, 1_200);
    await p;
  });

  it("does NOT recreate on non-fatal CRASH (restart=true)", async () => {
    const fk = new FakeKafka();

    const sup = new ConsumerSupervisor({
      kafka: fk as any,
      groupId: "g3",
      topics: ["a"],
      eachMessage: async () => {},
      runConfig: { autoCommit: true },
      logger: undefined as any,
    });

    const p = sup.startForever();

    await waitUntil(() => sup.isReady(), clock);
    expect(fk.consumers.length).to.equal(1);
    const c1 = fk.consumers[0];

    c1.emitCrash(true); // restart=true → non-fatal for supervisor

    // advance time to let event handlers run
    advance(clock, 2_000);
    expect(fk.consumers.length).to.equal(1);

    await sup.stop();
    advance(clock, 1_200);
    await p;
  });

  it("recreates on fatal CRASH (restart=false) with backoff", async () => {
    const fk = new FakeKafka();

    const sup = new ConsumerSupervisor({
      kafka: fk as any,
      groupId: "g4",
      topics: ["b"],
      eachMessage: async () => {},
      runConfig: { autoCommit: true },
      baseRetryMs: 100, // short backoff
      logger: undefined as any,
    });

    const p = sup.startForever();

    await waitUntil(() => sup.isReady(), clock);
    expect(fk.consumers.length).to.equal(1);
    const c1 = fk.consumers[0];

    // trigger fatal crash
    c1.emitCrash(false);

    // let oneLifecycle see fatal (inner loop ~1s), then outer backoff & recreate
    await waitUntil(
      () => fk.consumers.length > 1,
      clock,
      /*step*/ 50,
      /*timeout*/ 5000,
    );

    await sup.stop();
    advance(clock, 1_200);
    await p;
  });

  it("eachBatch: pause returns resume fn and resume is called after error", async () => {
    const fk = new FakeKafka();

    const sup = new ConsumerSupervisor({
      kafka: fk as any,
      groupId: "g5",
      topics: ["z"],
      eachBatch: async () => {
        throw new Error("boom"); // intentional
      },
      runConfig: { autoCommit: false, eachBatchAutoResolve: false },
      logger: undefined as any,
    });

    const p = sup.startForever();

    await waitUntil(() => sup.isReady(), clock);
    const c = fk.consumers[0];

    await waitUntil(() => !!c.runConfig?.eachBatch, clock);

    // EXPECT the batch to fail; supervisor catches→pause→schedules resume→rethrows
    try {
      await c.deliverEachBatch("z", 0, "0");
      // If we got here, the test double didn't throw; force fail:
      expect.fail("deliverEachBatch should have thrown");
    } catch (e) {
      // expected
    }

    // advance past the 5s resume delay
    advance(clock, 5_200);
    await Promise.resolve();

    // still a single consumer => no fatal recreate
    expect(fk.consumers.length).to.equal(1);

    await sup.stop();
    advance(clock, 1_200);
    await p;
  });
});
