import { Kafka, Producer, Message } from "kafkajs";

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
function backoffMs(base: number, attempt: number, cap = 30_000) {
  const exp = Math.min(cap, base * 2 ** attempt);
  return Math.floor(Math.random() * exp); // full jitter
}

export interface ResilientProducerOpts {
  kafka: Kafka;

  // KafkaJS producer knobs
  idempotent?: boolean; // default: true
  transactionalId?: string; // set to enable transactions
  maxInFlightRequests?: number; // default: 5 if idempotent else 100
  allowAutoTopicCreation?: boolean; // default: false
  acks?: -1 | 0 | 1; // default: -1 (all ISR)
  compression?: number; // KafkaJS CompressionTypes, optional

  // batching / flush
  lingerMs?: number; // default: 10
  maxBatchSize?: number; // default: 1000 messages
  maxQueueBytes?: number; // default: 10MB

  // resilience
  baseRetryMs?: number; // default: 1000
  // return true to recreate producer instance on send error
  recreateOnError?: (err: unknown) => boolean;
}

type Enqueued = {
  topic: string;
  messages: Message[];
  resolve: () => void;
  reject: (err: any) => void; //eslint-disable-line
};

type IOpts = Required<
  Omit<
    ResilientProducerOpts,
    | "transactionalId"
    | "acks"
    | "compression"
    | "recreateOnError"
    | "idempotent"
    | "lingerMs"
    | "maxBatchSize"
    | "maxQueueBytes"
    | "allowAutoTopicCreation"
    | "maxInFlightRequests"
  >
> & {
  transactionalId?: string;
  acks?: -1 | 0 | 1;
  compression?: number;
  recreateOnError?: (err: unknown) => boolean;
  idempotent?: boolean;
  lingerMs: number;
  maxBatchSize: number;
  maxQueueBytes: number;
  allowAutoTopicCreation?: boolean;
  maxInFlightRequests?: number;
};

export class ResilientProducer {
  private readonly opts: IOpts;

  private producer!: Producer;
  private running = false;
  private healthy = false;
  private ready = false;

  private q: Enqueued[] = [];
  private qBytes = 0;
  private flusher?: Promise<void>;

  constructor(opts: ResilientProducerOpts) {
    this.opts = {
      kafka: opts.kafka,
      baseRetryMs: opts.baseRetryMs ?? 1000,
      lingerMs: opts.lingerMs ?? 10,
      maxBatchSize: opts.maxBatchSize ?? 1000,
      maxQueueBytes: opts.maxQueueBytes ?? 10 * 1024 * 1024,
      transactionalId: opts.transactionalId,
      acks: opts.acks ?? -1,
      compression: opts.compression,
      idempotent: opts.idempotent ?? true,
      allowAutoTopicCreation: opts.allowAutoTopicCreation ?? false,
      maxInFlightRequests:
        opts.maxInFlightRequests ?? (opts.idempotent ? 5 : 100),
      recreateOnError: opts.recreateOnError ?? (() => true),
    };
  }

  isHealthy() {
    return this.healthy;
  }

  isReady() {
    return this.ready;
  }

  async start() {
    if (this.running) return;
    this.running = true;
    let attempt = 0;

    while (this.running) {
      try {
        await this.createAndConnect();
        this.ready = true;
        this.healthy = true;
        attempt = 0; // reset after success
        this.flusher = this.flushLoop();
        await this.flusher; // ends when stop() flips running=false
      } catch {
        // swallow; we'll backoff & retry
        // add a warning here
      } finally {
        this.healthy = false;
        this.ready = false;
        await this.producer?.disconnect();
      }

      if (!this.running) break;
      attempt++;
      await sleep(backoffMs(this.opts.baseRetryMs, attempt));
    }
  }

  async stop() {
    this.running = false;
    this.ready = false;
    await this.flusher?.catch(() => {});
    await this.producer?.disconnect();
  }

  /** Enqueue messages; resolves when that batch is successfully sent */
  async send(topic: string, messages: Message[]) {
    if (!this.running) throw new Error("producer not started");
    const bytes = approxSize(messages);
    if (this.qBytes + bytes > this.opts.maxQueueBytes) {
      throw new Error("producer queue is full");
    }
    return new Promise<void>((resolve, reject) => {
      this.q.push({ topic, messages, resolve, reject });
      this.qBytes += bytes;
    });
  }

  async sendOne(topic: string, message: Message) {
    return this.send(topic, [message]);
  }

  /**
   *
   * @param fn Explain this further
   * @returns
   */
  async withTransaction<T>(
    fn: (txn: Awaited<ReturnType<Producer["transaction"]>>) => Promise<T>,
  ) {
    if (!this.producer) throw new Error("producer not ready");
    const txn = await this.producer.transaction();
    try {
      const out = await fn(txn);
      await txn.commit();
      return out;
    } catch (e) {
      await txn.abort();
      throw e;
    }
  }

  private async createAndConnect() {
    this.producer = this.opts.kafka.producer({
      idempotent: this.opts.idempotent,
      transactionalId: this.opts.transactionalId,
      maxInFlightRequests: this.opts.maxInFlightRequests,
      allowAutoTopicCreation: this.opts.allowAutoTopicCreation,
      retry: { initialRetryTime: 300, retries: 10 },
    });

    this.producer.on(this.producer.events.CONNECT, () => {
      this.healthy = true;
    });
    this.producer.on(this.producer.events.DISCONNECT, () => {
      this.healthy = false;
      this.ready = false;
    });

    await this.producer.connect();
  }

  private async flushLoop() {
    const { lingerMs, maxBatchSize, acks, compression } = this.opts;
    while (this.running) {
      const batch = this.dequeueBatch(maxBatchSize);
      if (batch.length === 0) {
        await sleep(lingerMs);
        continue;
      }

      // coalesce per topic
      const byTopic = new Map<
        string,
        { msgs: Message[]; entries: Enqueued[] }
      >();
      for (const e of batch) {
        const cur = byTopic.get(e.topic) ?? { msgs: [], entries: [] };
        cur.msgs.push(...e.messages);
        cur.entries.push(e);
        byTopic.set(e.topic, cur);
      }

      for (const [topic, { msgs, entries }] of byTopic) {
        let attempt = 0;
        // retry loop (with optional producer recreation)
        // eslint-disable-next-line no-constant-condition
        while (true) {
          try {
            await this.producer.send({
              topic,
              messages: msgs,
              acks,
              compression,
            });
            entries.forEach((e) => e.resolve());
            break;
          } catch (err) {
            attempt++;
            const recreate = this.opts.recreateOnError?.(err);
            if (recreate) {
              await this.producer.disconnect();
              await this.createAndConnect();
            }
            await sleep(backoffMs(this.opts.baseRetryMs, attempt));
            if (!this.running) {
              entries.forEach((e) => e.reject(err));
              throw err;
            }
          }
        }
      }
    }

    // drain on stop
    if (this.q.length) {
      try {
        const rest = this.dequeueBatch(Number.MAX_SAFE_INTEGER);
        const byTopic = new Map<string, Message[]>();
        rest.forEach((it) => {
          const arr = byTopic.get(it.topic) ?? [];
          arr.push(...it.messages);
          byTopic.set(it.topic, arr);
        });
        for (const [topic, msgs] of byTopic) {
          await this.producer.send({
            topic,
            messages: msgs,
            acks: this.opts.acks,
            compression: this.opts.compression,
          });
        }
        rest.forEach((it) => it.resolve());
      } catch (e) {
        const rest = this.dequeueBatch(Number.MAX_SAFE_INTEGER);
        rest.forEach((it) => it.reject(e));
      }
    }
  }

  private dequeueBatch(n: number): Enqueued[] {
    if (this.q.length === 0) return [];
    const out = this.q.splice(0, n);
    let bytes = 0;
    for (const it of out) {
      bytes += approxSize(it.messages);
    }
    this.qBytes = Math.max(0, this.qBytes - bytes);
    return out;
  }
}

function approxSize(messages: Message[]) {
  let size = 0;
  for (const m of messages) {
    size += (m.key ? byteLen(m.key) : 0) + (m.value ? byteLen(m.value) : 0);
  }
  return size;
}
function byteLen(v: string | Buffer | null | undefined) {
  if (v == null) {
    return 0;
  }
  return typeof v === "string" ? Buffer.byteLength(v) : v.length;
}
