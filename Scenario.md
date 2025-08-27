# SCENARIOS — Power-MSK v2

This document explains the behavior of **ConsumerSupervisor** and **ResilientProducer** in real-world scenarios.
The goal is to help developers understand *when a component restarts, when it pauses, and how resilience is enforced*.

---

## 🟢 ConsumerSupervisor Lifecycle

### Normal startup

```mermaid
sequenceDiagram
    participant App
    participant Supervisor
    participant Kafka

    App->>Supervisor: startForever()
    Supervisor->>Kafka: connect + subscribe
    Kafka-->>Supervisor: CONNECT + GROUP_JOIN
    Supervisor-->>App: healthy=true, ready=true
```

* Consumer starts, joins group, begins processing.
* `isHealthy()` and `isReady()` both flip `true`.

---

### Message handler error (`eachMessage` / `eachBatch`)

```mermaid
sequenceDiagram
    participant Kafka
    participant Consumer
    participant Supervisor

    Kafka-->>Consumer: message
    Consumer->>Supervisor: handler throws
    Supervisor->>Consumer: pause(partition)
    Note right of Supervisor: Partition paused for 5s
    Supervisor->>Consumer: resume(partition)
```

* Partition where error occurred is **paused**.
* Automatically **resumes after 5s**.
* Supervisor keeps running; other partitions unaffected.

---

### Crash (KafkaJS emits `CRASH`)

```mermaid
sequenceDiagram
    participant Kafka
    participant Consumer
    participant Supervisor

    Kafka-->>Consumer: CRASH { restart=false }
    Consumer->>Supervisor: event
    Supervisor->>Supervisor: mark fatalCrash=true
    Supervisor-->>App: lifecycle ends
    Supervisor->>Supervisor: outer loop restarts consumer with backoff
```

* If `restart=true` → KafkaJS handles internally (no restart by us).
* If `restart=false` → Supervisor tears down and **recreates consumer**.
* Backoff = exponential + jitter.

---

### Stop

```mermaid
sequenceDiagram
    participant App
    participant Supervisor
    participant Kafka

    App->>Supervisor: stop()
    Supervisor->>Kafka: stop() + disconnect()
    Supervisor-->>App: healthy=false, ready=false
```

* Graceful stop shuts down the loop and disconnects.
* `isHealthy()` and `isReady()` both flip `false`.

---

## 🟢 ResilientProducer Lifecycle

### Normal flow

```mermaid
sequenceDiagram
    participant App
    participant Producer
    participant Kafka

    App->>Producer: start()
    Producer->>Kafka: connect()
    Kafka-->>Producer: CONNECT
    Producer-->>App: healthy=true, ready=true

    App->>Producer: send(topic, messages)
    Producer->>Kafka: batch send
    Kafka-->>Producer: ack
    Producer-->>App: resolve()
```

---

### Send error (recoverable)

```mermaid
sequenceDiagram
    participant Producer
    participant Kafka

    Producer->>Kafka: send
    Kafka-->>Producer: Error
    Producer->>Producer: recreateOnError(err)=true
    Producer->>Kafka: disconnect + reconnect
    Producer->>Kafka: retry send (with backoff)
```

* `send()` error triggers retry loop.
* If `recreateOnError(err)` returns true, producer is torn down + recreated.
* Entries in the batch resolve after retry succeeds.

---

### Backpressure

```mermaid
sequenceDiagram
    participant App
    participant Producer

    App->>Producer: send(large batch)
    Producer->>Producer: qBytes > maxQueueBytes
    Producer-->>App: Error("queue full")
```

* Protects against OOM by bounding queue size.
* App should catch and retry later.

---

### Stop

```mermaid
sequenceDiagram
    participant App
    participant Producer
    participant Kafka

    App->>Producer: stop()
    Producer->>Kafka: flush pending batches
    Producer->>Kafka: disconnect()
    Producer-->>App: healthy=false, ready=false
```

---

## 🔄 Summary Table

| Scenario            | ConsumerSupervisor Action    | ResilientProducer Action     |
| ------------------- | ---------------------------- | ---------------------------- |
| Handler throws      | Pause partition (5s)         | N/A                          |
| Non-retriable CRASH | Recreate consumer w/ backoff | N/A                          |
| Kafka rebalance     | KafkaJS auto-handles         | N/A                          |
| Producer send error | N/A                          | Retry, optional recreate     |
| Queue full          | N/A                          | Reject send                  |
| stop() called       | Disconnect + mark unhealthy  | Flush, disconnect, unhealthy |

---
