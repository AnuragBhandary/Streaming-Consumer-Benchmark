# Design

## 1. The question

My streaming platform ([Real-Time-Event-Streaming](https://github.com/AnuragBhandary/Real-Time-Event-Streaming))
fans events out to thousands of sockets from a Python asyncio event loop. Java 21 made a
different model practical: **one cheap virtual thread per connection, with plain blocking code**.
This project reimplements the fan-out hot path in both models and measures them under one harness:

- How do latency, throughput, CPU and memory behave as connections and event rates grow?
- Where do the two models *diverge*, especially with slow consumers and backpressure?

## 2. What both servers do (identical contract)

```
Kafka topic "events" (12 partitions, key = stream)
        │  one consumer, all partitions, starting at the end
        ▼
  parse {stream, seq} ──► per-stream state: ring buffer (last 10,000) + subscribers
        │                  publish = append + non-blocking offer to every subscriber queue
        ▼
  per-connection bounded queue (1,000 events) ──► writer ──► TCP socket
```

**Protocol** (newline-delimited, identical for both; checked by `harness/tests/test_conformance.py`):

| Client sends | Server replies |
|---|---|
| `SUB <stream> -1` | `{"type":"ok","head":H,"reset":false}` then every new event |
| `SUB <stream> <cursor>` | `ok`, then every event after `cursor` from the ring buffer, then live |
| cursor older than the buffer / ahead of the stream | `{"type":"ok","head":H,"reset":true}` (client resumes at H) |
| `STATS` | one JSON line of counters |
| anything else | `{"type":"error",...}` |

Events are the Kafka record bytes plus `\n`, built once and shared by every connection.

**Why plain TCP, not WebSockets?** The goal is to compare concurrency models, not WebSocket
libraries. The JDK has no WebSocket server, and a framework (Jetty, Netty) would bring its own
threading model into the measurement.

**Why Kafka?** It is the realistic source for a fan-out tier. Keying by stream keeps each stream
ordered on one partition. Both servers use the same consumer settings (`fetch.max.wait.ms=5`,
`max.poll.records=5000`, assign all partitions, seek to end).

## 3. The two concurrency models

### Java 21: virtual thread per connection (`java-server/`)
- `ServerSocket.accept()` → `Executors.newVirtualThreadPerTaskExecutor()`: one virtual thread
  per client, running a straight-line blocking loop: `queue.take()` → `write` → `flush`.
- **Backpressure is a blocking `write()`**. When a client stops reading, the kernel buffer fills,
  `write` blocks, and only that virtual thread parks. Its carrier (OS) thread moves on to other
  connections.
- A **platform** thread runs the Kafka poll loop (`KafkaConsumer` is not thread-safe) and calls
  `StreamState.publish`, which offers to each subscriber's `ArrayBlockingQueue` without blocking.
  With `FANOUT_CONSUMERS=2` there are two such threads, each owning half the partitions (a stream
  lives on one partition, so order is kept). This matters more than it looks: on JDK 21, virtual
  threads woken from a single platform thread piled up on one carrier (see RESULTS.md §4), so
  it is benchmarked as a separate implementation (`java-2disp`).
- Stream state is guarded by a `ReentrantLock`, not `synchronized`: on Java 21 a virtual thread
  blocking inside `synchronized` pins its carrier (fixed in JDK 24 by JEP 491).
- Detecting a departed client: the writer spends its life parked in `take()`, so a second virtual
  thread per connection blocks on `read()` and interrupts the writer at EOF. Two threads per
  connection is fine when threads cost a few hundred bytes of stack.

### Python: asyncio event loop (`python-server/`)
- `asyncio.start_server` on **uvloop**: one thread, one event loop, a coroutine per connection.
- **Backpressure is `await writer.drain()`**, which suspends the writer coroutine while the
  transport's buffer is above its high-water mark.
- The Kafka loop (`aiokafka`) and all connections share the same thread, so `publish` and
  `attach` can never interleave. No locks are needed.
- Departed clients: a small reader task awaits EOF and wakes the writer.
- **Multi-core**: one loop uses one core. The standard answer is N independent processes sharing
  the port (`SO_REUSEPORT`, `FANOUT_WORKERS=2`): the kernel spreads connections, and each
  process consumes Kafka and serves its own clients. It is benchmarked as a third
  implementation (`python-2proc`).

### Shared semantics
- **Kernel buffers**: both servers can cap each socket's send buffer (`FANOUT_SNDBUF`). Linux
  otherwise auto-grows it up to 4 MiB per connection, which quietly absorbs a stalled client's
  backlog. At 10k connections that is up to 40 GiB of kernel memory the server doesn't account for.
- **Ordering**: per-stream `seq` from the producer; the server drops `seq <= head` (Kafka
  redelivery). Clients audit contiguity.
- **Race-free join**: registering a subscriber and snapshotting its replay happen atomically
  (the lock in Java, a single non-awaiting code path in Python).
- **Slow consumers**: `offer` fails when the queue is full → the connection is closed → the client
  reconnects with its cursor and replays from the ring buffer (or resets if too far behind).
  Fast clients never wait for slow ones.

## 4. The harness (one for both)

`harness/src/bench/`:
- `producer.py`: open-loop Kafka producer (`rate` events/s round-robin over `streams`), stamping
  `sent_at` and per-stream `seq`.
- `clients.py`: N TCP subscribers over 6 processes (uvloop `Protocol`s, no buffering library).
  It records latency (receive − `sent_at`) into mergeable log histograms (2% precision), audits
  duplicates and gaps, reconnects with the cursor when cut off, and optionally stalls a fraction of
  clients by pausing the socket read (true TCP-level slowness). A parallel Kafka-only consumer
  measures the broker's share of the latency.
- `run.py`: for every run it stops the other servers, **recreates the server container** (cold
  start), connects clients, produces for `warmup + measure + 5` s, **discards the warm-up window**,
  samples server and harness CPU/memory with `docker stats`, then checks every client reached the
  producer's final seq.

**Implementations measured**: `java` (1 dispatch thread), `java-2disp` (2 dispatch threads),
`python` (1 event loop), `python-2proc` (2 processes). Each language gets a "default" and a
"use both cores" configuration.

**Fairness controls**
- Same container limits for every server: 2 CPUs, 1 GiB. Clients get 6 CPUs, the producer 1,
  Kafka 2, on a 12-vCPU VM.
- Same workload files, seeds, Kafka topic, consumer settings and queue/ring sizes.
- Harness CPU is recorded for every run; a run where the harness nears its limit would measure the
  harness, not the server.
- JVM warm-up: 30 s discarded per run, plus a dedicated cold-start experiment (per-second p99) that
  shows how long warm-up actually takes; JMH with warm-up iterations and forks for the hot path.
- Repetitions: medians of 3 for scaling (min-max bands on the charts).

## 5. Where the models diverge (summary; numbers in RESULTS.md)

| Concern | Java virtual threads | Python asyncio |
|---|---|---|
| Programming model | Blocking, straight-line; stack traces make sense | `async`/`await` everywhere; one blocking call stalls every client |
| Cores | All cores from one process | One core per process; scale with processes (`SO_REUSEPORT`) |
| Per-connection memory | Thread stack + buffers; must be sized deliberately | Small: a coroutine, a deque, transport buffers |
| Slow client | Its writer thread parks in `write()`; queue fills; cut off | `drain()` suspends its coroutine; queue fills; cut off |
| Detecting disconnects | Needs a reader (second virtual thread) | Transport callbacks / a reader task |
| Shared state | Needs locks (and care with pinning on Java 21) | No locks (single thread) |
| Dispatch parallelism | Threads you choose; one dispatcher concentrated wake-ups on one carrier | One loop per process; parallel only across processes |
| Warm-up | JIT: slower first second | None |

## 6. Limitations

- One machine: clients, producer, Kafka and the server share a laptop (in separate containers with
  CPU limits). Absolute numbers are laptop numbers; the comparison is the point.
- One Kafka broker, no replication (the broker is not what is being measured; the Kafka-only
  probe shows its share).
- Python's second process doubles Kafka consumption (each process consumes every event), which a
  production design might avoid with partition-aware routing.
