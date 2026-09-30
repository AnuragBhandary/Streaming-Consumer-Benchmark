# Streaming Consumer Benchmark: asyncio vs virtual threads

[![CI](https://github.com/AnuragBhandary/Streaming-Consumer-Benchmark/actions/workflows/ci.yml/badge.svg)](https://github.com/AnuragBhandary/Streaming-Consumer-Benchmark/actions/workflows/ci.yml)
![Java 21](https://img.shields.io/badge/java-21-orange) ![Python 3.12](https://img.shields.io/badge/python-3.12-blue)

The fan-out hot path from my [real-time streaming platform](https://github.com/AnuragBhandary/Real-Time-Event-Streaming),
Kafka in and thousands of TCP sockets out, **written twice**:

- **Java 21**: one virtual thread per connection, plain blocking I/O, no event loop.
- **Python 3.12**: asyncio on uvloop, one event loop per process.

Both implement the same protocol (checked by a shared conformance suite against real Kafka) and are
driven by **one load harness**: an open-loop Kafka producer and a client swarm that audits every
sequence number and measures end-to-end latency. Every run is a cold container with the warm-up
window discarded; results are medians of repetitions.

## Findings (details in [docs/RESULTS.md](docs/RESULTS.md))

![p99 vs connections](docs/charts/scaling_p99.png)

| 2 CPUs / 1 GiB per server | Java 21, virtual threads | Python, asyncio |
|---|---|---|
| ≤ 5,000 connections (≤ 50k deliveries/s) | p50 ≈ 2 ms, p99 ≈ 4-5 ms | p50 ≈ 2 ms, p99 ≈ 5-10 ms |
| 10,000 connections (100k deliveries/s) | p99 **8 ms** | 1 process: p99 **253 ms** (one core at 100%) · 2 processes: 18 ms |
| 20,000 connections (200k deliveries/s) | 2 dispatch threads: p50 **2.5 ms**, p99 67 ms | 2 processes: p50 184 ms |
| 320,000 deliveries/s (2,000 connections) | p99 25-28 ms | 1 process: 57 ms · 2 processes: 161 ms |
| memory per connection | ≈ 28 KiB (+190 MiB JVM base) | ≈ 12 KiB (+45 MiB base) |
| JVM warm-up | p99 14 ms in the first second only, then ≈ 3 ms | none |
| slow consumers | fast clients unaffected; stalled ones cut off and resumed, 0 lost | same; asyncio's larger user-space buffer rides out ~2× more of each stall |
| correctness (every run) | 0 duplicates, 0 gaps | 0 duplicates, 0 gaps |

What the measurements uncovered along the way:
- **Java ran out of memory at 10k connections** in its first version: 64 KiB write buffers plus
  an 8 KiB reader per connection. Right-sizing them cut memory at 5k connections from 880 to
  337 MiB.
- **One dispatch thread concentrated virtual-thread wake-ups on one carrier.** At 20k
  connections, one carrier ran at 81% while the other sat idle. GC and carrier count were ruled out
  with GC logs and what-if runs. Two dispatch threads took p50 from 87 ms to 2.3 ms.
- **Linux silently absorbed stalled clients** in up to 4 MiB of kernel send buffer per socket.
  Capping `SO_SNDBUF` made backpressure reach the application's bounded queue.

**Read next:** [docs/RESULTS.md](docs/RESULTS.md) (charts and findings) ·
[docs/DESIGN.md](docs/DESIGN.md) (how both servers work, fairness controls) ·
[docs/INTERVIEW.md](docs/INTERVIEW.md) (study guide).

## Layout

```
java-server/      Java 21 server (Maven): server/ (virtual threads, JUnit tests), jmh/ (JMH hot-path benchmarks)
python-server/    Python asyncio server (uv): src/fanout_py, tests, benchmarks/microbench.py
harness/          one harness for both: producer, client swarm, runner, report, conformance tests
results/          raw JSON of every run
docs/             RESULTS.md, DESIGN.md, INTERVIEW.md, charts/, tables.md (generated)
```

## Run it

```bash
make up            # Kafka + java-server, java2-server, python-server, python2-server
make conformance   # identical behaviour from all four, against real Kafka
make smoke         # ~1 minute benchmark
make bench         # every experiment in RESULTS.md (~3 hours)
make jmh microbench report
```

The protocol is newline-delimited TCP: `SUB <stream> <cursor>` → `{"type":"ok","head":H,"reset":false}`
followed by events in `seq` order. See [DESIGN.md §2](docs/DESIGN.md).
