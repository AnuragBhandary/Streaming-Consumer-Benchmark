# Interview guide

Numbers: [RESULTS.md](RESULTS.md). Reasoning: [DESIGN.md](DESIGN.md).

## 1. Learning path

| # | File | Be able to explain |
|---|---|---|
| 1 | `docs/DESIGN.md` §2 | The shared contract: protocol, ring buffer, bounded queues, reset |
| 2 | `java-server/.../StreamState.java` | Lock-protected publish/attach; why `ReentrantLock` (pinning on Java 21) |
| 3 | `java-server/.../ClientConnection.java` | Virtual thread per connection; blocking write = backpressure; reader thread for EOF; per-connection memory |
| 4 | `java-server/.../KafkaSource.java`, `Main.java` | One platform thread polls Kafka; accept loop + virtual-thread executor |
| 5 | `python-server/src/fanout_py/core.py` | The same logic on one event loop; `drain()`; no locks |
| 6 | `python-server/src/fanout_py/__main__.py` | aiokafka; `SO_REUSEPORT` multi-process mode |
| 7 | `harness/src/bench/clients.py` | TCP-level stalls, histograms, audit |
| 8 | `harness/src/bench/run.py` | Cold start, warm-up discard, resource sampling, fairness |
| 9 | `java-server/jmh/.../HotPathBenchmark.java` | JMH warm-up/forks; what micro-benchmarks can and cannot tell you |
| 10 | `docs/RESULTS.md` | Every chart, and *why* each curve bends where it does |

Practice: close each file and re-draw its control flow on paper.

## 2. The 60-second pitch

> "I took the fan-out hot path from my streaming platform, Kafka in and thousands of sockets out,
> and implemented it twice: once in Python asyncio and once in Java 21 with a virtual thread per
> connection and plain blocking I/O. Both honour the same protocol, which a conformance suite
> checks against real Kafka. One harness drives both: an open-loop producer and a client swarm
> that audits ordering and measures end-to-end latency. Every run is a cold container with the
> JIT warm-up window discarded, and the charts show medians over repetitions. The interesting
> findings were where they diverge. A single event loop saturates one core, and past that latency
> climbs into hundreds of milliseconds, while virtual threads spread across both cores. Java's
> first version ran out of memory at 10k connections because of per-connection buffers. And the
> two handle a stalled client differently: a parked virtual thread versus a suspended coroutine."

## 3. Questions and answers

**What is a virtual thread?**
A thread scheduled by the JVM, not the OS. When it blocks on I/O or a lock, the JVM unmounts it
from its carrier (a small pool of OS threads, one per core by default) and mounts another. So you
write blocking code, but you only pay for OS threads per core, not per connection.

**Then isn't it just an event loop in disguise?**
Underneath, yes: the JDK's socket I/O uses the same OS readiness notifications (epoll/kqueue).
The difference is the programming model (plain blocking code, real stack traces, no function
colouring) and that carriers run on all cores in parallel. An asyncio loop runs on one thread.

**What's pinning?**
On Java 21, a virtual thread that blocks while inside `synchronized` (or in native code) can't
unmount, so it holds its carrier. With few carriers that can stall everything. I used
`ReentrantLock` for shared stream state. JDK 24 (JEP 491) removed the `synchronized` limitation.

**How does backpressure work in each?**
Java: the writer thread's `write()` blocks when the kernel send buffer is full, so only that
virtual thread parks. Python: `await writer.drain()` suspends the coroutine when the transport
buffer passes its high-water mark. Either way, the Kafka side only does a non-blocking offer into a
bounded queue. A full queue means cut the client off, and it resumes from its cursor. A slow client
can never slow the producer path or other clients.

**Why did Java run out of memory, and what did you change?**
Every connection eagerly allocated a 64 KB `BufferedOutputStream` and an `InputStreamReader`,
which carries an 8 KB decode buffer. At 10k connections that's close to a gigabyte before any
data. I cut the write buffer to 8 KB (larger batches bypass it) and read the one command line
straight from the `InputStream`. Memory at 5k connections went from 880 MiB to about 340 MiB,
and 10k and 20k then ran fine. Lesson: "a thread per connection is cheap" is true for the thread;
you still own every buffer you attach to it.

**Why did Java fall over at 20k connections even after the memory fix?**
It wasn't CPU (114% of 200%) and it wasn't GC. With GC logging, pauses totalled about 0.4 s in a
100 s run, and doubling the heap barely changed anything. Per-thread `top` inside the container
showed one carrier thread at 81% and the other idle, and it stayed that way with 4 carriers. Every
writer thread was being woken by the single Kafka dispatch thread, and those wake-ups kept landing
on one carrier. Splitting dispatch across two threads (each owning half the Kafka partitions, so
per-stream order is kept) balanced the carriers at about 52% each, and p50 at 20k went from 87 ms to
about 2 ms. Lesson: virtual threads remove thread cost, not scheduling effects. Measure per-thread,
not just per-process.

**Why didn't stalled clients get disconnected at first?**
Linux auto-grows a socket's send buffer up to 4 MiB, so a 10-second stall at 100 events/s
(~250 KB) disappears into the kernel and never reaches the bounded queue. That's also a memory
risk: 10k connections × 4 MiB is 40 GiB. Capping `SO_SNDBUF` makes backpressure visible to the
application, which then cuts the client off and lets it resume from its cursor.

**Why does single-process Python fall over at 10k connections?**
10k connections × 10 events/s each = 100k socket writes per second, all on one thread. It hits
100% of one core, queues build, and p50 goes from about 2 ms to 140+ ms. The fix is more
processes, not more coroutines. With two processes behind `SO_REUSEPORT` it holds low
milliseconds at 10k.

**Is the comparison fair when Java can use two cores and Python only one?**
That's exactly why there are three lines: Java, Python with one process, and Python with two
processes. All get the same 2-CPU, 1 GiB container. "Python can't use the second core without
extra processes" is a real property of the model, so it's measured, not hidden.

**How did you control for JVM warm-up?**
Three ways. Every macro run starts a fresh container and discards the first 30 s. A dedicated
cold-start run records per-second p99 so you can see how long warm-up actually lasts. And JMH
runs warm-up iterations plus two forks for the micro-benchmarks.

**How do you know the harness wasn't the bottleneck?**
Its CPU is sampled in every run (6 CPUs available) and reported next to the server's. The
delivered ratio shows whether all expected events arrived, and a Kafka-only probe separates
broker latency from server latency.

**How do you know both servers do the same thing?**
A conformance suite runs the same tests against all three against real Kafka: live order,
resume from cursor, reset on a bad cursor, errors and stats. The harness also audits every run
for duplicates and gaps, and all were zero.

**What would you choose in production?**
For a fan-out tier with many mostly-idle connections and a Java shop: virtual threads, since
they're simple code that uses all cores. For a Python shop: asyncio with one process per core
behind `SO_REUSEPORT` or a load balancer. The deciding factors are team language and
per-connection memory discipline, not raw speed. Both hit the targets once sized correctly.

## 4. Leadership Principles stories

- **Dive Deep**: *The out-of-memory at 10k connections.* The benchmark showed Java using 880 MiB
  at 5k connections, then failing at 10k. I traced it to per-connection buffers, not the threads,
  fixed it, and re-ran every Java experiment so the results come from the same code.
- **Dive Deep (2)**: *The one busy carrier.* Java at 20k connections had high latency with CPU
  to spare. I ruled out GC with logs, ruled out carrier count, then per-thread CPU showed one
  carrier doing all the work. Sharding the dispatch fixed it: p50 went from 87 ms to about 2 ms.
- **Insist on the Highest Standards**: *My own harness fooled me twice.* Idle servers were
  consuming Kafka during other servers' runs, and the laptop went to sleep mid-run. I isolated each
  server per run, kept the machine awake, and re-ran the affected results instead of keeping them.
- **Are Right, A Lot / Disagree and Commit**: I didn't pre-decide a winner. Where a model loses
  (single-process asyncio past one core), RESULTS.md says so plainly and explains why.
- **Frugality**: Everything runs on a laptop with Docker and free CI; the harness shows its own
  limits so the numbers stay trustworthy.
