"""Client swarm: ``--conns`` TCP subscribers spread over ``--procs`` processes (uvloop).

Every connection subscribes to one stream, checks that seqs arrive exactly once and in order,
and records end-to-end latency (receive time - producer ``sent_at``) during the measurement
window only. When the server disconnects it (slow consumer), it reconnects with its cursor.
Optionally a fraction of clients periodically stop reading at the TCP level (slow consumers).

Control protocol on stdin/stdout (driven by ``bench.run``):
    -> READY {...}                 all connections subscribed
    <- START <warmup_s> <measure_s>
    <- FINAL {"s0": 123, ...}      producer finished; these are the last seqs
    -> RESULT {...}
"""

from __future__ import annotations

import argparse
import asyncio
import multiprocessing as mp
import random
import socket
import sys
import time
from multiprocessing.connection import Connection
from typing import Any, cast

import orjson

from bench.hist import Histogram


class Stats:
    def __init__(self) -> None:
        self.hist = Histogram()  # clients that keep reading
        self.stall_hist = Histogram()  # clients that periodically stop reading
        self.measuring = False
        self.received_in_window = 0
        self.received = 0
        self.duplicates = 0
        self.gaps = 0
        self.resets = 0
        self.server_disconnects = 0
        self.reconnects = 0
        # Optional per-second latency timeline from START (shows JIT warm-up as it happens).
        self.timeline_start_ms: float | None = None
        self.timeline: dict[int, Histogram] = {}


class Subscriber(asyncio.Protocol):
    def __init__(self, pool: Pool, stream: str, stall: bool) -> None:
        self.pool = pool
        self.stream = stream
        self.cursor = -1  # -1: start live; afterwards the last seq processed
        self.subscribed = False
        self.transport: asyncio.Transport | None = None
        self.buffer = b""
        self.stall = stall

    # -- asyncio.Protocol ---------------------------------------------------------------------

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = cast(asyncio.Transport, transport)  # uvloop's class is not a subclass
        self.transport.write(f"SUB {self.stream} {self.cursor}\n".encode())

    def data_received(self, data: bytes) -> None:
        stats = self.pool.stats
        now_ms = time.time() * 1000
        lines = (self.buffer + data).split(b"\n")
        self.buffer = lines.pop()
        loads = orjson.loads
        for line in lines:
            if line.startswith(b'{"type"'):
                ctrl = loads(line)
                if ctrl.get("type") == "ok":
                    if ctrl["reset"]:
                        stats.resets += 1
                        self.cursor = ctrl["head"]
                    elif self.cursor < 0:
                        self.cursor = ctrl["head"]
                    self.subscribed = True
                continue
            event = loads(line)
            seq = event["seq"]
            if seq <= self.cursor:
                stats.duplicates += 1
                continue
            if seq != self.cursor + 1:
                stats.gaps += 1
            self.cursor = seq
            stats.received += 1
            if stats.timeline_start_ms is not None:
                second = int((now_ms - stats.timeline_start_ms) / 1000)
                hist = stats.timeline.get(second)
                if hist is None:
                    hist = stats.timeline[second] = Histogram()
                hist.record(now_ms - event["sent_at"])
            if stats.measuring:
                stats.received_in_window += 1
                (stats.stall_hist if self.stall else stats.hist).record(now_ms - event["sent_at"])

    def connection_lost(self, exc: Exception | None) -> None:
        self.transport = None
        self.buffer = b""
        if not self.pool.closing:
            self.pool.stats.server_disconnects += 1
            self.pool.reconnect_soon(self)

    # -- slow-consumer simulation -------------------------------------------------------------

    async def stall_loop(self, every_s: float, for_s: float, rng: random.Random) -> None:
        await asyncio.sleep(rng.uniform(0, every_s))
        while True:
            transport = self.transport
            if transport is not None:
                transport.pause_reading()  # stop reading: the server's writes back up
                await asyncio.sleep(for_s)
                if not transport.is_closing():
                    transport.resume_reading()
            await asyncio.sleep(every_s)


class Pool:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.stats = Stats()
        self.subs: list[Subscriber] = []
        self.closing = False
        self.loop = asyncio.get_running_loop()

    async def connect(self, sub: Subscriber) -> None:
        for attempt in range(50):
            try:
                if sub.stall:
                    # A small receive window, like a congested mobile client: when it stops
                    # reading, the backlog lands in the server's queue instead of in megabytes
                    # of kernel buffers.
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024)
                    sock.setblocking(False)
                    info = await self.loop.getaddrinfo(
                        self.host, self.port, family=socket.AF_INET, type=socket.SOCK_STREAM
                    )
                    await self.loop.sock_connect(sock, info[0][4])
                    await self.loop.create_connection(lambda: sub, sock=sock)
                else:
                    await self.loop.create_connection(lambda: sub, self.host, self.port)
                return
            except OSError:
                await asyncio.sleep(min(2.0, 0.05 * 2**attempt) * random.random())
        raise ConnectionError(f"could not connect to {self.host}:{self.port}")

    def reconnect_soon(self, sub: Subscriber) -> None:
        self.stats.reconnects += 1

        async def later() -> None:
            await asyncio.sleep(random.uniform(0.05, 0.3))
            if not self.closing:
                await self.connect(sub)

        self.loop.create_task(later())


async def _worker(cfg: dict[str, Any], pipe: Connection) -> None:
    pool = Pool(cfg["host"], cfg["port"])
    rng = random.Random(cfg["seed"])
    for i, stream in enumerate(cfg["streams"]):
        sub = Subscriber(pool, stream, stall=rng.random() < cfg["stall_fraction"])
        pool.subs.append(sub)
        await pool.connect(sub)
        if i % 200 == 199:
            await asyncio.sleep(0.05)  # don't overflow the accept backlog
    while not all(s.subscribed for s in pool.subs):
        await asyncio.sleep(0.05)
    pipe.send(("READY", len(pool.subs)))

    loop = asyncio.get_running_loop()
    cmd, arg = await loop.run_in_executor(None, pipe.recv)
    assert cmd == "START"
    warmup_s, measure_s = arg
    if cfg["timeline"]:
        pool.stats.timeline_start_ms = time.time() * 1000
    stall_tasks = [
        asyncio.create_task(s.stall_loop(cfg["stall_every_s"], cfg["stall_for_s"], rng))
        for s in pool.subs
        if s.stall
    ]
    await asyncio.sleep(warmup_s)
    pool.stats.measuring = True
    await asyncio.sleep(measure_s)
    pool.stats.measuring = False
    for task in stall_tasks:
        task.cancel()
    for sub in pool.subs:
        if sub.transport is not None and not sub.transport.is_closing():
            sub.transport.resume_reading()

    cmd, final = await loop.run_in_executor(None, pipe.recv)
    assert cmd == "FINAL"
    deadline = time.monotonic() + cfg["drain_timeout_s"]
    while time.monotonic() < deadline:
        if all(s.cursor >= final[s.stream] for s in pool.subs):
            break
        await asyncio.sleep(0.1)
    complete = sum(s.cursor >= final[s.stream] for s in pool.subs)
    pool.closing = True
    for sub in pool.subs:
        if sub.transport is not None:
            sub.transport.close()
    st = pool.stats
    pipe.send(
        (
            "RESULT",
            {
                "clients": len(pool.subs),
                "complete": complete,
                "stalling_clients": sum(s.stall for s in pool.subs),
                "received": st.received,
                "received_in_window": st.received_in_window,
                "duplicates": st.duplicates,
                "gaps": st.gaps,
                "resets": st.resets,
                "server_disconnects": st.server_disconnects,
                "hist": st.hist.to_json(),
                "stall_hist": st.stall_hist.to_json(),
                "timeline": {str(k): v.to_json() for k, v in st.timeline.items()},
            },
        )
    )


def _worker_main(cfg: dict[str, Any], pipe: Connection) -> None:
    import uvloop

    uvloop.run(_worker(cfg, pipe))


async def kafka_probe(bootstrap: str, topic: str, window: asyncio.Event, hist: Histogram) -> None:
    """Baseline: producer -> Kafka -> consumer latency, with no fan-out server in between."""
    from aiokafka import AIOKafkaConsumer

    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers=bootstrap, group_id=None, auto_offset_reset="latest",
        enable_auto_commit=False, fetch_max_wait_ms=5,
    )  # fmt: skip
    await consumer.start()
    try:
        while not consumer.assignment():
            await asyncio.sleep(0.2)
        await consumer.seek_to_end(*consumer.assignment())
        while True:
            batches = await consumer.getmany(timeout_ms=100)
            now_ms = time.time() * 1000
            if window.is_set():
                for records in batches.values():
                    for record in records:
                        hist.record(now_ms - orjson.loads(record.value)["sent_at"])
    finally:
        await consumer.stop()


def emit(tag: str, payload: Any) -> None:
    sys.stdout.write(f"{tag} {orjson.dumps(payload).decode()}\n")
    sys.stdout.flush()


async def orchestrate(args: argparse.Namespace) -> None:
    host, port = args.server.rsplit(":", 1)
    ctx = mp.get_context("spawn")
    workers = []
    for p in range(args.procs):
        streams = [f"s{i % args.streams}" for i in range(p, args.conns, args.procs)]
        cfg = {
            "host": host, "port": int(port), "streams": streams, "seed": args.seed + p,
            "stall_fraction": args.stall_fraction, "stall_every_s": args.stall_every_s,
            "stall_for_s": args.stall_for_s, "drain_timeout_s": args.drain_timeout_s,
            "timeline": args.timeline,
        }  # fmt: skip
        parent, child = ctx.Pipe()
        proc = ctx.Process(target=_worker_main, args=(cfg, child), daemon=True)
        proc.start()
        workers.append((proc, parent))
    loop = asyncio.get_running_loop()
    ready = [await loop.run_in_executor(None, pipe.recv) for _, pipe in workers]
    emit("READY", {"clients": sum(n for _, n in ready)})

    probe_hist = Histogram()
    window = asyncio.Event()
    probe = (
        asyncio.create_task(kafka_probe(args.kafka, args.topic, window, probe_hist))
        if args.kafka
        else None
    )

    line = await loop.run_in_executor(None, sys.stdin.readline)
    _, warmup_s, measure_s = line.split()
    for _, pipe in workers:
        pipe.send(("START", (float(warmup_s), float(measure_s))))
    await asyncio.sleep(float(warmup_s))
    window.set()
    await asyncio.sleep(float(measure_s))
    window.clear()

    line = await loop.run_in_executor(None, sys.stdin.readline)
    final = orjson.loads(line.split(" ", 1)[1])
    for _, pipe in workers:
        pipe.send(("FINAL", final))
    results = [await loop.run_in_executor(None, pipe.recv) for _, pipe in workers]
    if probe:
        probe.cancel()
    merged = Histogram()
    stalled = Histogram()
    timeline: dict[int, Histogram] = {}
    totals: dict[str, int] = {}
    for _, result in results:
        merged.merge(result.pop("hist"))
        stalled.merge(result.pop("stall_hist"))
        for second, counts in result.pop("timeline").items():
            timeline.setdefault(int(second), Histogram()).merge(counts)
        for key, value in result.items():
            totals[key] = totals.get(key, 0) + value
    emit(
        "RESULT",
        {
            **totals,
            "throughput_per_s": round(totals["received_in_window"] / float(measure_s), 1),
            "latency_ms": merged.summary(),
            "stalled_clients_latency_ms": stalled.summary(),
            "hist": merged.to_json(),
            "kafka_only_latency_ms": probe_hist.summary(),
            "timeline": [{"second": sec, **timeline[sec].summary()} for sec in sorted(timeline)],
        },
    )
    for proc, _ in workers:
        proc.join(timeout=10)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--server", required=True, help="host:port")
    parser.add_argument("--conns", type=int, default=1000)
    parser.add_argument("--streams", type=int, default=100)
    parser.add_argument("--procs", type=int, default=4)
    parser.add_argument("--stall-fraction", type=float, default=0.0)
    parser.add_argument("--stall-every-s", type=float, default=15)
    parser.add_argument("--stall-for-s", type=float, default=5)
    parser.add_argument("--drain-timeout-s", type=float, default=60)
    parser.add_argument("--kafka", default="kafka:9092", help="'' disables the Kafka-only probe")
    parser.add_argument("--topic", default="events")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--timeline", action="store_true", help="record per-second latency")
    args = parser.parse_args()
    import uvloop

    uvloop.run(orchestrate(args))


if __name__ == "__main__":
    main()
