"""Kafka to TCP fan-out server on one asyncio event loop (uvloop)."""

from __future__ import annotations

import asyncio
import functools
import logging
import os
import signal
from dataclasses import dataclass

from aiokafka import AIOKafkaConsumer

from fanout_py.core import Registry, Stats, handle_connection

log = logging.getLogger("fanout_py")


@dataclass(frozen=True)
class Config:
    kafka: str
    topic: str
    port: int
    queue_capacity: int
    ring_capacity: int
    max_batch_bytes: int
    workers: int

    @classmethod
    def from_env(cls) -> Config:
        env = os.environ
        return cls(
            kafka=env.get("FANOUT_KAFKA", "localhost:9094"),
            topic=env.get("FANOUT_TOPIC", "events"),
            port=int(env.get("FANOUT_PORT", "7000")),
            queue_capacity=int(env.get("FANOUT_QUEUE", "1000")),
            ring_capacity=int(env.get("FANOUT_RING", "10000")),
            max_batch_bytes=int(env.get("FANOUT_MAX_BATCH_BYTES", "65536")),
            workers=int(env.get("FANOUT_WORKERS", "1")),
        )


async def consume(config: Config, registry: Registry, stats: Stats) -> None:
    # No group_id: aiokafka assigns every partition to this consumer (no group coordination),
    # starting at the end, the same as the Java server's assign() + seekToEnd().
    consumer = AIOKafkaConsumer(
        config.topic,
        bootstrap_servers=config.kafka,
        group_id=None,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        fetch_max_wait_ms=5,  # same as the Java server
        max_poll_records=5000,
    )
    await consumer.start()
    try:
        while not consumer.assignment():
            log.info("waiting for topic %s", config.topic)
            await asyncio.sleep(0.5)
        assigned = sorted(consumer.assignment())
        await consumer.seek_to_end(*assigned)
        for tp in assigned:
            await consumer.position(tp)
        stats.ready = True
        log.info("consuming %d partitions of %s", len(assigned), config.topic)
        publish = registry.publish_raw
        while True:
            batches = await consumer.getmany(timeout_ms=100, max_records=5000)
            for records in batches.values():
                for record in records:
                    publish(record.value, stats)
    finally:
        await consumer.stop()


async def serve(config: Config) -> None:
    registry = Registry(config.ring_capacity)
    stats = Stats()
    handler = functools.partial(
        handle_connection,
        registry=registry,
        stats=stats,
        queue_capacity=config.queue_capacity,
        max_batch_bytes=config.max_batch_bytes,
    )
    server = await asyncio.start_server(
        handler, port=config.port, backlog=8192, limit=1024, reuse_port=config.workers > 1
    )
    source = asyncio.create_task(consume(config, registry, stats))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    log.info("listening on %d (asyncio + uvloop, one event loop)", config.port)
    async with server:
        await stop.wait()
    source.cancel()
    await asyncio.gather(source, return_exceptions=True)


def _run(config: Config) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    import uvloop

    uvloop.run(serve(config))


def main() -> None:
    """One event loop per process. An asyncio loop runs on one core, so to use N cores the
    usual answer is N independent processes sharing the port (SO_REUSEPORT): the kernel spreads
    connections across them, and each consumes Kafka and fans out to its own clients."""
    config = Config.from_env()
    if config.workers <= 1:
        _run(config)
        return
    import multiprocessing

    ctx = multiprocessing.get_context("spawn")
    procs = [ctx.Process(target=_run, args=(config,), daemon=True) for _ in range(config.workers)]
    for proc in procs:
        proc.start()

    def forward(signum: int, _frame: object) -> None:
        for proc in procs:
            if proc.pid is not None:
                os.kill(proc.pid, signum)

    signal.signal(signal.SIGTERM, forward)
    signal.signal(signal.SIGINT, forward)
    for proc in procs:
        proc.join()


if __name__ == "__main__":
    main()
