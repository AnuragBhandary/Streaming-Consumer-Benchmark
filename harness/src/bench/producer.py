"""Workload producer: ``--rate`` events/s for ``--duration`` s, round-robin over ``--streams``
streams, open loop (the schedule never waits for the servers). Each event carries its per-stream
``seq`` and the wall-clock ``sent_at`` used for end-to-end latency. Kafka key = stream, so every
stream's events stay in order on one partition.

Prints ``RESULT {...}`` (including each stream's final seq) when done.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time

import orjson
from aiokafka import AIOKafkaProducer


async def produce(args: argparse.Namespace) -> dict[str, object]:
    producer = AIOKafkaProducer(
        bootstrap_servers=args.bootstrap, linger_ms=1, acks=1, max_batch_size=256 * 1024
    )
    await producer.start()
    streams = [f"s{i}" for i in range(args.streams)]
    seqs = dict.fromkeys(streams, 0)
    payload = "x" * args.payload_bytes
    total = int(args.rate * args.duration)
    pending: list[asyncio.Future[object]] = []
    sent = 0
    started = time.perf_counter()
    try:
        while sent < total:
            due = min(total, int((time.perf_counter() - started) * args.rate) + 1)
            while sent < due:
                stream = streams[sent % len(streams)]
                seqs[stream] += 1
                value = orjson.dumps(
                    {
                        "stream": stream,
                        "seq": seqs[stream],
                        "sent_at": time.time() * 1000,
                        "payload": payload,
                    }
                )
                pending.append(await producer.send(args.topic, value, key=stream.encode()))
                sent += 1
            if len(pending) > 10_000:
                await asyncio.gather(*pending)
                pending.clear()
            await asyncio.sleep(0.001)
        await asyncio.gather(*pending)
    finally:
        await producer.stop()
    elapsed = time.perf_counter() - started
    return {
        "sent": sent,
        "duration_s": round(elapsed, 2),
        "rate": round(sent / elapsed, 1),
        "final": seqs,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="kafka:9092")
    parser.add_argument("--topic", default="events")
    parser.add_argument("--streams", type=int, default=100)
    parser.add_argument("--rate", type=float, default=200)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--payload-bytes", type=int, default=200)
    args = parser.parse_args()
    import uvloop

    result = uvloop.run(produce(args))
    sys.stdout.write("RESULT " + orjson.dumps(result).decode() + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
