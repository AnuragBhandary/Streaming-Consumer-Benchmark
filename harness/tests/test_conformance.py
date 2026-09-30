"""Conformance: both servers must behave identically, against real Kafka.

Needs the compose stack (``docker compose up -d --wait``); skipped otherwise. Every test runs
against every implementation, which is what makes the benchmark an apples-to-apples comparison.
"""

from __future__ import annotations

import asyncio
import os
import socket
import uuid
from collections.abc import AsyncIterator

import orjson
import pytest
from aiokafka import AIOKafkaProducer

SERVERS = {"java": 7002, "python": 7001, "python-2proc": 7003}
KAFKA = os.environ.get("CONFORMANCE_KAFKA", "localhost:9094")


def _stack_up() -> bool:
    try:
        with socket.create_connection(("localhost", SERVERS["java"]), timeout=1):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _stack_up(), reason="docker compose stack is not running")


@pytest.fixture
async def producer() -> AsyncIterator[AIOKafkaProducer]:
    p = AIOKafkaProducer(bootstrap_servers=KAFKA, linger_ms=0)
    await p.start()
    yield p
    await p.stop()


async def publish(p: AIOKafkaProducer, stream: str, seqs: range) -> None:
    for seq in seqs:
        value = orjson.dumps({"stream": stream, "seq": seq, "sent_at": 0, "payload": "x"})
        await p.send_and_wait("events", value, key=stream.encode())


class Client:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader, self.writer = reader, writer

    @classmethod
    async def open(cls, port: int, command: str) -> Client:
        reader, writer = await asyncio.open_connection("localhost", port)
        writer.write(command.encode() + b"\n")
        await writer.drain()
        return cls(reader, writer)

    async def line(self) -> dict[str, object]:
        raw = await asyncio.wait_for(self.reader.readline(), 10)
        assert raw, "connection closed"
        result: dict[str, object] = orjson.loads(raw)
        return result

    def close(self) -> None:
        self.writer.close()


async def settle(port: int, stream: str, head: int) -> None:
    """Wait until the server has consumed the stream up to head (python-2proc: both processes)."""
    for _ in range(200):
        oks = []
        for _ in range(4):
            c = await Client.open(port, f"SUB {stream} -1")
            oks.append((await c.line())["head"])
            c.close()
        if all(h == head for h in oks):
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"server never reached head {head}")


@pytest.mark.parametrize("impl", list(SERVERS))
async def test_live_events_arrive_in_order(impl: str, producer: AIOKafkaProducer) -> None:
    port, stream = SERVERS[impl], f"conf-{uuid.uuid4().hex[:8]}"
    client = await Client.open(port, f"SUB {stream} -1")
    assert await client.line() == {"type": "ok", "head": 0, "reset": False}
    await publish(producer, stream, range(1, 21))
    assert [(await client.line())["seq"] for _ in range(20)] == list(range(1, 21))
    client.close()


@pytest.mark.parametrize("impl", list(SERVERS))
async def test_resume_replays_after_cursor(impl: str, producer: AIOKafkaProducer) -> None:
    port, stream = SERVERS[impl], f"conf-{uuid.uuid4().hex[:8]}"
    await publish(producer, stream, range(1, 11))
    await settle(port, stream, 10)
    client = await Client.open(port, f"SUB {stream} 6")
    assert await client.line() == {"type": "ok", "head": 10, "reset": False}
    assert [(await client.line())["seq"] for _ in range(4)] == [7, 8, 9, 10]
    await publish(producer, stream, range(11, 13))
    assert [(await client.line())["seq"] for _ in range(2)] == [11, 12]
    client.close()


@pytest.mark.parametrize("impl", list(SERVERS))
async def test_future_cursor_resets(impl: str, producer: AIOKafkaProducer) -> None:
    port, stream = SERVERS[impl], f"conf-{uuid.uuid4().hex[:8]}"
    await publish(producer, stream, range(1, 4))
    await settle(port, stream, 3)
    client = await Client.open(port, f"SUB {stream} 50")
    assert await client.line() == {"type": "ok", "head": 3, "reset": True}
    client.close()


@pytest.mark.parametrize("impl", list(SERVERS))
async def test_errors_and_stats(impl: str) -> None:
    port = SERVERS[impl]
    bad = await Client.open(port, "HELLO")
    assert (await bad.line())["type"] == "error"
    stats = await Client.open(port, "STATS")
    body = await stats.line()
    assert body["ready"] is True and "impl" in body
