"""Python server: ring buffer, stream state, and the real TCP handler (no Kafka needed)."""

from __future__ import annotations

import asyncio
import functools
from collections.abc import AsyncIterator

import orjson
import pytest

from fanout_py.core import Registry, RingBuffer, Stats, StreamState, Subscriber, handle_connection


def event(stream: str, seq: int, payload: str = "x") -> bytes:
    return orjson.dumps({"stream": stream, "seq": seq, "sent_at": 1.5, "payload": payload})


class TestRing:
    def test_keeps_last_capacity_events(self) -> None:
        ring = RingBuffer(3)
        for seq in range(1, 6):
            ring.add(seq, bytes([seq]))
        assert (ring.oldest_seq, ring.newest_seq) == (3, 5)
        assert ring.covers(2) and not ring.covers(1) and not ring.covers(6)
        assert ring.since(3) == [b"\x04", b"\x05"] and ring.since(5) == []

    def test_restarts_on_jump_and_validates(self) -> None:
        ring = RingBuffer(5)
        for seq in (1, 2, 10):
            ring.add(seq, b"")
        assert (ring.oldest_seq, ring.newest_seq) == (10, 10)
        assert RingBuffer(2).newest_seq == 0
        with pytest.raises(ValueError):
            RingBuffer(0)


class FakeWriter:
    class Transport:
        aborted = False

        def abort(self) -> None:
            self.aborted = True

    def __init__(self) -> None:
        self.transport = self.Transport()


def test_stream_attach_and_publish() -> None:
    stats = Stats()
    state = StreamState("s", RingBuffer(5))
    sub = Subscriber(FakeWriter(), 3, 65536, stats)  # type: ignore[arg-type]
    for seq in range(1, 9):
        state.publish(seq, b"%d\n" % seq)
    assert state.attach(sub, -1).replay == [] and state.head == 8
    assert state.attach(sub, 5).replay == [b"6\n", b"7\n", b"8\n"]
    assert state.attach(sub, 1).reset and state.attach(sub, 99).reset
    assert state.publish(8, b"dup") == []  # redelivery
    assert state.publish(9, b"9") == [] and state.publish(10, b"10") == []
    assert state.publish(11, b"11") == []
    assert state.publish(12, b"12") == [sub]  # queue of 3 overflowed
    assert state.publish(13, b"13") == []  # reported once
    assert stats.slow_disconnects == 1
    state.detach(sub)
    assert not state.subscribers


def test_registry_parses_and_skips_malformed() -> None:
    registry, stats = Registry(10), Stats()
    registry.publish_raw(event("a", 1), stats)
    for bad in (
        b"nope",
        b"[1]",
        b'{"stream":"a"}',
        b'{"stream":1,"seq":2}',
        b'{"stream":"a","seq":0}',
    ):
        registry.publish_raw(bad, stats)
    assert registry.get("a").head == 1 and stats.consumed == 6
    assert b'"impl":"python-asyncio"' in stats.to_json(1)


@pytest.fixture
async def server() -> AsyncIterator[tuple[int, Registry, Stats]]:
    registry, stats = Registry(100), Stats()
    handler = functools.partial(
        handle_connection, registry=registry, stats=stats, queue_capacity=50, max_batch_bytes=4096
    )
    srv = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = srv.sockets[0].getsockname()[1]
    yield port, registry, stats
    srv.close()


async def _client(port: int, command: str) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(command.encode() + b"\n")
    await writer.drain()
    return reader, writer


async def _until(predicate: object, timeout: float = 5) -> None:
    async def poll() -> None:
        while not predicate():  # type: ignore[operator]
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


def _publish(
    registry: Registry, stats: Stats, stream: str, seqs: range, payload: str = "x"
) -> None:
    for seq in seqs:
        registry.publish_raw(event(stream, seq, payload), stats)


async def test_live_then_disconnect_is_noticed(server: tuple[int, Registry, Stats]) -> None:
    port, registry, stats = server
    _publish(registry, stats, "m", range(1, 4))
    reader, writer = await _client(port, "SUB m -1")
    assert orjson.loads(await reader.readline()) == {"type": "ok", "head": 3, "reset": False}
    _publish(registry, stats, "m", range(4, 7))
    for seq in range(4, 7):
        assert orjson.loads(await reader.readline())["seq"] == seq
    writer.close()
    # Noticed without any further event being published (read-side EOF watcher).
    await _until(lambda: not registry.get("m").subscribers and stats.connections == 0)


async def test_resume_and_reset(server: tuple[int, Registry, Stats]) -> None:
    port, registry, stats = server
    _publish(registry, stats, "m", range(1, 11))
    reader, _ = await _client(port, "SUB m 7")
    assert orjson.loads(await reader.readline())["reset"] is False
    assert [orjson.loads(await reader.readline())["seq"] for _ in range(3)] == [8, 9, 10]
    _publish(registry, stats, "m", range(11, 151))
    reader2, _ = await _client(port, "SUB m 3")
    assert orjson.loads(await reader2.readline()) == {"type": "ok", "head": 150, "reset": True}


async def test_slow_consumer_is_cut_off(server: tuple[int, Registry, Stats]) -> None:
    port, registry, stats = server
    # Keep the writers referenced: a garbage-collected StreamWriter closes its socket.
    stuck_reader, stuck_writer = await _client(port, "SUB m -1")  # never reads after this
    fast_reader, fast_writer = await _client(port, "SUB m -1")
    await stuck_reader.readline()
    await fast_reader.readline()

    async def drain() -> int:
        count = 0
        while await fast_reader.readline():
            count += 1
        return count

    drainer = asyncio.create_task(drain())
    padding = "y" * 20_000
    seq = 0
    while stats.slow_disconnects == 0 and seq < 3000:
        seq += 1
        _publish(registry, stats, "m", range(seq, seq + 1), padding)
        await asyncio.sleep(0)
    assert stats.slow_disconnects == 1
    await _until(lambda: len(registry.get("m").subscribers) == 1)
    drainer.cancel()
    stuck_writer.close()
    fast_writer.close()


async def test_stats_and_bad_commands(server: tuple[int, Registry, Stats]) -> None:
    port, _, _ = server
    reader, _ = await _client(port, "STATS")
    assert orjson.loads(await reader.readline())["impl"] == "python-asyncio"
    for bad in ("HELLO", "SUB m", "SUB m x"):
        reader, _ = await _client(port, bad)
        assert orjson.loads(await reader.readline())["type"] == "error"
