"""Streams, subscribers and the fan-out hot path on one asyncio event loop.

The same logic as the Java server, expressed the asyncio way: everything runs on one thread, so
there are no locks; ``publish`` and ``attach`` can never interleave because neither awaits.
Backpressure comes from ``await writer.drain()``, which suspends a subscriber's writer coroutine
when its transport buffer is above the high-water mark.
"""

from __future__ import annotations

import asyncio
import contextlib
import resource
from collections import deque
from dataclasses import dataclass, field

import orjson


@dataclass
class Stats:
    consumed: int = 0
    delivered: int = 0
    flushes: int = 0
    slow_disconnects: int = 0
    accepted: int = 0
    connections: int = 0
    ready: bool = False

    def to_json(self, streams: int) -> bytes:
        rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
        return (
            orjson.dumps(
                {
                    "impl": "python-asyncio",
                    "ready": self.ready,
                    "connections": self.connections,
                    "accepted": self.accepted,
                    "streams": streams,
                    "consumed": self.consumed,
                    "delivered": self.delivered,
                    "flushes": self.flushes,
                    "slow_disconnects": self.slow_disconnects,
                    "max_rss_mb": rss_mb,
                }
            )
            + b"\n"
        )


class RingBuffer:
    """The last ``capacity`` events of a stream; seqs are contiguous within it."""

    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self._lines: deque[bytes] = deque(maxlen=capacity)
        self.oldest_seq = 0

    def add(self, seq: int, line: bytes) -> None:
        if self._lines and seq != self.newest_seq + 1:
            self._lines.clear()  # joined mid-stream: restart the window
        if not self._lines:
            self.oldest_seq = seq
        elif len(self._lines) == self._lines.maxlen:
            self.oldest_seq += 1
        self._lines.append(line)

    @property
    def newest_seq(self) -> int:
        return self.oldest_seq + len(self._lines) - 1 if self._lines else 0

    def covers(self, cursor: int) -> bool:
        return bool(self._lines) and self.oldest_seq - 1 <= cursor <= self.newest_seq

    def since(self, cursor: int) -> list[bytes]:
        skip = cursor - self.oldest_seq + 1
        return list(self._lines)[skip:]


class Subscriber:
    """One client connection: a bounded queue plus a writer coroutine."""

    __slots__ = (
        "_max_batch_bytes", "_queue", "_wakeup", "capacity", "closed", "slow", "stats", "writer",
    )  # fmt: skip

    def __init__(
        self, writer: asyncio.StreamWriter, capacity: int, max_batch_bytes: int, stats: Stats
    ) -> None:
        self.writer = writer
        self.capacity = capacity
        self.stats = stats
        self.slow = False
        self.closed = False
        self._queue: deque[bytes] = deque()
        self._wakeup = asyncio.Event()
        self._max_batch_bytes = max_batch_bytes

    def offer(self, line: bytes) -> bool:
        """Never blocks. False means the queue overflowed and the client must be dropped."""
        if self.slow:
            return True
        if len(self._queue) >= self.capacity:
            self.slow = True
            self.stats.slow_disconnects += 1
            return False
        self._queue.append(line)
        self._wakeup.set()
        return True

    def kill(self) -> None:
        self.writer.transport.abort()
        self._wakeup.set()

    async def watch_for_disconnect(self, reader: asyncio.StreamReader) -> None:
        """The writer only notices a departed client when it next writes; reading until EOF
        notices immediately (the asyncio twin of the Java server's reader thread)."""
        with contextlib.suppress(ConnectionError, OSError):
            while await reader.read(1024):
                pass
        self.closed = True
        self._wakeup.set()

    async def write_loop(self) -> None:
        queue, writer, max_bytes = self._queue, self.writer, self._max_batch_bytes
        while not (self.slow or self.closed):
            await self._wakeup.wait()
            self._wakeup.clear()
            while queue and not (self.slow or self.closed):
                batch: list[bytes] = []
                size = 0
                while queue and size < max_bytes:
                    line = queue.popleft()
                    batch.append(line)
                    size += len(line)
                writer.write(b"".join(batch))
                await writer.drain()  # suspends this coroutine while the client is slow
                self.stats.delivered += len(batch)
                self.stats.flushes += 1


@dataclass
class Attach:
    head: int
    reset: bool
    replay: list[bytes]


@dataclass
class StreamState:
    id: str
    ring: RingBuffer
    head: int = 0
    subscribers: set[Subscriber] = field(default_factory=set)

    def publish(self, seq: int, line: bytes) -> list[Subscriber]:
        if seq <= self.head:
            return []  # Kafka redelivery
        self.head = seq
        self.ring.add(seq, line)
        return [s for s in self.subscribers if not s.offer(line)]

    def attach(self, sub: Subscriber, cursor: int) -> Attach:
        self.subscribers.add(sub)
        if cursor < 0 or cursor == self.head:
            return Attach(self.head, False, [])
        if cursor < self.head and self.ring.covers(cursor):
            return Attach(self.head, False, self.ring.since(cursor))
        return Attach(self.head, True, [])

    def detach(self, sub: Subscriber) -> None:
        self.subscribers.discard(sub)


class Registry:
    def __init__(self, ring_capacity: int) -> None:
        self.streams: dict[str, StreamState] = {}
        self._ring_capacity = ring_capacity

    def get(self, stream_id: str) -> StreamState:
        state = self.streams.get(stream_id)
        if state is None:
            state = self.streams[stream_id] = StreamState(
                stream_id, RingBuffer(self._ring_capacity)
            )
        return state

    def publish_raw(self, value: bytes, stats: Stats) -> None:
        """Parse one Kafka record value and fan it out (the hot path)."""
        stats.consumed += 1
        try:
            event = orjson.loads(value)
            stream, seq = event["stream"], event["seq"]
        except (orjson.JSONDecodeError, KeyError, TypeError):
            return
        if not isinstance(stream, str) or not isinstance(seq, int) or seq < 1:
            return
        for slow in self.get(stream).publish(seq, value + b"\n"):
            slow.kill()


ERROR_LINE = b'{"type":"error","reason":"expected: SUB <stream> <cursor>"}\n'


async def handle_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    registry: Registry,
    stats: Stats,
    queue_capacity: int,
    max_batch_bytes: int,
    socket_send_buffer: int = 0,
) -> None:
    stats.connections += 1
    stats.accepted += 1
    stream: StreamState | None = None
    sub: Subscriber | None = None
    try:
        sock = writer.get_extra_info("socket")
        if sock is not None:
            import socket

            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if socket_send_buffer > 0:  # same cap as the Java server's FANOUT_SNDBUF
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, socket_send_buffer)
        command = (await reader.readline()).decode("ascii", "replace").strip()
        if command == "STATS":
            writer.write(stats.to_json(len(registry.streams)))
            await writer.drain()
            return
        parts = command.split(" ")
        if len(parts) != 3 or parts[0] != "SUB":
            writer.write(ERROR_LINE)
            await writer.drain()
            return
        try:
            cursor = int(parts[2])
        except ValueError:
            writer.write(ERROR_LINE)
            await writer.drain()
            return
        stream = registry.get(parts[1])
        sub = Subscriber(writer, queue_capacity, max_batch_bytes, stats)
        attach = stream.attach(sub, cursor)
        writer.write(
            orjson.dumps({"type": "ok", "head": attach.head, "reset": attach.reset}) + b"\n"
        )
        if attach.replay:
            writer.write(b"".join(attach.replay))
        await writer.drain()
        stats.delivered += len(attach.replay)
        watcher = asyncio.create_task(sub.watch_for_disconnect(reader))
        try:
            await sub.write_loop()
        finally:
            watcher.cancel()
    except (ConnectionError, OSError, asyncio.IncompleteReadError):
        pass
    finally:
        if stream is not None and sub is not None:
            stream.detach(sub)
        stats.connections -= 1
        with contextlib.suppress(Exception):
            writer.close()
