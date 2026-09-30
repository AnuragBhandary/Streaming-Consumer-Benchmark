"""Python counterpart of the JMH ``HotPathBenchmark``: the same two operations on the Python
server's real code, timed with ``timeit`` (best of several repeats, after a warm-up pass).

    cd python-server && uv run python benchmarks/microbench.py
"""

from __future__ import annotations

import asyncio
import json
import sys
import timeit

import orjson

from fanout_py.core import Registry, Stats, Subscriber

RAW = orjson.dumps({"stream": "s42", "seq": 1, "sent_at": 1790000000123.456, "payload": "x" * 200})


class _Writer:
    class transport:
        @staticmethod
        def abort() -> None: ...


def bench_parse(number: int = 200_000) -> float:
    def op() -> object:
        event = orjson.loads(RAW)
        return event["stream"], event["seq"]

    timeit.timeit(op, number=number // 10)  # warm-up
    return min(timeit.repeat(op, number=number, repeat=5)) / number * 1e9


def bench_publish(subscribers: int, number: int) -> float:
    asyncio.set_event_loop(asyncio.new_event_loop())  # Subscriber creates an asyncio.Event
    stats = Stats()
    registry = Registry(10_000)
    stream = registry.get("s42")
    subs = [Subscriber(_Writer(), 10**9, 65536, stats) for _ in range(subscribers)]  # type: ignore[arg-type]
    for sub in subs:
        stream.attach(sub, -1)
    line = RAW + b"\n"
    seq = 0

    def op() -> None:
        nonlocal seq
        seq += 1
        stream.publish(seq, line)
        if seq % 50_000 == 0:
            for sub in subs:
                sub._queue.clear()

    timeit.timeit(op, number=max(1, number // 10))
    result = min(timeit.repeat(op, number=number, repeat=5)) / number * 1e9
    for sub in subs:
        sub._queue.clear()
    return result


def main() -> None:
    results: dict[str, object] = {"parse_ns": round(bench_parse(), 1)}
    for n, number in ((10, 100_000), (100, 20_000), (1000, 2_000)):
        results[f"publish_{n}_subscribers_ns"] = round(bench_publish(n, number), 1)
    results["python"] = sys.version.split()[0]
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
