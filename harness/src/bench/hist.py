"""Mergeable latency histogram with ~2% relative precision (log-spaced buckets).

Percentiles cannot be averaged across processes, but bucket counts can be summed, so every
process records into one of these and the orchestrator merges them exactly.
"""

from __future__ import annotations

import math
from collections import Counter

_BASE_MS = 0.01
_GROWTH = 1.02
_LOG_GROWTH = math.log(_GROWTH)


def bucket_of(ms: float) -> int:
    return int(math.log(max(ms, _BASE_MS) / _BASE_MS) / _LOG_GROWTH)


def upper_bound_ms(bucket: int) -> float:
    return _BASE_MS * _GROWTH ** (bucket + 1)


class Histogram:
    __slots__ = ("counts",)

    def __init__(self, counts: dict[int, int] | None = None) -> None:
        self.counts: Counter[int] = Counter(counts or {})

    def record(self, ms: float) -> None:
        self.counts[bucket_of(ms)] += 1

    def merge(self, other: Histogram | dict[int, int] | dict[str, int]) -> Histogram:
        items = other.counts.items() if isinstance(other, Histogram) else other.items()
        for key, value in items:
            self.counts[int(key)] += value
        return self

    @property
    def total(self) -> int:
        return sum(self.counts.values())

    def percentile(self, p: float) -> float:
        total = self.total
        if not total:
            return math.nan
        target = max(1, math.ceil(total * p / 100))
        running = 0
        for key in sorted(self.counts):
            running += self.counts[key]
            if running >= target:
                return upper_bound_ms(key)
        raise AssertionError("unreachable")

    def summary(self) -> dict[str, float]:
        if not self.total:
            return {"count": 0}
        return {
            "count": self.total,
            "p50": round(self.percentile(50), 2),
            "p90": round(self.percentile(90), 2),
            "p99": round(self.percentile(99), 2),
            "p99.9": round(self.percentile(99.9), 2),
            "max": round(upper_bound_ms(max(self.counts)), 2),
        }

    def to_json(self) -> dict[str, int]:
        return {str(k): v for k, v in self.counts.items()}
