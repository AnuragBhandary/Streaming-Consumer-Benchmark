from __future__ import annotations

import math
import random

from bench.hist import Histogram, bucket_of, upper_bound_ms


def test_percentiles_within_two_percent() -> None:
    rng = random.Random(1)
    values = [rng.lognormvariate(1, 1) for _ in range(20_000)]
    hist = Histogram()
    for v in values:
        hist.record(v)
    values.sort()
    for p in (50, 90, 99):
        exact = values[math.ceil(len(values) * p / 100) - 1]
        assert abs(hist.percentile(p) - exact) / exact < 0.03


def test_merge_equals_single_histogram() -> None:
    a, b, both = Histogram(), Histogram(), Histogram()
    for i in range(1, 1000):
        (a if i % 2 else b).record(i / 10)
        both.record(i / 10)
    merged = Histogram().merge(a).merge(b.to_json())
    assert merged.counts == both.counts
    assert merged.summary() == both.summary()


def test_edges() -> None:
    assert math.isnan(Histogram().percentile(50))
    assert Histogram().summary() == {"count": 0}
    assert bucket_of(0) == bucket_of(-5) == 0  # clamped to the smallest bucket
    assert upper_bound_ms(bucket_of(5.0)) >= 5.0
