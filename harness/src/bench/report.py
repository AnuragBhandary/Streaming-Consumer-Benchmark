"""Turn ``results/`` into charts (``docs/charts/*.png``) and markdown tables
(``docs/tables.md``). Repetitions are aggregated by median; the spread (min-max) is kept.

    uv run --group report python -m bench.report
"""

from __future__ import annotations

import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[3]
RESULTS = ROOT / "results"
CHARTS = ROOT / "docs" / "charts"
IMPLS = ["java", "java-2disp", "python", "python-2proc"]
LABELS = {
    "java": "Java 21 virtual threads (1 dispatch thread)",
    "java-2disp": "Java 21 virtual threads (2 dispatch threads)",
    "python": "Python asyncio (1 process)",
    "python-2proc": "Python asyncio (2 processes)",
}
COLORS = {
    "java": "#d9480f",
    "java-2disp": "#862e9c",
    "python": "#1c7ed6",
    "python-2proc": "#37b24d",
}


def load(suite: str) -> list[dict[str, Any]]:
    return [json.loads(p.read_text()) for p in sorted((RESULTS / suite).glob("*.json"))]


def metrics(r: dict[str, Any]) -> dict[str, float]:
    lat = r["clients"]["latency_ms"]
    return {
        "p50": lat.get("p50", float("nan")),
        "p99": lat.get("p99", float("nan")),
        "p999": lat.get("p99.9", float("nan")),
        "throughput": r["clients"]["throughput_per_s"],
        "ratio": r["delivered_ratio"],
        "cpu": r["server"]["cpu_pct_mean"] or float("nan"),
        "mem": r["server"]["mem_mib_max"] or float("nan"),
        "harness_cpu": r.get("harness_cpu_pct_mean") or float("nan"),
        "dups": r["clients"]["duplicates"],
        "gaps": r["clients"]["gaps"],
        "disconnects": r["clients"]["server_disconnects"],
        "resets": r["clients"]["resets"],
        "complete": r["clients"]["complete"] / r["clients"]["clients"],
        "kafka_p99": r["clients"]["kafka_only_latency_ms"].get("p99", float("nan")),
    }


def aggregate(runs: list[dict[str, Any]], key: str) -> dict[str, dict[float, dict[str, Any]]]:
    """impl -> x value -> {metric: median, metric_min, metric_max}."""
    grouped: dict[str, dict[float, list[dict[str, float]]]] = defaultdict(lambda: defaultdict(list))
    for r in runs:
        grouped[r["run"]["impl"]][r["run"][key]].append(metrics(r))
    out: dict[str, dict[float, dict[str, Any]]] = {}
    for impl, by_x in grouped.items():
        out[impl] = {}
        for x, ms in sorted(by_x.items()):
            agg: dict[str, Any] = {"reps": len(ms)}
            for m in ms[0]:
                values = [v[m] for v in ms]
                agg[m] = statistics.median(values)
                agg[f"{m}_min"], agg[f"{m}_max"] = min(values), max(values)
            out[impl][x] = agg
    return out


def line_chart(
    data: dict[str, dict[float, dict[str, Any]]],
    metric: str,
    title: str,
    xlabel: str,
    ylabel: str,
    path: Path,
    xscale: str = "log",
    x_transform: Any = None,
    yscale: str = "linear",
) -> None:
    fig, ax = plt.subplots(figsize=(7, 4.2), dpi=150)
    for impl in IMPLS:
        if impl not in data:
            continue
        xs = sorted(data[impl])
        px = [x_transform(x) if x_transform else x for x in xs]
        ys = [data[impl][x][metric] for x in xs]
        lo = [data[impl][x][f"{metric}_min"] for x in xs]
        hi = [data[impl][x][f"{metric}_max"] for x in xs]
        ax.plot(px, ys, marker="o", label=LABELS[impl], color=COLORS[impl], linewidth=2)
        ax.fill_between(px, lo, hi, color=COLORS[impl], alpha=0.15, linewidth=0)
    ax.set_xscale(xscale)
    ax.set_yscale(yscale)
    ax.set_title(title, fontsize=11)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(True, which="both", alpha=0.3)
    ax.legend(frameon=False, fontsize=9)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def table(data: dict[str, dict[float, dict[str, Any]]], xname: str, x_fmt: Any = str) -> str:
    rows = [
        f"| {xname} | implementation | p50 ms | p99 ms | p99.9 ms | delivered/s | delivered % "
        "| CPU % (of 200) | memory MiB | dup / gap |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    xs = sorted({x for impl in data.values() for x in impl})
    for x in xs:
        for impl in IMPLS:
            a = data.get(impl, {}).get(x)
            if not a:
                continue
            rows.append(
                f"| {x_fmt(x)} | {LABELS[impl]} | {a['p50']:.1f} | {a['p99']:.1f} | {a['p999']:.1f} "
                f"| {a['throughput']:,.0f} | {a['ratio'] * 100:.1f} | {a['cpu']:.0f} | {a['mem']:.0f} "
                f"| {a['dups']:.0f} / {a['gaps']:.0f} |"
            )
    return "\n".join(rows)


def warmup_chart(runs: list[dict[str, Any]], path: Path) -> str:
    fig, ax = plt.subplots(figsize=(7, 4.2), dpi=150)
    lines = [
        "| implementation | p99, first second | p99, seconds 1-5 | p99, seconds 5-30 | p99 after 30 s |",
        "|---|---|---|---|---|",
    ]
    for impl in IMPLS:
        series = [r for r in runs if r["run"]["impl"] == impl]
        if not series:
            continue
        timeline = series[0]["clients"]["timeline"]
        xs = [t["second"] for t in timeline if t.get("count")]
        ys = [t["p99"] for t in timeline if t.get("count")]
        ax.plot(xs, ys, label=LABELS[impl], color=COLORS[impl], linewidth=1.5)

        def window(lo: int, hi: int, tl: list[dict[str, Any]] = timeline) -> float:
            values = [t["p99"] for t in tl if lo <= t["second"] < hi and t.get("count")]
            return statistics.median(values) if values else float("nan")

        lines.append(
            f"| {LABELS[impl]} | {window(0, 1):.1f} | {window(1, 5):.1f} | {window(5, 30):.1f} "
            f"| {window(30, 10**6):.1f} |"
        )
    ax.axvline(30, color="grey", linestyle="--", linewidth=1)
    ax.text(30.5, ax.get_ylim()[1] * 0.9, "end of discarded warm-up", fontsize=8, color="grey")
    ax.set_yscale("log")
    ax.set_title("Cold start: per-second p99 (5,000 connections, 50,000 deliveries/s)", fontsize=11)
    ax.set_xlabel("seconds since traffic started")
    ax.set_ylabel("p99 latency (ms, log)")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend(frameon=False, fontsize=9)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return "\n".join(lines)


def slow_table(runs: list[dict[str, Any]]) -> str:
    rows = [
        "| implementation | fast clients p50 / p99 ms | stalled clients p50 / p99 ms | server cut-offs per run | resets per run | CPU % | memory MiB | dup / gap | clients complete |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for impl in IMPLS:
        mine = [r for r in runs if r["run"]["impl"] == impl]
        if not mine:
            continue

        def med(fn: Any, rs: list[dict[str, Any]] = mine) -> float:
            return statistics.median(fn(r) for r in rs)

        def lat(section: str, key: str, rs: list[dict[str, Any]] = mine) -> float:
            return statistics.median(
                r["clients"].get(section, {}).get(key, float("nan")) for r in rs
            )

        rows.append(
            f"| {LABELS[impl]} | {lat('latency_ms', 'p50'):.1f} / {lat('latency_ms', 'p99'):.1f} "
            f"| {lat('stalled_clients_latency_ms', 'p50'):.1f} / {lat('stalled_clients_latency_ms', 'p99'):,.0f} "
            f"| {med(lambda r: r['clients']['server_disconnects']):.0f} "
            f"| {med(lambda r: r['clients']['resets']):.0f} "
            f"| {med(lambda r: r['server']['cpu_pct_mean']):.0f} | {med(lambda r: r['server']['mem_mib_max']):.0f} "
            f"| {sum(r['clients']['duplicates'] for r in mine)} / {sum(r['clients']['gaps'] for r in mine)} "
            f"| {med(lambda r: r['clients']['complete'] / r['clients']['clients']) * 100:.0f}% |"
        )
    return "\n".join(rows)


WHATIF_LABELS = {
    "": "baseline: 1 GiB, 1 dispatch thread, 2 carriers",
    "mem1g-gclog": "same, with GC logging (pauses: 71, 0.39 s total, max 19 ms)",
    "mem2g-gclog": "2 GiB memory limit (pauses: 59, 0.41 s total)",
    "carriers4": "4 carrier threads (-Djdk.virtualThreadScheduler.parallelism=4)",
    "threadprobe": "repeat, sampling per-thread CPU (one carrier ~81%, the other ~8%)",
    "jfr": "repeat under JFR profiling (observer effect: timing perturbed)",
    "consumers2": "2 dispatch threads (FANOUT_CONSUMERS=2): carriers ~52% / ~52%",
}


def whatif_table(runs: list[dict[str, Any]], baseline: list[dict[str, Any]]) -> str:
    rows = [
        "| Java, 20,000 connections, 200,000 deliveries/s | p50 ms | p99 ms | CPU % (of 200) | memory MiB |",
        "|---|---|---|---|---|",
    ]
    base = [b for b in baseline if b["run"]["impl"] == "java" and b["run"]["conns"] == 20000][:1]
    for r in base + sorted(runs, key=lambda r: r["run"].get("variant", "")):
        m = metrics(r)
        label = WHATIF_LABELS.get(r["run"].get("variant", ""), r["run"].get("variant", ""))
        rows.append(
            f"| {label} | {m['p50']:.1f} | {m['p99']:.1f} | {m['cpu']:.0f} | {m['mem']:.0f} |"
        )
    return "\n".join(rows)


def main() -> None:
    CHARTS.mkdir(parents=True, exist_ok=True)
    sections: list[str] = [
        "# Generated tables\n\nProduced by `python -m bench.report` from `results/`.\n"
    ]

    scaling = load("scaling")
    if scaling:
        data = aggregate(scaling, "conns")
        for metric, ylabel, name, yscale in (
            ("p99", "p99 latency (ms)", "scaling_p99.png", "log"),
            ("p50", "p50 latency (ms)", "scaling_p50.png", "log"),
            ("cpu", "server CPU (% of one core; limit 200)", "scaling_cpu.png", "linear"),
            ("mem", "server memory (MiB)", "scaling_memory.png", "linear"),
        ):
            line_chart(
                data, metric, f"Scaling connections at 1,000 events/s: {ylabel}",
                "concurrent connections", ylabel, CHARTS / name, yscale=yscale,
            )  # fmt: skip
        sections.append(
            "## Scaling (1,000 events/s, 100 streams; deliveries/s = 10 x connections)\n"
        )
        sections.append(table(data, "connections", lambda x: f"{int(x):,}"))

    saturation = load("saturation")
    if saturation:
        data = aggregate(saturation, "rate")
        offered = lambda rate: rate * 2000 / 100  # noqa: E731
        line_chart(
            data, "p99", "Saturation at 2,000 connections: p99 latency", "offered deliveries/s",
            "p99 latency (ms)", CHARTS / "saturation_p99.png", xscale="linear",
            x_transform=offered, yscale="log",
        )  # fmt: skip
        line_chart(
            data, "throughput", "Saturation at 2,000 connections: throughput",
            "offered deliveries/s", "delivered per second", CHARTS / "saturation_throughput.png",
            xscale="linear", x_transform=offered,
        )  # fmt: skip
        line_chart(
            data, "cpu", "Saturation at 2,000 connections: server CPU", "offered deliveries/s",
            "CPU (% of one core; limit 200)", CHARTS / "saturation_cpu.png", xscale="linear",
            x_transform=offered,
        )  # fmt: skip
        sections.append("\n## Saturation (2,000 connections, 100 streams)\n")
        sections.append(table(data, "offered deliveries/s", lambda r: f"{int(offered(r)):,}"))

    whatif = load("whatif")
    if whatif:
        sections.append("\n## Java at 20,000 connections: what-if runs\n")
        sections.append(whatif_table(whatif, scaling))

    slow_os = load("slow-os-buffers")
    if slow_os:
        sections.append(
            "\n## Slow consumers, OS-default socket buffers (5% of clients stall 10 s every 30 s)\n"
        )
        sections.append(slow_table(slow_os))

    slow = load("slow")
    if slow:
        sections.append(
            "\n## Slow consumers, SO_SNDBUF capped at 64 KiB (5% of clients stall 20 s every 30 s)\n"
        )
        sections.append(slow_table(slow))

    warmup = load("warmup")
    if warmup:
        sections.append("\n## Cold start (no discarded window)\n")
        sections.append(warmup_chart(warmup, CHARTS / "warmup_timeline.png"))

    (ROOT / "docs" / "tables.md").write_text("\n".join(sections) + "\n")
    print("wrote docs/tables.md and", len(list(CHARTS.glob("*.png"))), "charts")


if __name__ == "__main__":
    main()
