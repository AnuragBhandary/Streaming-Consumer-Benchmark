"""Experiment runner (runs on the host; drives docker).

For every run: recreate the server container (a cold process, so JVM warm-up is part of every
run and is then discarded), start the client swarm and the producer as containers on the compose
network, discard the warm-up window, measure, sample the server's CPU and memory, then check that
every client received every event.

    uv run python -m bench.run --suite scaling --reps 2
    uv run python -m bench.run --suite smoke
"""

from __future__ import annotations

import argparse
import asyncio
import json
import socket
import statistics
import subprocess
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

NETWORK = "streaming-consumer-benchmark_default"
SERVERS = {
    "java": ("java-server", 7002),
    "python": ("python-server", 7001),
    "python-2proc": ("python2-server", 7003),
}
RESULTS = Path(__file__).resolve().parents[3] / "results"


@dataclass(frozen=True)
class Run:
    suite: str
    impl: str
    conns: int
    rate: float  # events/s
    streams: int = 100
    warmup_s: float = 30
    measure_s: float = 60
    stall_fraction: float = 0.0
    payload_bytes: int = 200
    client_procs: int = 6
    timeline: bool = False
    rep: int = 1

    @property
    def name(self) -> str:
        stall = f"-stall{int(self.stall_fraction * 100)}" if self.stall_fraction else ""
        return f"{self.impl}-c{self.conns}-r{int(self.rate)}{stall}-rep{self.rep}"


def suites(reps: int) -> dict[str, list[Run]]:
    impls = list(SERVERS)
    return {
        "smoke": [
            Run("smoke", i, 200, 200, warmup_s=5, measure_s=10, client_procs=2) for i in impls
        ],
        # Short run at the heaviest saturation point: checks the harness keeps up.
        "probe": [Run("probe", i, 2000, 8000, warmup_s=10, measure_s=15) for i in impls],
        # Fixed event rate, growing connection count (deliveries/s = rate * conns / streams).
        "scaling": [
            Run("scaling", i, c, 1000, rep=r)
            for r in range(1, reps + 1)
            for c in (100, 1000, 5000, 10000, 20000)
            for i in impls
        ],
        # Fixed connections, growing event rate until the servers saturate.
        "saturation": [
            Run("saturation", i, 2000, rate, rep=r)
            for r in range(1, reps + 1)
            for rate in (2000, 4000, 8000, 12000, 16000)
            for i in impls
        ],
        # Cold start with no discarded window: per-second p99 shows JIT warm-up (and that the
        # 30 s discarded in every other suite covers it).
        "warmup": [
            Run("warmup", i, 5000, 1000, warmup_s=0, measure_s=90, timeline=True, rep=r)
            for r in range(1, reps + 1)
            for i in impls
        ],
        # 5% of clients stop reading for 5 s every 15 s.
        "slow": [
            Run("slow", i, 1000, 500, stall_fraction=0.05, rep=r)
            for r in range(1, reps + 1)
            for i in impls
        ],
    }


def sh(*argv: str, check: bool = True) -> str:
    return subprocess.run(argv, capture_output=True, text=True, check=check).stdout


def server_stats(port: int) -> dict[str, Any]:
    with socket.create_connection(("localhost", port), timeout=5) as sock:
        sock.sendall(b"STATS\n")
        data = b""
        while not data.endswith(b"\n"):
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
    return json.loads(data)


def wait_ready(port: int, timeout: float = 120) -> None:
    deadline = time.monotonic() + timeout
    streak = 0
    while time.monotonic() < deadline:
        try:
            streak = streak + 1 if server_stats(port)["ready"] else 0
        except (OSError, ValueError):
            streak = 0
        if streak >= 6:  # several processes may answer (python-2proc): all must be ready
            return
        time.sleep(0.5)
    raise TimeoutError("server not ready")


class Container:
    def __init__(self, name: str, argv: list[str], cpus: str) -> None:
        self.argv = [
            "docker", "run", "-i", "--rm", "--name", name, "--network", NETWORK, "--cpus", cpus,
            "--ulimit", "nofile=65536:65536", "fanout-harness:latest", *argv,
        ]  # fmt: skip
        self.proc: asyncio.subprocess.Process | None = None

    async def start(self) -> None:
        self.proc = await asyncio.create_subprocess_exec(
            *self.argv, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, limit=2**26
        )

    async def expect(self, tag: str, timeout: float) -> Any:
        assert self.proc and self.proc.stdout
        while True:
            line = await asyncio.wait_for(self.proc.stdout.readline(), timeout)
            if not line:
                raise RuntimeError(f"{self.argv[5]} exited before {tag}")
            text = line.decode()
            if text.startswith(tag + " "):
                return json.loads(text[len(tag) + 1 :])

    def send(self, line: str) -> None:
        assert self.proc and self.proc.stdin
        self.proc.stdin.write(line.encode() + b"\n")


async def sample_resources(
    container: str, stop: asyncio.Event, out: list[dict[str, float]]
) -> None:
    while not stop.is_set():
        raw = await asyncio.to_thread(
            sh,
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.CPUPerc}} {{.MemUsage}}",
            container,
        )
        try:
            cpu, mem = raw.split(" ", 1)
            used = mem.split("/")[0].strip()
            value, unit = float(used[:-3]), used[-3:]
            mib = value * {"KiB": 1 / 1024, "MiB": 1, "GiB": 1024}[unit]
            out.append({"cpu_pct": float(cpu.rstrip("%")), "mem_mib": mib})
        except (ValueError, KeyError):
            pass


async def execute(run: Run) -> dict[str, Any]:
    service, port = SERVERS[run.impl]
    # Only the server under test runs: idle servers would still consume Kafka and use CPU.
    others = [svc for svc, _ in SERVERS.values() if svc != service]
    await asyncio.to_thread(sh, "docker", "compose", "stop", *others)
    await asyncio.to_thread(
        sh, "docker", "compose", "up", "-d", "--force-recreate", "--no-deps", service
    )
    await asyncio.to_thread(wait_ready, port)
    container = sh("docker", "compose", "ps", "-q", service).strip()

    clients = Container(
        "bench-clients",
        ["bench.clients", "--server", f"{service}:7000", "--conns", str(run.conns),
         "--streams", str(run.streams), "--procs", str(run.client_procs),
         "--stall-fraction", str(run.stall_fraction),
         *(["--timeline"] if run.timeline else [])],
        cpus="6",
    )  # fmt: skip
    await clients.start()
    ready = await clients.expect("READY", 300)
    duration = run.warmup_s + run.measure_s + 5
    producer = Container(
        "bench-producer",
        ["bench.producer", "--streams", str(run.streams), "--rate", str(run.rate),
         "--duration", str(duration), "--payload-bytes", str(run.payload_bytes)],
        cpus="1",
    )  # fmt: skip
    await producer.start()
    clients.send(f"START {run.warmup_s} {run.measure_s}")
    samples: list[dict[str, float]] = []
    harness_samples: list[dict[str, float]] = []
    stop = asyncio.Event()
    await asyncio.sleep(run.warmup_s)
    samplers = [
        asyncio.create_task(sample_resources(container, stop, samples)),
        asyncio.create_task(sample_resources("bench-clients", stop, harness_samples)),
    ]
    await asyncio.sleep(run.measure_s)
    stop.set()
    await asyncio.gather(*samplers)
    produced = await producer.expect("RESULT", duration + 120)
    clients.send("FINAL " + json.dumps(produced["final"]))
    result = await clients.expect("RESULT", 300)
    server = await asyncio.to_thread(server_stats, port)
    for c in (clients, producer):
        if c.proc:
            await c.proc.wait()

    expected_per_s = run.rate * run.conns / run.streams
    cpu = [s["cpu_pct"] for s in samples]
    mem = [s["mem_mib"] for s in samples]
    hist = result.pop("hist")
    return {
        "run": asdict(run) | {"name": run.name},
        "timestamp": datetime.now(UTC).isoformat(),
        "clients_connected": ready["clients"],
        "producer": produced | {"final": len(produced["final"])},
        "expected_deliveries_per_s": expected_per_s,
        "delivered_ratio": round(result["throughput_per_s"] / expected_per_s, 4),
        "clients": result,
        "server": {
            "cpu_pct_mean": round(statistics.fmean(cpu), 1) if cpu else None,
            "cpu_pct_max": max(cpu) if cpu else None,
            "mem_mib_max": round(max(mem), 1) if mem else None,
            "stats": server,
        },
        # The client swarm has 6 CPUs (600%). Near that, the harness itself is the bottleneck
        # and the run says more about the harness than about the server.
        "harness_cpu_pct_mean": (
            round(statistics.fmean(h["cpu_pct"] for h in harness_samples), 1)
            if harness_samples
            else None
        ),
        "hist": hist,
    }


async def main_async(args: argparse.Namespace) -> None:
    runs = suites(args.reps)[args.suite]
    if args.only:
        runs = [r for r in runs if r.impl in args.only.split(",")]
    out_dir = RESULTS / args.suite
    out_dir.mkdir(parents=True, exist_ok=True)
    for i, run in enumerate(runs, 1):
        path = out_dir / f"{run.name}.json"
        if path.exists() and not args.force:
            print(f"[{i}/{len(runs)}] {run.name}: exists, skipping", flush=True)
            continue
        print(f"[{i}/{len(runs)}] {run.name} ...", flush=True)
        try:
            result = await execute(run)
        except Exception as exc:
            print(f"  FAILED: {exc!r}", flush=True)
            sh("docker", "rm", "-f", "bench-clients", "bench-producer", check=False)
            continue
        path.write_text(json.dumps(result, indent=1))
        c, s = result["clients"], result["server"]
        print(
            f"  p50 {c['latency_ms'].get('p50')} ms  p99 {c['latency_ms'].get('p99')} ms  "
            f"thr {c['throughput_per_s']}/s ({result['delivered_ratio']:.0%})  "
            f"cpu {s['cpu_pct_mean']}%  mem {s['mem_mib_max']} MiB  "
            f"harness {result['harness_cpu_pct_mean']}%  "
            f"complete {c['complete']}/{c['clients']}  dup {c['duplicates']} gap {c['gaps']}  "
            f"disconnects {c['server_disconnects']}",
            flush=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--suite",
        choices=["smoke", "probe", "scaling", "saturation", "slow", "warmup"],
        required=True,
    )
    parser.add_argument("--reps", type=int, default=1)
    parser.add_argument("--only", help="comma-separated impls, e.g. java,python")
    parser.add_argument("--force", action="store_true", help="re-run even if a result exists")
    asyncio.run(main_async(parser.parse_args()))


if __name__ == "__main__":
    main()
