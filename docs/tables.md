# Generated tables

Produced by `python -m bench.report` from `results/`.

## Scaling (1,000 events/s, 100 streams; deliveries/s = 10 x connections)

| connections | implementation | p50 ms | p99 ms | p99.9 ms | delivered/s | delivered % | CPU % (of 200) | memory MiB | dup / gap |
|---|---|---|---|---|---|---|---|---|---|
| 100 | Java 21 virtual threads (1 dispatch thread) | 2.0 | 3.9 | 13.5 | 1,000 | 100.0 | 11 | 196 | 0 / 0 |
| 100 | Java 21 virtual threads (2 dispatch threads) | 2.2 | 3.8 | 16.1 | 1,000 | 100.0 | 12 | 199 | 0 / 0 |
| 100 | Python asyncio (1 process) | 2.4 | 4.0 | 13.8 | 1,000 | 100.0 | 22 | 47 | 0 / 0 |
| 100 | Python asyncio (2 processes) | 2.3 | 4.7 | 22.6 | 1,000 | 100.0 | 39 | 126 | 0 / 0 |
| 1,000 | Java 21 virtual threads (1 dispatch thread) | 2.0 | 4.0 | 14.1 | 10,000 | 100.0 | 24 | 220 | 0 / 0 |
| 1,000 | Java 21 virtual threads (2 dispatch threads) | 2.3 | 4.0 | 17.5 | 10,000 | 100.0 | 29 | 219 | 0 / 0 |
| 1,000 | Python asyncio (1 process) | 2.4 | 4.3 | 15.5 | 10,000 | 100.0 | 31 | 56 | 0 / 0 |
| 1,000 | Python asyncio (2 processes) | 2.1 | 4.6 | 19.3 | 10,000 | 100.0 | 49 | 133 | 0 / 0 |
| 5,000 | Java 21 virtual threads (1 dispatch thread) | 2.1 | 4.5 | 17.5 | 50,000 | 100.0 | 59 | 337 | 0 / 0 |
| 5,000 | Java 21 virtual threads (2 dispatch threads) | 2.2 | 4.9 | 17.1 | 50,000 | 100.0 | 63 | 344 | 0 / 0 |
| 5,000 | Python asyncio (1 process) | 2.3 | 10.0 | 39.3 | 49,999 | 100.0 | 59 | 96 | 0 / 0 |
| 5,000 | Python asyncio (2 processes) | 2.2 | 5.4 | 22.1 | 50,001 | 100.0 | 81 | 173 | 0 / 0 |
| 10,000 | Java 21 virtual threads (1 dispatch thread) | 2.1 | 7.9 | 21.3 | 100,000 | 100.0 | 92 | 468 | 0 / 0 |
| 10,000 | Java 21 virtual threads (2 dispatch threads) | 2.3 | 8.1 | 23.5 | 100,000 | 100.0 | 96 | 496 | 0 / 0 |
| 10,000 | Python asyncio (1 process) | 145.4 | 253.1 | 279.4 | 99,926 | 99.9 | 102 | 159 | 0 / 0 |
| 10,000 | Python asyncio (2 processes) | 2.5 | 18.2 | 40.9 | 100,001 | 100.0 | 122 | 224 | 0 / 0 |
| 20,000 | Java 21 virtual threads (1 dispatch thread) | 86.9 | 188.1 | 211.8 | 199,986 | 100.0 | 114 | 766 | 0 / 0 |
| 20,000 | Java 21 virtual threads (2 dispatch threads) | 2.5 | 67.2 | 103.8 | 200,004 | 100.0 | 160 | 752 | 0 / 0 |
| 20,000 | Python asyncio (1 process) | 383.6 | 604.9 | 667.9 | 199,975 | 100.0 | 102 | 282 | 0 / 0 |
| 20,000 | Python asyncio (2 processes) | 184.4 | 327.4 | 354.4 | 199,941 | 100.0 | 206 | 353 | 0 / 0 |

## Saturation (2,000 connections, 100 streams)

| offered deliveries/s | implementation | p50 ms | p99 ms | p99.9 ms | delivered/s | delivered % | CPU % (of 200) | memory MiB | dup / gap |
|---|---|---|---|---|---|---|---|---|---|
| 40,000 | Java 21 virtual threads (1 dispatch thread) | 2.1 | 4.5 | 18.5 | 40,000 | 100.0 | 50 | 293 | 0 / 0 |
| 40,000 | Java 21 virtual threads (2 dispatch threads) | 2.3 | 5.8 | 24.5 | 40,000 | 100.0 | 52 | 292 | 0 / 0 |
| 40,000 | Python asyncio (1 process) | 2.4 | 5.0 | 20.1 | 40,000 | 100.0 | 48 | 94 | 0 / 0 |
| 40,000 | Python asyncio (2 processes) | 2.4 | 5.2 | 21.7 | 39,999 | 100.0 | 70 | 199 | 0 / 0 |
| 80,000 | Java 21 virtual threads (1 dispatch thread) | 2.1 | 6.1 | 24.0 | 79,998 | 100.0 | 66 | 372 | 0 / 0 |
| 80,000 | Java 21 virtual threads (2 dispatch threads) | 2.2 | 7.3 | 26.5 | 80,001 | 100.0 | 74 | 373 | 0 / 0 |
| 80,000 | Python asyncio (1 process) | 2.5 | 16.5 | 32.9 | 79,987 | 100.0 | 71 | 149 | 0 / 0 |
| 80,000 | Python asyncio (2 processes) | 2.4 | 8.6 | 32.3 | 80,001 | 100.0 | 90 | 309 | 0 / 0 |
| 160,000 | Java 21 virtual threads (1 dispatch thread) | 2.1 | 11.8 | 27.6 | 159,999 | 100.0 | 106 | 542 | 0 / 0 |
| 160,000 | Java 21 virtual threads (2 dispatch threads) | 2.1 | 12.0 | 31.0 | 160,002 | 100.0 | 113 | 532 | 0 / 0 |
| 160,000 | Python asyncio (1 process) | 20.5 | 45.2 | 58.5 | 160,016 | 100.0 | 102 | 264 | 0 / 0 |
| 160,000 | Python asyncio (2 processes) | 2.5 | 18.2 | 34.9 | 160,003 | 100.0 | 143 | 531 | 0 / 0 |
| 240,000 | Java 21 virtual threads (1 dispatch thread) | 2.1 | 15.5 | 30.4 | 240,005 | 100.0 | 135 | 707 | 0 / 0 |
| 240,000 | Java 21 virtual threads (2 dispatch threads) | 2.2 | 20.5 | 37.1 | 240,005 | 100.0 | 158 | 686 | 0 / 0 |
| 240,000 | Python asyncio (1 process) | 22.1 | 50.9 | 62.0 | 240,004 | 100.0 | 102 | 347 | 0 / 0 |
| 240,000 | Python asyncio (2 processes) | 4.9 | 29.2 | 47.0 | 240,020 | 100.0 | 196 | 700 | 0 / 0 |
| 320,000 | Java 21 virtual threads (1 dispatch thread) | 3.0 | 24.9 | 40.9 | 320,034 | 100.0 | 173 | 740 | 0 / 0 |
| 320,000 | Java 21 virtual threads (2 dispatch threads) | 2.8 | 28.1 | 43.4 | 319,998 | 100.0 | 202 | 764 | 0 / 0 |
| 320,000 | Python asyncio (1 process) | 23.1 | 57.3 | 74.1 | 320,083 | 100.0 | 101 | 348 | 0 / 0 |
| 320,000 | Python asyncio (2 processes) | 12.5 | 160.5 | 243.3 | 319,762 | 99.9 | 198 | 710 | 0 / 0 |

## Java at 20,000 connections: what-if runs

| Java, 20,000 connections, 200,000 deliveries/s | p50 ms | p99 ms | CPU % (of 200) | memory MiB |
|---|---|---|---|---|
| baseline: 1 GiB, 1 dispatch thread, 2 carriers | 88.6 | 191.8 | 114 | 766 |
| 4 carrier threads (-Djdk.virtualThreadScheduler.parallelism=4) | 97.8 | 203.6 | 115 | 769 |
| 2 dispatch threads (FANOUT_CONSUMERS=2): carriers ~52% / ~52% | 2.3 | 62.0 | 161 | 745 |
| repeat under JFR profiling (observer effect: timing perturbed) | 4.2 | 184.4 | 139 | 738 |
| same, with GC logging (pauses: 71, 0.39 s total, max 19 ms) | 86.9 | 180.8 | 115 | 735 |
| 2 GiB memory limit (pauses: 59, 0.41 s total) | 56.2 | 157.4 | 123 | 756 |
| repeat, sampling per-thread CPU (one carrier ~81%, the other ~8%) | 94.0 | 195.7 | 115 | 769 |

## Slow consumers, OS-default socket buffers (5% of clients stall 10 s every 30 s)

| implementation | fast clients p50 / p99 ms | stalled clients p50 / p99 ms | server cut-offs per run | resets per run | CPU % | memory MiB | dup / gap | clients complete |
|---|---|---|---|---|---|---|---|---|
| Java 21 virtual threads (1 dispatch thread) | 2.1 / 4.7 | 2.3 / 9,677 | 0 | 0 | 66 | 257 | 0 / 0 | 100% |
| Python asyncio (1 process) | 2.8 / 13.5 | 3.0 / 9,677 | 0 | 0 | 80 | 87 | 0 / 0 | 100% |
| Python asyncio (2 processes) | 2.3 / 6.8 | 2.4 / 9,677 | 0 | 0 | 95 | 190 | 0 / 0 | 100% |

## Slow consumers, SO_SNDBUF capped at 64 KiB (5% of clients stall 20 s every 30 s)

| implementation | fast clients p50 / p99 ms | stalled clients p50 / p99 ms | server cut-offs per run | resets per run | CPU % | memory MiB | dup / gap | clients complete |
|---|---|---|---|---|---|---|---|---|
| Java 21 virtual threads (1 dispatch thread) | 2.2 / 4.8 | 2.8 / 19,739 | 102 | 0 | 66 | 261 | 0 / 0 | 100% |
| Java 21 virtual threads (2 dispatch threads) | 2.2 / 6.0 | 2.8 / 19,739 | 102 | 0 | 70 | 262 | 0 / 0 | 100% |
| Python asyncio (1 process) | 2.8 / 13.1 | 3.8 / 19,739 | 56 | 0 | 81 | 99 | 0 / 0 | 100% |
| Python asyncio (2 processes) | 2.3 / 7.8 | 2.9 / 19,739 | 57 | 0 | 96 | 202 | 0 / 0 | 100% |

## Cold start (no discarded window)

| implementation | p99, first second | p99, seconds 1-5 | p99, seconds 5-30 | p99 after 30 s |
|---|---|---|---|---|
| Java 21 virtual threads (1 dispatch thread) | 14.1 | 3.2 | 4.3 | 3.8 |
| Java 21 virtual threads (2 dispatch threads) | 17.8 | 3.4 | 4.0 | 4.2 |
| Python asyncio (1 process) | 5.1 | 4.8 | 3.6 | 4.5 |
| Python asyncio (2 processes) | 3.2 | 3.7 | 4.5 | 4.4 |
