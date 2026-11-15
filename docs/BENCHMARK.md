# ReliQ local benchmark

Each benchmark series ran three sequential trials per worker count on the same machine. The Node v24.22.3 series and Redis process SIGKILL rerun were recorded on November 15, 2026 (IST).

| Resource                    | Measured configuration                                                                                                                                             |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| CPU                         | Intel Core i7-12650H, 10 physical cores, 16 logical processors                                                                                                     |
| Host RAM                    | 16,849,879,040 bytes reported by Windows                                                                                                                           |
| Host OS                     | Windows 11 Home Single Language 25H2, build 26200.9974                                                                                                             |
| Guest OS                    | Ubuntu 26.04.1 LTS, WSL 2 mode, WSL package 3.0.5.0, kernel 6.18.58.1-microsoft-standard-WSL2                                                                      |
| Docker Engine and Compose   | Engine 29.8.1, Compose v5.5.1                                                                                                                                      |
| Node v20.20.2 original runs | Docker reported 16 logical CPUs and 8,164,069,376 bytes of RAM. Neither container had an explicit CPU, RAM, or swap cap. WSL reported 2,147,483,648 bytes of swap. |
| Node v24.22.3 rerun         | Docker reported 16 logical CPUs and 8,164,073,472 bytes of RAM. Neither container had an explicit CPU, RAM, or swap cap. WSL reported 2,147,483,648 bytes of swap. |
| Services for both series    | Redis 7.4.12 with AOF `everysec` and PostgreSQL 16.16 running in Compose.                                                                                          |

## Workload

Each trial submits 2,000 immediate normal-priority jobs to a fresh queue in batches of up to 32. Each job runs the real `sleep` handler with a 5 ms timer. One job runs at a time per worker process. Workers connect before timing starts. The driver bypasses HTTP and the PostgreSQL audit writers, so the run measures the Redis queue and worker path. PostgreSQL was available in Compose but did not serve the workload. Trials ran in order `1, 2, 5, 10` for each of three rounds, with a one-second pause after each trial.

Throughput is completed jobs divided by elapsed time from the first submission until the producer receives the last completion event over IPC. Latency is enqueue to successful ack using Redis millisecond timestamps. Percentiles use nearest rank over the 2,000 latencies, so they include time waiting for a worker. The original Node v20.20.2 tables retain their three-decimal truncation. The Node v24.22.3 tables use the exact values emitted by the benchmark. Raw output is available for [Node v20.20.2](benchmark-runs.ndjson) and [Node v24.22.3](benchmark-runs-node24.ndjson).

## Individual trials

| Node.js  | Workers | Trial | Jobs run | Lost | Duplicate handler runs |           Jobs/sec | p50 (ms) | p95 (ms) | p99 (ms) |
| -------- | ------: | ----: | -------: | ---: | ---------------------: | -----------------: | -------: | -------: | -------: |
| v20.20.2 |       1 |     1 |    2,000 |    0 |                      0 |            138.808 |    7,301 |   13,622 |   14,158 |
| v20.20.2 |       1 |     2 |    2,000 |    0 |                      0 |            141.396 |    7,064 |   13,333 |   13,892 |
| v20.20.2 |       1 |     3 |    2,000 |    0 |                      0 |            144.429 |    6,591 |   13,012 |   13,591 |
| v20.20.2 |       2 |     1 |    2,000 |    0 |                      0 |            301.954 |    3,161 |    6,152 |    6,435 |
| v20.20.2 |       2 |     2 |    2,000 |    0 |                      0 |            286.146 |    3,436 |    6,542 |    6,819 |
| v20.20.2 |       2 |     3 |    2,000 |    0 |                      0 |            290.152 |    3,366 |    6,450 |    6,719 |
| v20.20.2 |       5 |     1 |    2,000 |    0 |                      0 |            780.937 |    1,242 |    2,333 |    2,429 |
| v20.20.2 |       5 |     2 |    2,000 |    0 |                      0 |            776.885 |    1,251 |    2,351 |    2,446 |
| v20.20.2 |       5 |     3 |    2,000 |    0 |                      0 |            782.562 |    1,231 |    2,329 |    2,424 |
| v20.20.2 |      10 |     1 |    2,000 |    0 |                      0 |           1494.843 |      621 |    1,147 |    1,193 |
| v20.20.2 |      10 |     2 |    2,000 |    0 |                      0 |           1499.019 |      608 |    1,136 |    1,184 |
| v20.20.2 |      10 |     3 |    2,000 |    0 |                      0 |           1500.186 |      618 |    1,143 |    1,190 |
| v24.22.3 |       1 |     1 |    2,000 |    0 |                      0 |  141.1305225679291 |    7,015 |   13,366 |   13,913 |
| v24.22.3 |       1 |     2 |    2,000 |    0 |                      0 | 139.20394918708402 |    7,127 |   13,555 |   14,121 |
| v24.22.3 |       1 |     3 |    2,000 |    0 |                      0 | 141.25918324776077 |    7,098 |   13,335 |   13,912 |
| v24.22.3 |       2 |     1 |    2,000 |    0 |                      0 |  293.6798764764466 |    3,376 |    6,380 |    6,651 |
| v24.22.3 |       2 |     2 |    2,000 |    0 |                      0 |  290.4667017146887 |    3,390 |    6,450 |    6,726 |
| v24.22.3 |       2 |     3 |    2,000 |    0 |                      0 |  291.3599111036204 |    3,364 |    6,435 |    6,711 |
| v24.22.3 |       5 |     1 |    2,000 |    0 |                      0 |  802.4967207355814 |    1,214 |    2,271 |    2,363 |
| v24.22.3 |       5 |     2 |    2,000 |    0 |                      0 |   799.169307618876 |    1,217 |    2,288 |    2,382 |
| v24.22.3 |       5 |     3 |    2,000 |    0 |                      0 |  800.2099955882181 |    1,228 |    2,286 |    2,380 |
| v24.22.3 |      10 |     1 |    2,000 |    0 |                      0 |  1440.573245582927 |      664 |    1,201 |    1,247 |
| v24.22.3 |      10 |     2 |    2,000 |    0 |                      0 | 1490.1529264076175 |      638 |    1,157 |    1,204 |
| v24.22.3 |      10 |     3 |    2,000 |    0 |                      0 | 1524.5580824087706 |      617 |    1,132 |    1,178 |

## Observed spread across trials

| Node.js  | Workers |                        Jobs/sec range | p50 range (ms) | p95 range (ms) | p99 range (ms) |
| -------- | ------: | ------------------------------------: | -------------: | -------------: | -------------: |
| v20.20.2 |       1 |                       138.808–144.429 |    6,591–7,301 |  13,012–13,622 |  13,591–14,158 |
| v20.20.2 |       2 |                       286.146–301.954 |    3,161–3,436 |    6,152–6,542 |    6,435–6,819 |
| v20.20.2 |       5 |                       776.885–782.562 |    1,231–1,251 |    2,329–2,351 |    2,424–2,446 |
| v20.20.2 |      10 |                     1494.843–1500.186 |        608–621 |    1,136–1,147 |    1,184–1,193 |
| v24.22.3 |       1 | 139.20394918708402–141.25918324776077 |    7,015–7,127 |  13,335–13,555 |  13,912–14,121 |
| v24.22.3 |       2 |   290.4667017146887–293.6798764764466 |    3,364–3,390 |    6,380–6,450 |    6,651–6,726 |
| v24.22.3 |       5 |    799.169307618876–802.4967207355814 |    1,214–1,228 |    2,271–2,288 |    2,363–2,382 |
| v24.22.3 |      10 |  1440.573245582927–1524.5580824087706 |        617–664 |    1,132–1,201 |    1,178–1,247 |

These trials use an I/O-style timer payload and include the burst backlog. CPU-heavy and transactional handlers have different limits. The [chaos summaries](../chaos-results/) record separate transfer workloads and count actual committed side effects.

## Redis process SIGKILL

These runs use the transfer workload, seed `20261115`, and five recovery workers. A job counts as lost if its enqueue was acknowledged and the run never observes both queue completion and a committed receipt. Redis process SIGKILL leaves the host running, so these results do not cover host power loss or fsync failure.

| Node.js  | Run                                                                                   |     Seed | Workers | Acknowledged | Completed | Lost | Handler executions | Repeat executions | Committed effects | Duplicate effects |
| -------- | ------------------------------------------------------------------------------------- | -------: | ------: | -----------: | --------: | ---: | -----------------: | ----------------: | ----------------: | ----------------: |
| v20.20.2 | [Original](../chaos-results/hard-20261115-reliq-chaos-e5b6163d3d0c/summary.json)      | 20261115 |       5 |       10,000 |    10,000 |    0 |             10,006 |                 6 |            30,000 |                 0 |
| v20.20.2 | [Traced rerun](../chaos-results/hard-20261115-reliq-chaos-07de23409221/summary.json)  | 20261115 |       5 |       10,000 |    10,000 |    0 |             10,006 |                 6 |            30,000 |                 0 |
| v24.22.3 | [Node 24 rerun](../chaos-results/hard-20261115-reliq-chaos-b1eddfdd1728/summary.json) | 20261115 |       5 |       10,000 |    10,000 |    0 |             10,006 |                 6 |            30,000 |                 0 |
