# ReliQ local benchmark

All numbers below were measured in three sequential trials per worker count on the same machine and runtime.

| Resource                  | Measured configuration                                                                                                                                                               |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| CPU                       | Intel Core i7-12650H, 10 physical cores, 16 logical processors                                                                                                                       |
| Host RAM                  | 16,849,879,040 bytes reported by Windows                                                                                                                                             |
| Host OS                   | Windows 11 Home Single Language 25H2, build 26200.9974                                                                                                                               |
| Guest OS                  | Ubuntu 26.04.1 LTS, WSL 2 mode, WSL package 3.0.5.0, kernel 6.18.58.1-microsoft-standard-WSL2                                                                                        |
| Docker Engine and Compose | Engine 29.8.1, Compose v5.5.1                                                                                                                                                        |
| Docker resource limits    | Daemon sees 16 logical CPUs and 8,164,069,376 bytes of RAM. Redis and PostgreSQL have no explicit CPU quota, CPU set, RAM cap, or swap cap. WSL reports 2,147,483,648 bytes of swap. |
| Runtime and services      | Node v20.20.2 in WSL, Redis 7.4.12 with AOF `everysec`, PostgreSQL 16.16 running in Compose                                                                                          |

## Workload

Each trial submits 2,000 immediate normal-priority jobs to a fresh queue in batches of up to 32. Each job runs the real `sleep` handler with a 5 ms timer. One job runs at a time per worker process. Workers connect before timing starts. The driver bypasses HTTP and the PostgreSQL audit writers, so the run measures the Redis queue and worker path. PostgreSQL was available in Compose but did not serve the workload. Trials ran in order `1, 2, 5, 10` for each of three rounds, with a one-second pause after each trial.

Throughput is completed jobs divided by elapsed time from the first submission until the producer receives the last completion event over IPC. Latency is enqueue to successful ack using Redis millisecond timestamps. Percentiles use nearest rank over the 2,000 latencies, so they include time waiting for a worker. All throughputs below are truncated to three decimals. The [raw trial output](benchmark-runs.ndjson) preserves the original precision.

## Individual trials

| Workers | Trial | Jobs run | Lost | Duplicate handler runs | Jobs/sec | p50 (ms) | p95 (ms) | p99 (ms) |
| ------: | ----: | -------: | ---: | ---------------------: | -------: | -------: | -------: | -------: |
|       1 |     1 |    2,000 |    0 |                      0 |  138.808 |    7,301 |   13,622 |   14,158 |
|       1 |     2 |    2,000 |    0 |                      0 |  141.396 |    7,064 |   13,333 |   13,892 |
|       1 |     3 |    2,000 |    0 |                      0 |  144.429 |    6,591 |   13,012 |   13,591 |
|       2 |     1 |    2,000 |    0 |                      0 |  301.954 |    3,161 |    6,152 |    6,435 |
|       2 |     2 |    2,000 |    0 |                      0 |  286.146 |    3,436 |    6,542 |    6,819 |
|       2 |     3 |    2,000 |    0 |                      0 |  290.152 |    3,366 |    6,450 |    6,719 |
|       5 |     1 |    2,000 |    0 |                      0 |  780.937 |    1,242 |    2,333 |    2,429 |
|       5 |     2 |    2,000 |    0 |                      0 |  776.885 |    1,251 |    2,351 |    2,446 |
|       5 |     3 |    2,000 |    0 |                      0 |  782.562 |    1,231 |    2,329 |    2,424 |
|      10 |     1 |    2,000 |    0 |                      0 | 1494.843 |      621 |    1,147 |    1,193 |
|      10 |     2 |    2,000 |    0 |                      0 | 1499.019 |      608 |    1,136 |    1,184 |
|      10 |     3 |    2,000 |    0 |                      0 | 1500.186 |      618 |    1,143 |    1,190 |

## Observed spread across trials

| Workers |    Jobs/sec range | p50 range (ms) | p95 range (ms) | p99 range (ms) |
| ------: | ----------------: | -------------: | -------------: | -------------: |
|       1 |   138.808–144.429 |    6,591–7,301 |  13,012–13,622 |  13,591–14,158 |
|       2 |   286.146–301.954 |    3,161–3,436 |    6,152–6,542 |    6,435–6,819 |
|       5 |   776.885–782.562 |    1,231–1,251 |    2,329–2,351 |    2,424–2,446 |
|      10 | 1494.843–1500.186 |        608–621 |    1,136–1,147 |    1,184–1,193 |

These trials use an I/O-style timer payload and include the burst backlog. CPU-heavy and transactional handlers have different limits. The [chaos summaries](../chaos-results/) record separate transfer workloads and count actual committed side effects.
