# ReliQ

ReliQ is a Redis-backed job queue with lease recovery and a PostgreSQL dead-letter archive. See [the design](docs/DESIGN.md) for the state machine and durability guarantees.

## Local API and CLI

Use Node 20.19 or later in the Node 20 series and Docker Compose.

```bash
test -f .env || cp .env.example .env
docker compose up -d --wait
npm ci
npm run build
npm run db:migrate
npm run api
```

To run the API in Compose, use `docker compose --profile api up -d --build --wait` in place of the host API command. Compose probes `GET /health`, which returns `200` when Redis and PostgreSQL respond and `503` otherwise. Set `API_QUEUES` to a comma-separated list of API-visible queues. The default is `default`.

In a second terminal, run `npm run worker` to consume jobs in `QUEUE_NAME`. In another terminal, try the CLI against the API:

```bash
printf '%s\n' '{"type":"sleep","payload":{"durationMs":1000}}' > /tmp/reliq-job.json
npm run cli -- enqueue default --file /tmp/reliq-job.json
npm run cli -- stats default
npm run cli -- in-flight default --limit 20
npm run cli -- dlq default --offset 0 --limit 20
```

Every route starts with `/queues/:queue`. Send mutation bodies as `application/json`.

| Method and suffix      | Body or query                                                                                | Success                                                        |
| ---------------------- | -------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| `POST /jobs`           | `type`, `payload`, optional `id`, `priority`, `maxAttempts`, and either `runAt` or `delayMs` | `201` with job ID and schedule                                 |
| `GET /stats`           |                                                                                              | `200` with live depth by state                                 |
| `GET /in-flight`       | `offset`, `limit`                                                                            | `200` with worker IDs and lease time remaining in milliseconds |
| `GET /dlq`             | `offset`, `limit`                                                                            | `200` with the newest dead letters first                       |
| `POST /dlq/:id/replay` | `{ "replayCount": 0 }`                                                                       | `200` with the next generation                                 |
| `POST /dlq/replay`     | `{ "jobs": [{ "id": "<uuid>", "replayCount": 0 }] }`                                         | `200` with one result per selection                            |
| `DELETE /dlq/:id`      | `{ "replayCount": 0 }`                                                                       | `204`                                                          |

Use the ID and `replayCount` returned by DLQ inspection for replay or purge. Run `npm run cli -- --help` for those CLI commands. Single-operation errors use `{ "error": { "code": "…", "message": "…", "details": [] } }`. Bulk replay returns the same error fields on individual results, and the CLI exits nonzero when any item fails.

## Transfer demo and crash tests

The `transfer` handler accepts `debitAccountId`, `creditAccountId`, and `amountCents`. Each account must exist in PostgreSQL. The demo records debit, credit, and receipt as three separate idempotent transactions. A receipt is a database row, representing a side effect within the same database.

Run the crash harness from Linux or WSL with Docker Compose and Node 20. It creates disposable Compose projects with their own ports and volumes. Each scenario processes 10,000 acknowledged jobs using five recovery workers. The harness kills one worker immediately after a committed debit, then restarts or hard-kills that scenario’s Redis container during queue processing.

```bash
test -f .env || cp .env.example .env
npm ci
npm run build
docker compose up -d --wait redis postgres
npm run test:mutations
npm run chaos -- graceful 20261115 10000
npm run chaos -- hard 20261115 10000
npm run chaos -- continuous 20261115 10000
npm run test:idempotency-mutation -- 20261115 100
```

`hard` runs the **Redis process SIGKILL** scenario. It does not simulate host power loss or fsync failure. `continuous` keeps sending jobs during a graceful Redis restart and counts failed attempts, retries, and replies reconciled by job ID.

Each run writes one small `summary.json` under `chaos-results/`. Git ignores the larger acknowledgment, attempt, and event journals. A job counts as lost if enqueue was acknowledged but the run never observed completion. Completion requires a successful queue transition and a committed receipt. The graceful scenarios assert zero losses. All scenarios check for duplicate side effects. [Crash test design](docs/DESIGN.md#crash-measurement)

## Limitations

- Recovery runs only while at least one worker for the affected queue is online. When all workers stop, expired leases and due delayed jobs wait for a worker to resume maintenance.
- The operator API has no authentication and is for localhost use only. Compose publishes the port on `127.0.0.1`. Depth covers the four indexed live states. Succeeded jobs retain a terminal hash.
- Purge keeps a permanent Redis ID reservation. These reservations grow without bound. One option is a bounded deduplication window with expiring reservations and server-assigned, non-reusable IDs so an expired reservation cannot let an old ID collide with archive or idempotency history.
- DLQ and in-flight pages can shift when other clients change the queue between reads.
