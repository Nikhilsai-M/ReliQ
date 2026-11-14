# ReliQ

ReliQ is a Redis-backed job queue with lease recovery and a PostgreSQL dead-letter archive. See [the design](docs/DESIGN.md) for the state machine and durability guarantees.

## Limitations

- Recovery runs only while at least one worker for the affected queue is online. When all workers stop, expired leases and due delayed jobs wait for a worker to resume maintenance.
