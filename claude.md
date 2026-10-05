# AI Repository Guide: inflora-saruman

## Purpose

`inflora-saruman` is the stateful core of the active Inflora backend. It owns the donation engine, financial ledger behavior, NATS consumers, scheduled jobs, email workflows, internal gRPC/admin HTTP surfaces, and the shared PostgreSQL migration runner. Canonical ports are `7002` and `8082`.

This is a high-risk financial repository. Correctness, idempotency, transaction ordering, and reversible schema evolution take priority over convenience.

## Important paths

- `cmd/server/`: service composition.
- `cmd/migrate/`: embedded migration runner.
- `migrations/`: canonical PostgreSQL schema and development seed migrations.
- `internal/ledger/`: ledger posting and balance rules.
- `internal/service/`: donation and financial workflows.
- `internal/subscriber/`: event consumers and replay handling.
- `internal/cron/`: scheduled state transitions.
- `internal/repo/`: PostgreSQL persistence.
- `internal/migrate/`: migration discovery, locking, checksums, and lifecycle tests.

## Contract and correctness rules

- Read `/Users/frederickmarvel/Inflora/almanac/planning/WIRE_GUIDE.md` and `almanac/planning/schemas/` before changing contracts or ownership.
- Money is integer IDR. Ledger-affecting operations must remain balanced, atomic, append-only where specified, and idempotent.
- Commit durable state before publishing dependent events; prefer the shared outbox/inbox patterns.
- Migrations require paired `*.up.sql` and `*.down.sql` files, deterministic ordering, safe upgrade/downgrade behavior, and updated discovery tests.
- Legacy rows must be considered when adding constraints. Do not assume a new field is populated unless a backfill or compatibility rule guarantees it.
- Donation pricing/display snapshots are immutable after intent creation; later settings changes must not alter existing intents.
- Shared events and RPC messages belong in `inflora-shared`.

## Commands

```sh
make tidy
make lint
make test
make build
make run
make migrate
make migrate-seed
make migrate-status
make migrate-down
```

Run `go test ./...`, `go vet ./...`, and `git diff --check`. PostgreSQL lifecycle tests require `TEST_DATABASE_URL`; if it is absent, state that the database integration coverage was skipped rather than treating the run as full validation.
