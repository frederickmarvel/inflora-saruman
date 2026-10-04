# inflora-saruman

Donation engine, ledger, NATS consumer, cron owner, and migration runner.

Canonical port(s): 7002, 8082. Phase 2 provides the single shared PostgreSQL migration runner; Saruman service logic begins in Phase 6.

```sh
make tidy
make lint
make test
make build
make run
make migrate       # schema only
make migrate-seed  # schema + deterministic dev data (ENV=dev/test only)
make migrate-status
make migrate-down  # ENV=dev/test or ALLOW_DOWN_MIGRATIONS=true
```

The runner uses `DB_DSN`, defaults to the workspace development database, embeds paired SQL files, serializes concurrent runs with a PostgreSQL advisory lock, and verifies applied migration checksums. `migrations/00001_init.up.sql` is an exact copy of the canonical v2 schema.

Source of truth: `almanac/planning/WIRE_GUIDE.md` and `almanac/planning/schemas/`.
