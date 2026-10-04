# inflora-saruman

Donation engine, ledger, NATS consumer, cron owner, and migration runner.

Canonical port(s): 7002, 8082. Phase 0 contains a compileable placeholder only; implementation begins in Phase 6 of the backend build plan.

```sh
make tidy
make lint
make test
make build
make run
```

Source of truth: `almanac/planning/WIRE_GUIDE.md` and `almanac/planning/schemas/`.
