# Architecture

## Responsibility

Donation engine, ledger, NATS consumer, cron owner, and migration runner.

This repository is independently versioned and deployed. It may import `github.com/frederickmarvel/inflora-shared` after the shared library is released, but it must never import another service repository. Ports and connections are fixed by the almanac wire guide.

Phase 2 implements the workspace's single PostgreSQL migration runner in `cmd/migrate`. Migrations are embedded, transactional, checksum-verified, and serialized with a session advisory lock. Down migrations are gated outside development and test environments. HTTP/gRPC handlers, repositories, messaging, and business workflows remain deferred to Phase 6.
