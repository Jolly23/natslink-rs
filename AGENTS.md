# natslink-rs: notes for contributors and coding agents

Read `README.md` first; it is the specification. `CONTRIBUTING.md` has the
test and release procedure. This file only lists what is easy to get wrong.

- **Two implementations, one behaviour.** This crate and the Go module
  `github.com/Jolly23/natslink` are the same design. A new feature or a
  semantic fix is done in both, each in the idiomatic, best-performing way for
  its runtime; the `README.md` table "Differences from the Go module" is the
  only place where they are allowed to differ, and every entry there needs a
  reason rooted in `nats.go` vs `async-nats`.
- **Scope.** One connection, one subject, self-supervising. The library owns
  pool failover, independent mirrored send queues and optional shared
  byte-level deduplication. Business routing, content-level deduplication and
  ordering across endpoints belong to the caller.
- **Constants are the product.** `DIAL_TIMEOUT`, `RECONNECT_WAIT*`,
  `PING_INTERVAL`, `REBUILD_BACKOFF*`, `STOP_GRACE`, `FLUSH_ON_STOP` and the
  mirror queue bounds are deliberately `pub(crate)` and documented at the top
  of `src/lib.rs`. Do not add options for them.
- **Log lines are grep targets.** Keep the `[natslink]` prefix and existing
  message texts and field keys; add new lines rather than renaming.
- **Credentials never reach an error or a log line.** URLs are split before
  they reach `async-nats` (`split_auth`), and every error that can carry a URL
  goes through `redact::redact_error`.
- **Public `publish` is non-blocking by contract.** A plain publisher makes
  one client enqueue attempt and returns `Error::BufferFull` if full. A
  mirrored pool admits to independent bounded endpoint queues; its internal
  send tasks may await client capacity within the fixed send timeout. Keep
  those waits out of the caller's path, and do not count a temporary full
  queue as a loss when the send task subsequently succeeds.
- **Tests.** `cargo test` must pass without a server. The full suite runs in
  CI against two docker NATS servers; `tests/rejection.rs` is opt-in and
  spawns its own container. Keep `testdata/contract-v1.json` identical to the
  Go repository and run `make conformance GO_CONFORMANCE_DIR=/path/to/natslink`
  after changing shared behavior. CI pins the other implementation by commit,
  independently of the published Go reference.
- **Versioning.** Bump `Cargo.toml` `version`, `CHANGELOG.md` and, when the
  Go reference moves, `GO_REFERENCE_VERSION` plus
  `package.metadata.go-reference` together; the release workflow refuses a
  tag that does not match. Major and minor versions match the Go reference;
  patch versions can advance independently. Run `make reference` to check the
  version and reference metadata before release.
