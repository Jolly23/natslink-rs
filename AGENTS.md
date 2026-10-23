# natslink-rs: notes for contributors and coding agents

Read `README.md` first; it is the specification. `CONTRIBUTING.md` has the
test and release procedure. This file only lists what is easy to get wrong.

- **Two implementations, one behaviour.** This crate and the Go module
  `github.com/Jolly23/natslink` are the same design. A new feature or a
  semantic fix is done in both, each in the idiomatic, best-performing way for
  its runtime; the `README.md` table "Differences from the Go module" is the
  only place where they are allowed to differ, and every entry there needs a
  reason rooted in `nats.go` vs `async-nats`.
- **Scope.** One connection, one subject, self-supervising. No business logic.
  Cross-connection state (routing, content-level deduplication, ordering
  across endpoints) belongs to the caller; `DedupSet` only collapses
  byte-identical payloads.
- **Constants are the product.** `DIAL_TIMEOUT`, `RECONNECT_WAIT*`,
  `PING_INTERVAL`, `REBUILD_BACKOFF*`, `STOP_GRACE`, `FLUSH_ON_STOP` and the
  mirror queue bounds are deliberately `pub(crate)` and documented at the top
  of `src/lib.rs`. Do not add options for them.
- **Log lines are grep targets.** Keep the `[natslink]` prefix and existing
  message texts and field keys; add new lines rather than renaming.
- **Credentials never reach an error or a log line.** URLs are split before
  they reach `async-nats` (`split_auth`), and every error that can carry a URL
  goes through `redact::redact_error`.
- **`publish` is non-blocking by contract.** It makes one enqueue attempt and
  returns `Error::BufferFull`; never introduce an `await` on the client's
  command queue in that path.
- **Tests.** `cargo test` must pass without a server. The full suite runs in
  CI against two docker NATS servers; `tests/rejection.rs` is opt-in and
  spawns its own container.
- **Versioning.** Bump `Cargo.toml` `version`, `CHANGELOG.md` and, when the
  Go reference moves, `GO_REFERENCE_VERSION` plus
  `package.metadata.go-reference` together; the release workflow refuses a
  tag that does not match.
