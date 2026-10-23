# Changelog

All notable changes to this crate are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the crate
adheres to [Semantic Versioning](https://semver.org/). Version numbers track
the Go module [`github.com/Jolly23/natslink`](https://github.com/Jolly23/natslink)
whose semantics this crate implements (`natslink::GO_REFERENCE_VERSION`).

## [Unreleased]

## [1.8.3] - 2026-10-22

First public release. Rust port of Go natslink v1.8.3 on `async-nats` 0.50.

### Added
- `probe` / `must_probe` / `must_probe_each`: fail-fast startup checks with
  credentials redacted from every error.
- `Subscriber`: one connection, one subject; automatic reconnect, rebuild
  after a permanent close with re-subscribe, three dispatch modes
  (`Workers::Pool`, `Workers::Sync`, `Workers::PerMessage`), optional `gate`,
  optional byte-level `dedup` (`DedupSet`, shareable across subscribers),
  runtime `pause` / `resume` with partial-failure self-healing, probe-free
  recovery of a server-rejected SUB on a rebuild schedule.
- `Publisher` / `PublisherPool`: non-blocking `publish` (`Error::BufferFull`
  when the outbound queue is full), round-robin with health-first failover.
- Mirroring: `Subscriber::new_mirrored`, `Publisher::new_mirrored` (at most 5
  endpoints), `PublisherPool::new_mirrored` (at most 64). A mirrored pool
  runs one bounded send task per endpoint (4096 messages / 16 MiB); a single
  URL degrades every mirrored constructor to its plain counterpart.
- `Stats` with the same field names as the Go module, `endpoint_stats()` on
  mirrored subscribers, `[natslink]` status line via `tracing`.
- Re-export of `async_nats`, `Message` and `Bytes` so hosts depend on one
  client version only.

### Earlier history
Versions 1.5.x to 1.8.2 of this crate were maintained inside a private
workspace and track the corresponding Go releases; see the Go module's
CHANGELOG for the behaviour each version introduced.
