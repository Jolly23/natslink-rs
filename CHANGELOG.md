# Changelog

All notable changes to this crate are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the crate
adheres to [Semantic Versioning](https://semver.org/). Major and minor versions track
the Go module [`github.com/Jolly23/natslink`](https://github.com/Jolly23/natslink)
whose semantics this crate implements (`natslink::GO_REFERENCE_VERSION`).

## [Unreleased]

### Changed
- Document that a successful `publish` only queues the message: on tokio's
  multi-thread runtime the woken send task waits in the caller's worker LIFO
  slot until the caller yields, so callers doing synchronous work after a
  publish should `yield_now().await` first. Documentation only; behavior is
  unchanged.

## [1.8.4] - 2026-10-23

### Fixed
- Mirrored send tasks wait for client command capacity within a bounded send
  timeout instead of discarding accepted messages on a temporary full queue.
  Public `publish` remains non-blocking and each endpoint waits independently.
- Traffic statistics count actual payload admission and subscription reads
  instead of estimating payload bytes from protocol counters. Document the
  receive-buffer and direct-client boundaries imposed by `async-nats`.
- Reuse the normalized subject and a shared client snapshot on publish calls,
  removing wrapper allocations from full-queue rejection.
- URL authentication decodes percent-encoded userinfo exactly once, preserving
  literal plus signs and the distinction between encoded and separating colons.
  Error redaction covers encoded usernames and passwords, short credentials
  and quoted escape forms consistently with the Go implementation.
- `make itest` works with POSIX shells and preserves the test process's exit
  status while retaining its log.

### Added
- Shared Go/Rust contract fixtures and CI checks for deduplication, subscriber
  dispatch and redaction, plus validation of the Go reference metadata.

### Changed
- Document independent patch versions within a shared major/minor contract,
  the boundary between public non-blocking admission and internal send waits,
  and the two separate 5 s mirrored-stop phases.
- Track Go natslink v1.8.4 (`GO_REFERENCE_VERSION`); the cross-implementation
  contract CI runs against that release's commit.

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

### Changed
- Lockfile: rustls 0.23.46, rustls-webpki 0.103.16, webpki-roots 1.0.10
  (RUSTSEC-2026-0346/0347/0348/0349/0381/0382), tokio 1.54.1, tokio-util
  0.7.20, yoke-derive 0.8.4 (the 0.8.3 release was yanked).
- `fetch_update` call sites carry `#[allow(deprecated)]`: Rust 1.99 renamed it
  to `try_update`, which does not exist on the minimum supported 1.91 yet.

### Earlier behavior
See the Go module's CHANGELOG for the behavior introduced before v1.8.3.
