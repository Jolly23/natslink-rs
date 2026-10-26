# natslink-rs

[![CI](https://github.com/Jolly23/natslink-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/Jolly23/natslink-rs/actions/workflows/ci.yml)

Self-supervising NATS connections for Rust: **one connection, one subject, zero babysitting.**

`natslink-rs` is the Rust port of the Go module
[`github.com/Jolly23/natslink`](https://github.com/Jolly23/natslink) on top of
[`async-nats`](https://github.com/nats-io/nats.rs). Both implementations share
one design: a unit owns a single connection bound to a single subject and
takes care of dialing, exponential-backoff reconnects, rebuilding the
connection after a permanent close, re-subscribing, bounded back-pressure and
periodic status reporting. You write the handler; the library keeps the pipe
alive. The Go README's mental model and failure model also apply here. This
README covers the Rust API, the shared mirroring behavior and the differences
in buffering, lifecycle and statistics imposed by `async-nats`.

- **`Subscriber`** - one connection subscribing to one subject. Three dispatch
  modes (bounded worker pool, synchronous, task-per-message), an optional
  `gate` to drop messages at the door, an optional byte-level `dedup`, and
  `pause` / `resume` to withdraw interest without dropping the connection.
- **`Publisher`** - one connection publishing to one subject. `publish` is
  synchronous and non-blocking: a full outbound queue returns
  `Error::BufferFull` instead of awaiting.
- **`PublisherPool`** - N publishers with round-robin and health-first
  failover. Mirrored pools run one independent send task per endpoint, so a
  stalled endpoint never delays the others.
- **Mirroring** - `Subscriber::new_mirrored`, `Publisher::new_mirrored` and
  `PublisherPool::new_mirrored` fan the same subject out to M *independent*
  NATS endpoints. Publishes go to every endpoint; subscribers receive every
  copy, or exactly one when you opt in to the built-in **`DedupSet`**
  (byte-identical copies collapse before decoding, first arrival wins). The
  return types are the plain `PublisherPool` / `Subscriber`, so the rest of
  your code does not change.

Dependencies: `async-nats` 0.50 (`default-features = false`, `ring` TLS),
`tokio`, `tokio-util`, `tracing`, `bytes`, `futures`, `thiserror`, `rand` and
`rapidhash`. The crate re-exports `async_nats`, `Message` and `Bytes` so a
host never has to declare its own `async-nats` (two versions in one dependency
graph would make the types incompatible).

## Install

The crate is distributed as a git dependency pinned by tag:

```toml
[dependencies]
natslink = { git = "https://github.com/Jolly23/natslink-rs", tag = "v1.8.4" }
```

Requires Rust 1.91 or newer. `natslink::VERSION` is the crate version and
`natslink::GO_REFERENCE_VERSION` the Go release it matches; both are printed in
the `[natslink] starting` log line.

## Mental model (read this first)

1. **One connection = one subject = one self-supervising unit.** Need more
   subjects? Create more units. Failures stay isolated.
2. **Failures fall into four classes, each with one owner:**

   | Phase   | Class | Surface | What you do |
   |---------|-------|---------|-------------|
   | Startup | **Environment** (bad address, no route, bad credentials) | `probe` returns `Err` | `must_probe(url).await` - panic, fail fast |
   | Startup | **Configuration** (malformed URL or subject) | `new` / `start` return `Err` | `must_start().await` - panic, fail fast |
   | Runtime | **Ordinary disconnect** (server restart, flaky network, stalled peer) | automatic reconnect | nothing; it never gives up |
   | Runtime | **Permanent close** (`Event::Closed`, `MaxReconnects`, a subscription stream ending) | automatic rebuild | nothing; it never gives up |

   Startup errors must crash you. Runtime failures must never bother you.
   A successful start means supervision is running; the server may still be
   unreachable and retried in the background. Use `wait_connected` when startup
   must wait for a connection. Connection readiness is not a delivery acknowledgement.
3. **Instances are single-use.** After `stop()` create a new one.
4. **Supervision parameters are not configurable.** Dial timeout 5 s,
   reconnect backoff 500 ms doubling to 5 s with +50 % jitter, rebuild backoff
   2 s doubling to 30 s, heartbeat 3 s, stop grace 5 s, flush-on-stop bound
   1 s. They are constants at the top of `src/lib.rs`.
5. **Back-pressure is always visible.** A full worker queue drops and counts
   (`dropped`), a full client receive buffer is dropped by `async-nats` and
   counted (`slow_consumers`), a full outbound queue returns
   `Error::BufferFull`. The one exception is the explicit
   `Workers::PerMessage` mode (unbounded concurrency, no back-pressure signal).

## Five-minute start

```rust
use natslink::{handler, must_probe, Options, Publisher, PublisherOptions, ReportInterval,
               Subscriber, SubscriberOptions, Workers};

let url = "nats://TOKEN@127.0.0.1:4222".to_string();
must_probe(&url).await;                       // environment errors panic within 5 s, credentials redacted

let sub = Subscriber::new(SubscriberOptions {
    workers: Workers::Pool(8),                // or Workers::Sync / Workers::PerMessage
    ..SubscriberOptions::new(
        Options { url: url.clone(), topic: "events.block".into(), name: "edge-sub-block".into(), ..Default::default() },
        handler(|msg| async move { /* msg.payload is Bytes and may be kept after the callback returns */ }),
    )
})?;
sub.must_start().await;                       // started != connected: an unreachable server is retried in the background

let publ = Publisher::new(PublisherOptions {
    options: Options { url, topic: "events.tx".into(), ..Default::default() },
    max_payload: 8 << 20,
    queue_capacity: 16,                       // outbound queue counts messages; keep it small for large payloads
})?;
publ.must_start().await;
publ.publish(bytes::Bytes::from_static(b"..."))?;   // synchronous, non-blocking: Err(BufferFull) when the queue is full

// at any time: sub.stats() / sub.is_connected() / sub.pause().await / sub.resume().await
sub.stop().await;
publ.stop().await;
```

`Options.report_interval`: `ReportInterval::Default` (one `[natslink] status`
line per minute), `Off` (the host aggregates `stats()` itself) or `Every(d)`.

## Failure model

**At startup, `must_probe` is the only correct check.** `start` / `must_start`
never fail on an unreachable server or a bad token: `async-nats` keeps
retrying in the background forever, which is what you want at runtime but
means `must_start` cannot catch environment mistakes. One probe per URL, then
start as many connections as you like. `must_probe_each` probes several URLs
in sequence; a mirrored deployment with M endpoints is M promises, so every
endpoint is probed.

**At runtime there are two layers, both retrying forever:**

1. **Ordinary disconnect** → `async-nats` reconnects with the crate's backoff
   (500 ms doubling to 5 s, +50 % jitter; the very first dial waits 0).
   Subscriptions are restored by the client.
2. **Permanent close** (`Event::Closed`, `ClientError::MaxReconnects`, or a
   subscription stream that ended unexpectedly) → the unit rebuilds the whole
   connection: redial, re-subscribe, swap in. Backoff 2 s doubling to 30 s.
   Late events from the previous connection are filtered by connection
   generation, so one close never triggers two rebuilds.

A silently dead connection is caught by the 3 s heartbeat after two
consecutive misses. A status line is logged every minute (`report_interval`)
and a `degraded` warning while disconnected, so even an unknown failure mode is
never silent.

## Subscribing

| `workers` | Behaviour | Use when |
| --- | --- | --- |
| `Pool(0)` (= 8) / `Pool(n)` | n tokio tasks behind a bounded queue (`queue_size`, default 4096); a full queue drops the new message and counts `dropped` | the default: bounded concurrency, visible overload |
| `Sync` | the callback is awaited on the delivery task | lowest latency; the callback must be very light or `slow_consumers` rises |
| `PerMessage` | `tokio::spawn` per message | mostly light messages with a few heavy ones that must not be dropped; **no back-pressure signal** |

Delivery order is Gate → Dedup → dispatch; a mirrored subscriber runs it once
per endpoint copy.

- **`gate: Option<Gate>`** - a cheap `Fn() -> bool` evaluated on the delivery
  task; `false` drops the message and counts `gated`. Use it when the host is
  still catching up on state and cannot act on messages yet.
- **`dedup: Option<Arc<DedupSet>>`** - `DedupSet::new(entries, window)` is a
  FIFO set of 64-bit `rapidhash` values of the payload (`entries` 0 = 4096;
  `window` 0 = evict by capacity only; a hit does not refresh). Byte-identical
  payloads already in the set are dropped and counted `deduped`. One set can
  be shared by several subscribers, so identical payloads on different
  subjects collapse too. `dedup_key` supplies a precomputed key; returning
  `None` bypasses the set. Empty payloads are never recorded. The hash is not
  cryptographic and collisions are possible: this is a transport-level filter,
  never a substitute for content-level deduplication in the host.
- **`pending_msg_limit`** - per-subscription client receive buffer
  (0 = 65536); `async-nats` has no byte limit.
- **`pause().await` / `resume().await`** - withdraw and restore the SUB while
  keeping the connection, heartbeats and reconnects. The server withdraws
  interest upstream, so paused streams stop costing bandwidth (a gate only
  saves the callback). Idempotent; `Error::Stopped` after `stop`. A
  connection rebuilt while paused does not subscribe. If `resume` fails on
  some endpoints the pause state is still cleared and the error returned; a
  single-flight healer retries those endpoints on the rebuild schedule
  (`stats().diverged > 0` until it converges), so the caller never retries.
  A server-rejected SUB (permissions violation on the subject) is retried the
  same way on a 2-30 s backoff; it is detected from the client's error event,
  so detection follows the event, not `start`.

Control-plane waits (`pause`, `resume`, setup of a rebuilt connection, the
healer) hold the shared state lock across the `subscribe` enqueue into the
client's command queue; while a server is offline one endpoint can delay the
others' control-plane calls. `stop` cancels that wait. Data-plane delivery is
never behind that lock.

## Publishing

`Publisher::new(PublisherOptions { options, max_payload, queue_capacity })`.
`publish(Bytes)` makes exactly one non-blocking enqueue attempt
(`now_or_never` on the client's `publish`): success means the message is in
the outbound queue, a full queue returns `Error::BufferFull` and counts
`dropped`, an oversized payload returns `Error::Oversized`. The caller
is never parked on NATS back-pressure. `queue_capacity` counts messages, not
bytes (default 1024); choose a small value for large payloads so a stalled
server cannot pin memory.

**`Ok` means queued, not written.** The socket write happens in the client's
connection task (or, with mirroring, the endpoint's send task), which the
enqueue wakes. On tokio's multi-thread runtime a task woken from a worker goes
into that worker's LIFO slot, which other workers do not steal, so it runs only
after the calling task reaches an `.await` that returns `Pending`. A caller
that publishes and then does synchronous work (decoding, hashing, bookkeeping,
draining already-buffered input) delays the actual send by that much, even
with idle workers. When send latency matters, yield right after publishing,
before the heavy work:

```rust,ignore
pool.publish(payload)?;
tokio::task::yield_now().await; // let the woken send task write first
expensive_bookkeeping();
```

A caller that returns to an `.await` immediately (for example a loop that
publishes and then waits on its input channel) needs nothing extra. The Go
module has no equivalent caveat: idle Ps in the Go scheduler can steal a
just-readied goroutine from another P's `runnext` slot after a few
microseconds.

`PublisherPool::new(n, opts)` keeps `n` publishers and round-robins between
them, trying connected members first and disconnected ones only as a last
resort. `dropped()` counts messages that no member could send.

## Mirroring (active-active across independent NATS endpoints)

`Subscriber::new_mirrored(urls, opts)`, `Publisher::new_mirrored(urls, opts)`
(at most 5 endpoints) and `PublisherPool::new_mirrored(urls, n, opts)` (at most
64) mirror one subject across M mutually independent NATS endpoints. Each
publish goes to every endpoint, each subscriber subscribes on every endpoint
and the handler sees every copy, or exactly one with a shared `DedupSet`.
Mirroring is only a constructor: the returned types are the plain
`PublisherPool` / `Subscriber`.

- **Not the same as a comma-separated URL.** A comma list inside one `url` is
  ordered failover within one endpoint (`retain_servers_order`; one server
  connected at a time, the next tried after a disconnect). `async-nats` sends
  one set of credentials to every server in the list, so all entries must
  carry identical userinfo (or none); differing credentials are rejected at
  `probe` / `start` with `Error::Config` so a token is never sent to a server
  it was not written for. The mirror `urls` are active-active across
  endpoints; the two compose.
- **Every endpoint is an independent send line.** With two or more endpoints
  each gets a bounded queue (4096 messages and 16 MiB, whichever fills
  first) and a tokio send task. `publish` reserves a slot and the bytes per
  endpoint, shares the immutable `Bytes` with every queue (no copy), returns
  `Ok` as soon as one endpoint accepted and `Error::QueueFull` when none
  could. A stalled endpoint backs up and drops only its own copies
  (`queue_dropped` on that endpoint); the pool-level `dropped()` is settled
  asynchronously and only counts messages that every endpoint failed to send.
  Each send task waits for capacity in its own client's command queue within
  a fixed 5 s send timeout. A temporary full command queue therefore keeps
  the message pending instead of discarding it immediately. That wait never
  blocks the public `publish` call or another endpoint's send task.
- **Stop** stops accepting first, drains all send tasks in parallel within one
  5 s grace, then closes the member connections in parallel within another
  5 s. Concurrent `stop` calls wait for the same sequence. Accepted into a
  queue is not the same as acknowledged by the server.
- **A single URL degrades** every mirrored constructor to its plain
  counterpart: same naming, no send queue, synchronous `publish`.
- **Startup** runs the subscriber's workers before any endpoint is dialled
  and dials at most 8 connections concurrently; a configuration failure
  rolls back every member that succeeded, late finishers included. `start` is
  single-shot per instance.

## Statistics

`stats()` uses the Go module's field names: `name / running /
connected / status / server / reconnects / rebuilds / in_msgs / out_msgs /
in_bytes / out_bytes / dropped / queue_dropped / deduped / paused / pauses /
diverged / gated / slow_consumers / queue_len / queue_cap`; counters accumulate
across rebuilds. Traffic counters are measured by natslink, with these boundaries:

- `out_msgs` / `out_bytes` count successful admission to the client's command
  queue, with payload bytes only. They do not confirm server receipt.
- `in_msgs` / `in_bytes` count messages obtained by the subscription reader,
  before Gate and Dedup. Bytes include payload and encoded headers, excluding
  subject and reply names. Messages discarded inside the client's full
  subscription buffer never reach this reader and are not included;
  `slow_consumers` reports client overflow events, not a lost-message count.
- Direct publishes and subscriptions created through `client()` bypass
  wrapper accounting. Messages delivered to a natslink subscription still
  count at its reader. The underlying client's own statistics remain
  available from that handle.

A mirrored subscriber reports `connected` only when every
endpoint is connected, sums the counters and counts `dropped / gated /
deduped` per copy; `endpoint_stats()` splits them per endpoint. For a mirrored
pool, `queue_dropped` is reported per endpoint queue: aggregate it once per
endpoint, not once per member. `diverged` counts endpoints whose subscription
does not match the pause state yet (a failed `resume`, a pending rejected-SUB
recovery); it is 0 while paused.

## Logging

Logs go through `tracing`; every message starts with `[natslink]` and uses
the same field keys as the Go module (`client`, `topic`, `server`, `status`,
`reconnects`, `rebuilds`, `dropped`, `deduped`, `queue_dropped`, ...), so one
set of grep patterns covers both implementations. Credentials never reach a
log line: the URL is split before it reaches `async-nats` and every error that
can carry a URL goes through `redact::redact_error`.

## Differences from the Go module (read before porting host code)

| Topic | Go (`nats.go`) | Rust (`async-nats` 0.50) |
| --- | --- | --- |
| Runtime | goroutines, synchronous API | tokio; lifecycle is `async fn`, `publish` is synchronous and non-blocking; the handler is `Fn(Message) -> BoxFuture` (wrap a closure with `natslink::handler`) |
| Receive buffer | `PendingMsgLimit` / `PendingBytesLimit` | `pending_msg_limit` only; no byte limit in `async-nats` |
| Credentials in the URL | parsed by `nats.go` | `async-nats` does not parse userinfo; the crate splits token / `user:pass` out of the URL and hands them to `ConnectOptions`. Errors are redacted, including short and escaped credentials; `tests/redaction.rs` pins that. |
| Comma-separated URL | per-server credentials | one global credential set; entries with differing userinfo are rejected as `Error::Config` |
| Reconnect give-up | needs `MaxReconnects(-1)` + `IgnoreAuthErrorAbort` | infinite reconnects by default, auth errors do not abort |
| Permanent close | `ClosedHandler` → rebuild | `Event::Closed` / `ClientError::MaxReconnects` (filtered by connection generation) or an ended subscription stream → rebuild |
| Heartbeat | 3 s × 2 misses | `ping_interval` 3 s; the miss count is an `async-nats` internal constant (2) |
| Backoff callback | after a disconnect only | called before every attempt including the first dial; the crate returns 0 for the first so a healthy start does not wait 500 ms |
| Publish back-pressure | 16 MB reconnect buffer (bytes), error on overflow | outbound command queue counted in messages (`queue_capacity`); `publish` never awaits |
| Mirror send task | waits on a member's synchronous publish | awaits the member's command capacity within a fixed 5 s send timeout; public queue admission stays non-blocking |
| Traffic counters | client message/payload counters, including input rejected by a full pending buffer | natslink admission/read counters; `async-nats` exposes protocol-byte counters rather than equivalent payload counters, so input discarded before the reader and direct `client()` I/O are excluded |
| `Flush` | PING / PONG round trip | `client.flush()` only writes the buffer to the socket; to know the server processed a SUB, `request` an unanswered subject (the test harness does) |
| Rejected SUB recovery | probe (temporary SUB + flush + `LastError` delta) | no `LastError` / round-trip flush API in `async-nats`: a conservative rebuild of the affected endpoint on the 2-30 s backoff; a stale error can cost one extra reconnect |
| Stop | waits for in-flight pool callbacks | same; `Sync` / `PerMessage` callbacks are not awaited. `async-nats` 0.50 exposes no abort for its offline initial-connect loop, so `stop` cannot promise to end that internal retry task immediately |
| Logging | go-ethereum `log` / `slog` | `tracing`, same `[natslink]` prefix and keys |
| `Diverged` | missing SUB after a failed `resume` + leftover SUB after a failed `pause` | missing SUB + pending rejected-SUB recovery; always 0 while paused |

The normal data path is identical: Gate → optional byte dedup → bounded
dispatch, once per endpoint copy when mirrored.

## Testing

```bash
make test     # cargo fmt --check, clippy -D warnings, cargo test (integration tests return early without NATS_TEST_URL)
make itest    # two throw-away docker NATS servers, the full suite, containers removed afterwards
make acl-test # opt-in ACL hot-reload drill (spawns its own container)
make reference # Python 3.9+: check Go reference and release metadata
make conformance GO_CONFORMANCE_DIR=/path/to/natslink # shared offline contract
```

The integration suite covers probe environment errors and redaction, all
three dispatch modes, gate, pause / resume (including pause before start and
`docker restart` of the server), worker-pool back-pressure, publisher queue
overflow, pool health-first failover and oversized payloads, comma-list
failover order, dual-server mirroring with pause, shared `DedupSet` with a
custom key, and permanent close → exactly one rebuild → resubscribe →
delivery resumes. See `CONTRIBUTING.md` for the environment variables.

## Versioning

Major and minor versions track the Go module's behavior: Rust `vX.Y.*`
implements the Go `vX.Y.*` contract. Patch versions may advance independently
for implementation-specific fixes. `GO_REFERENCE_VERSION` and
`package.metadata.go-reference` record the exact published Go release used as
the reference; the CI conformance commit is pinned separately. Tags are the
release artefacts. The release workflow checks the reference metadata and
refuses a tag whose version differs from `Cargo.toml`.

## License

MIT, see `LICENSE`.
