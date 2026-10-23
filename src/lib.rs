//! # natslink (Rust)
//!
//! Wraps "**one NATS connection bound to one topic**" into a self-supervising unit that works
//! out of the box. Tracks the public Go module `github.com/Jolly23/natslink` v1.8.3; client-level
//! differences are listed in the README.
//!
//! - [`probe`] / [`must_probe`]: fail-fast startup probe. Environment errors (address / network /
//!   token) surface within 5s.
//! - [`Subscriber`]: one connection subscribing to one topic. Connection supervision, automatic
//!   reconnect, self-healing rebuild + automatic resubscribe after a permanent close, statistics
//!   accumulated across rebuilds; three callback dispatch modes (bounded worker pool / synchronous /
//!   unbounded per-message concurrency, see [`Workers`]); runtime [`Subscriber::pause`] /
//!   [`Subscriber::resume`] to unsubscribe / resubscribe without dropping the connection (v1.5.0);
//!   optional [`SubscriberOptions::gate`] admission filter.
//! - [`Publisher`]: one connection publishing to one topic; publish failures are returned as errors
//!   for the caller to decide.
//! - [`PublisherPool`]: N publishers in round-robin with health-first failover.
//! - Mirrored constructors [`PublisherPool::new_mirrored`] / [`Subscriber::new_mirrored`]: the same
//!   topic mirrored across M mutually independent NATS endpoints (independent send queues; on the
//!   subscribe side a [`DedupSet`] can filter byte-identical copies).
//!
//! Standard startup sequence: `must_probe(url).await` → `new` → `must_start().await`. From then on,
//! disconnects and permanent closes are handled entirely by the crate's self-healing: it never gives
//! up and never fails silently.
//!
//! ## Differences from the Go version (read this first)
//!
//! - Built on `async-nats` (tokio); every API is an `async fn`; callbacks are
//!   `Fn(Message) -> BoxFuture`.
//! - `async-nats` **does not parse userinfo in URLs**; this crate splits `nats://token@host` /
//!   `nats://user:pass@host` into `ConnectOptions::token` / `user_and_password` itself, so the URL
//!   contract matches the Go version.
//! - `async-nats` reconnects indefinitely by default (`max_reconnects(None)`); the Go version's
//!   "give up after reconnects are exhausted" and "give up after two auth errors" lines of defence
//!   simply do not exist here. A "permanent close" maps to `Event::Closed` /
//!   `ClientError::MaxReconnects` or an unexpectedly ended subscription stream, and goes through the
//!   same self-healing rebuild.
//! - Heartbeat: `ping_interval` is configurable; the number of consecutive missed responses is an
//!   internal `async-nats` constant (2) and is not exposed.
//! - Publishing: `async-nats`'s `publish` **blocks on await** when the outbound queue is full instead
//!   of erroring; this crate uses `now_or_never` for a single non-blocking enqueue (same semantics as
//!   Go's "return once written to the buffer, error when the buffer is full"). A full queue counts
//!   as `dropped` and returns [`Error::BufferFull`], so callers are never stalled by NATS
//!   backpressure. The outbound queue capacity `PublisherOptions::queue_capacity` is per topic
//!   (keep it small for large payloads so memory is not pinned).
//! - The reconnect backoff callback is invoked before **every** attempt, including the first
//!   connect; it returns 0 the first time so a healthy startup does not wait 500ms for nothing.
//! - Logging goes through `tracing`; messages still start with `[natslink]` and field keys match the
//!   Go version so the same grep patterns work.
//! - The underlying version is pinned to `async-nats` 0.50 (`default-features = false,
//!   features = ["ring"]`: core + TLS only).

use std::fmt;
use std::time::Duration;

use async_nats::ConnectOptions;

mod dedup;
mod link;
mod mirror;
mod publisher;
pub mod redact;
mod subscriber;

pub use dedup::DedupSet;
pub use publisher::{Publisher, PublisherOptions, PublisherPool};
pub use redact::{redact_error, redact_url};
pub use subscriber::{handler, DedupKey, Gate, Handler, Subscriber, SubscriberOptions, Workers};

/// Downstream crates that need the concrete client types (`Client` / `Subscriber` /
/// `ConnectOptions`) take them from here instead of depending on `async-nats` directly, so the
/// dependency graph holds exactly one copy of async-nats.
pub use async_nats;
pub use async_nats::Message;
pub use bytes::Bytes;

/// Rust crate version from Cargo; the Go source lives in its own public module. Logged at startup
/// to identify the Rust build.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// Exact Go module whose semantics this port implements.
pub const GO_REFERENCE_VERSION: &str = "v1.8.3";

// —— Connection supervision parameters: fixed values tuned in production; callers need not (and cannot) configure them ——
pub(crate) const DIAL_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const RECONNECT_WAIT: Duration = Duration::from_millis(500);
pub(crate) const RECONNECT_WAIT_MAX: Duration = Duration::from_secs(5);
pub(crate) const PING_INTERVAL: Duration = Duration::from_secs(3);
pub(crate) const REBUILD_BACKOFF: Duration = Duration::from_secs(2);
pub(crate) const REBUILD_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Shutdown grace for a single connection's "drain + close"; past it the wait is abandoned and the
/// close continues in the background (a production shutdown once hung past the service manager's
/// stop budget).
pub(crate) const STOP_GRACE: Duration = Duration::from_secs(5);
pub(crate) const FLUSH_ON_STOP: Duration = Duration::from_secs(1);
/// Default capacity of a publishing connection's outbound command queue (≈ the role of the Go
/// version's reconnect buffer, but counted in commands, not bytes); lower it via
/// `PublisherOptions::queue_capacity` for large-payload topics. A subscribing connection's outbound
/// queue only carries SUB/UNSUB/PING and uses the same default.
pub(crate) const DEFAULT_PUBLISH_QUEUE: usize = 1024;
/// Default receive buffer limit per subscription (beyond it async-nats drops messages and raises
/// SlowConsumer).
pub(crate) const SUBSCRIPTION_CAPACITY: usize = 65_536;

pub(crate) const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(60);
pub(crate) const DEFAULT_WORKERS: usize = 8;
pub(crate) const DEFAULT_QUEUE_SIZE: usize = 4096;

/// Crate error. Helpers such as `is_no_conn()` spare callers from dealing with the underlying
/// async-nats error types. All text is redacted (tokens / passwords from URLs never appear).
#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("natslink: already started")]
    AlreadyStarted,
    #[error("natslink: already stopped; create a new instance")]
    Stopped,
    #[error("natslink: not running")]
    NotRunning,
    #[error("natslink: no usable connection (rebuilding)")]
    NoConn,
    #[error("natslink: payload exceeds MaxPayload: {} > {}", pretty_size(*.size as u64), pretty_size(*.max as u64))]
    Oversized { size: usize, max: usize },
    /// Outbound queue full (unresponsive peer / backlog from a long disconnect); the message was not
    /// accepted. Corresponds to Go's reconnect buffer overflow.
    #[error("natslink: outbound queue full ({0} commands), message dropped")]
    BufferFull(usize),
    /// No mirrored endpoint could reserve space in its bounded send queue.
    #[error("natslink: every endpoint send queue is full")]
    QueueFull,
    #[error("natslink: {0}")]
    Config(String),
    /// Underlying NATS error (text already redacted).
    #[error("natslink: {0}")]
    Nats(String),
    /// Aggregate when every endpoint / member failed (corresponds to Go's errors.Join).
    #[error("natslink: {}", .0.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "))]
    Multiple(Vec<Error>),
}

impl Error {
    /// Whether the connection is in a self-healing rebuild window / has no usable connection;
    /// callers use this to fail over.
    pub fn is_no_conn(&self) -> bool {
        matches!(self, Error::NoConn)
    }
}

/// Status report interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReportInterval {
    /// Default: 1 minute.
    #[default]
    Default,
    /// Disable the built-in status report; the caller polls `stats()` and aggregates itself.
    Off,
    /// Custom interval.
    Every(Duration),
}

impl ReportInterval {
    pub(crate) fn duration(self) -> Option<Duration> {
        match self {
            ReportInterval::Default => Some(DEFAULT_REPORT_INTERVAL),
            ReportInterval::Off => None,
            ReportInterval::Every(d) if d.is_zero() => None,
            ReportInterval::Every(d) => Some(d),
        }
    }
}

/// Escape hatch: further rewrite `ConnectOptions` (e.g. TLS settings) after this crate's policy
/// parameters and before the event callbacks. Overriding `max_reconnects` /
/// `retry_on_initial_connect` / `ping_interval` removes the corresponding line of defence, at your
/// own risk; the event callbacks are injected afterwards and cannot be overridden.
pub type ConnectTweak = std::sync::Arc<dyn Fn(ConnectOptions) -> ConnectOptions + Send + Sync>;

/// Connection configuration shared by Subscriber / Publisher.
#[derive(Clone, Default)]
pub struct Options {
    /// Required. Full NATS URL, e.g. `nats://token@host:4222` (in deployments typically a
    /// co-located leaf node). Comma-separated addresses = ordered failover within the endpoint
    /// (only one is connected at a time, preferred in written order); every entry's userinfo must
    /// be identical (or all absent), otherwise start / probe reports a configuration error, see
    /// `split_auth`.
    pub url: String,
    /// Required. The single topic bound to this connection (no wildcards on the publish side).
    pub topic: String,
    /// Client name (shown in server monitoring and logs); empty = `natslink-<role>-<topic>`.
    pub name: String,
    /// Built-in status report interval, see [`ReportInterval`].
    pub report_interval: ReportInterval,
    /// See [`ConnectTweak`].
    pub tweak: Option<ConnectTweak>,
}

impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Options")
            .field("url", &redact_url(&self.url))
            .field("topic", &self.topic)
            .field("name", &self.name)
            .field("report_interval", &self.report_interval)
            .finish()
    }
}

impl Options {
    /// Validates required fields and fills in defaults. `role` is "sub" / "pub", used for the
    /// default name and topic validation.
    pub(crate) fn normalize(mut self, role: &str) -> Result<Options, Error> {
        if self.url.trim().is_empty() {
            return Err(Error::Config("Options.url is required".into()));
        }
        validate_topic(&self.topic, role)?;
        if self.name.is_empty() {
            self.name = format!("natslink-{role}-{}", self.topic);
        }
        Ok(self)
    }
}

/// Static subject validation at construction time (core NATS rules: tokens separated by `.`,
/// non-empty, no whitespace; wildcards `*` / `>` are only legal on the subscribe side). The publish
/// side never subscribes, so an invalid subject would sail through probe / start and only be
/// rejected by the server on the first publish; hence the fail-fast here.
pub(crate) fn validate_topic(topic: &str, role: &str) -> Result<(), Error> {
    if topic.is_empty() {
        return Err(Error::Config("Options.topic is required".into()));
    }
    let mut tokens = topic.split('.').peekable();
    while let Some(tok) = tokens.next() {
        if tok.is_empty() {
            return Err(Error::Config(format!(
                "invalid topic {topic:?}: empty token (leading/trailing/double dot)"
            )));
        }
        if tok.chars().any(char::is_whitespace) {
            return Err(Error::Config(format!(
                "invalid topic {topic:?}: whitespace in token"
            )));
        }
        if tok == ">" && tokens.peek().is_some() {
            return Err(Error::Config(format!(
                "invalid topic {topic:?}: > must be the final token"
            )));
        }
        if role == "pub" && (tok == "*" || tok == ">") {
            return Err(Error::Config(format!(
                "invalid topic {topic:?}: wildcard is subscribe-only, cannot publish to it"
            )));
        }
    }
    Ok(())
}

/// Splits Options.url into the credentials and the list of addresses with userinfo stripped
/// (async-nats does not parse userinfo).
///
/// A comma-separated list = ordered failover within the endpoint (nats.go semantics). nats.go uses
/// the userinfo from each server's own URL (no credentials if absent); async-nats has a single
/// global `ConnectOptions` credential sent to every server, and `auth_callback` cannot see the
/// current server either. So only lists whose **entries carry identical credentials** are accepted
/// (all absent, or all the same token / the same user:pass), which is then equivalent to Go;
/// differing entries (including some with and some without) are a configuration error: one
/// entry's credentials must never be sent to a server the operator did not label with them.
/// Deliberately does not derive `Debug`: it holds plaintext credentials.
#[derive(PartialEq, Eq)]
pub(crate) enum Auth {
    None,
    Token(String),
    UserPass(String, String),
}

pub(crate) fn split_auth(raw: &str) -> Result<(Vec<String>, Auth), Error> {
    let mut first: Option<Auth> = None;
    let mut servers = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (scheme, rest) = match part.find("://") {
            Some(i) => (&part[..i + 3], &part[i + 3..]),
            None => ("nats://", part),
        };
        let this = match rest.rfind('@') {
            Some(at) => {
                let userinfo = &rest[..at];
                let hostport = &rest[at + 1..];
                if hostport.is_empty() {
                    return Err(Error::Config(format!(
                        "invalid url {}: empty host",
                        redact_url(raw)
                    )));
                }
                servers.push(format!("{scheme}{hostport}"));
                match userinfo.split_once(':') {
                    Some((u, p)) => Auth::UserPass(u.to_string(), p.to_string()),
                    None => Auth::Token(userinfo.to_string()),
                }
            }
            None => {
                servers.push(format!("{scheme}{rest}"));
                Auth::None
            }
        };
        match &first {
            None => first = Some(this),
            Some(f) if *f == this => {}
            Some(_) => {
                return Err(Error::Config(format!(
                    "invalid url {}: entry {} carries different credentials than entry 0; \
                     comma-separated servers must all share the same userinfo (or all have none)",
                    redact_url(raw),
                    servers.len() - 1
                )));
            }
        }
    }
    if servers.is_empty() {
        return Err(Error::Config("Options.url has no usable endpoint".into()));
    }
    Ok((servers, first.unwrap_or(Auth::None)))
}

pub(crate) fn apply_auth(co: ConnectOptions, auth: &Auth) -> ConnectOptions {
    match auth {
        Auth::None => co,
        Auth::Token(t) => co.token(t.clone()),
        Auth::UserPass(u, p) => co.user_and_password(u.clone(), p.clone()),
    }
}

/// Point-in-time snapshot of status and traffic statistics. Counters accumulate across self-healing
/// rebuilds; they may dip briefly at the moment of a rebuild switch, so delta-based monitoring
/// should clamp negative increments to zero. Field meanings match the Go version one for one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stats {
    /// Connection name (pool members carry a `-<i>` suffix, mirrored endpoints `-e<i>`); the
    /// attribution field for a degraded connection.
    pub name: String,
    /// True after start and before stop.
    pub running: bool,
    /// Whether currently connected.
    pub connected: bool,
    /// Underlying connection state (CONNECTED / DISCONNECTED / PENDING), or no-conn without a
    /// connection.
    pub status: String,
    /// Name of the currently connected server.
    pub server: String,
    /// Cluster name of the currently connected server (empty if the server has no cluster).
    pub cluster: String,

    /// Successful client auto-reconnects (excluding the initial connect).
    pub reconnects: u64,
    /// Whole-connection self-healing rebuilds after a "permanent close" (excluding the initial
    /// connect).
    pub rebuilds: u64,
    pub in_msgs: u64,
    pub out_msgs: u64,
    pub in_bytes: u64,
    pub out_bytes: u64,

    /// Publisher: publish failures (rebuild window / timeout / oversized); Subscriber: messages
    /// dropped on worker queue overflow (only grows in worker-pool mode). A pool member's dropped
    /// includes intermediate failures during failover; to monitor a pool's real losses use
    /// `PublisherPool::dropped()`.
    pub dropped: u64,
    /// Whether the subscriber is paused (after pause, before resume). Always false for Publisher.
    pub paused: bool,
    /// Cumulative successful pause transitions on the subscriber. Always 0 for Publisher.
    pub pauses: u64,
    /// Subscriber only: endpoints that are not paused and have a registered connection but no
    /// subscription (subscribe failed on resume; the repair task is retrying; same-named field in
    /// Go v1.5.1). Endpoints inside a rebuild window are owned by the rebuild and not counted.
    /// Always 0 for Publisher.
    pub diverged: usize,
    /// Messages the subscriber's gate (gate returned false) rejected at the delivery entry.
    pub gated: u64,
    /// Copies removed by the optional payload deduplicator, after Gate and before dispatch.
    pub deduped: u64,
    /// Events where the subscriber consumed too slowly and the async-nats client dropped messages
    /// (`Event::SlowConsumer`).
    pub slow_consumers: u64,

    /// Subscriber worker queue length / capacity (0 for synchronous / PerMessage); for a mirrored
    /// Publisher member, the endpoint send queue.
    pub queue_len: usize,
    pub queue_cap: usize,
    /// Mirrored endpoint copies rejected by queue limits or discarded during shutdown.
    /// Separate from per-member send failures and pool-level total losses.
    pub queue_dropped: u64,
}

/// Verifies with a one-off scouting connection that the URL is reachable and authentication
/// succeeds, then closes that connection immediately.
///
/// Why it exists: `start` enables retry_on_initial_connect + unlimited reconnects, so an
/// unreachable server or a wrong token is **not a start error** (the connection is created as
/// usual and retries in the background). The right startup semantics are fail-fast: a configured
/// NATS endpoint is a promise, and failing to connect is an environment error that should kill the
/// process immediately. Each URL needs only one probe; errors surface within 5s.
/// The URL in the returned error is redacted and safe to print.
pub async fn probe(nats_url: &str) -> Result<(), Error> {
    let (servers, auth) = split_auth(nats_url)?;
    let co = apply_auth(
        ConnectOptions::new()
            .name("natslink-probe")
            .connection_timeout(DIAL_TIMEOUT)
            .retain_servers_order(),
        &auth,
    );
    let client = tokio::time::timeout(DIAL_TIMEOUT, co.connect(servers))
        .await
        .map_err(|_| {
            Error::Nats(format!(
                "probe {} failed (check url / network / token): connect timed out after {:?}",
                redact_url(nats_url),
                DIAL_TIMEOUT
            ))
        })?
        .map_err(|e| {
            Error::Nats(format!(
                "probe {} failed (check url / network / token): {}",
                redact_url(nats_url),
                redact_error(nats_url, &e.to_string())
            ))
        })?;
    // The handshake is complete when connect() returns (retry_on_initial_connect is off); one more
    // flush confirms authentication passed.
    if let Err(e) = tokio::time::timeout(DIAL_TIMEOUT, client.flush()).await {
        return Err(Error::Nats(format!(
            "probe {} failed: flush timed out: {e}",
            redact_url(nats_url)
        )));
    }
    drop(client);
    Ok(())
}

/// Same as [`probe`], but panics on failure. Intended for process startup only.
pub async fn must_probe(nats_url: &str) {
    if let Err(e) = probe(nats_url).await {
        panic!("{e}");
    }
}

/// Runs [`must_probe`] once per URL; the standard fail-fast startup step for mirrored
/// (multi-endpoint) constructors.
pub async fn must_probe_each<I, S>(urls: I)
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    for u in urls {
        must_probe(u.as_ref()).await;
    }
}

pub(crate) fn pretty_size(bytes: u64) -> String {
    const UNIT: u64 = 1024;
    if bytes < UNIT {
        return format!("{bytes}B");
    }
    let mut div = UNIT;
    let mut exp = 0usize;
    let mut n = bytes / UNIT;
    while n >= UNIT {
        div *= UNIT;
        exp += 1;
        n /= UNIT;
    }
    let units = ['K', 'M', 'G', 'T', 'P', 'E'];
    format!("{:.2}{}B", bytes as f64 / div as f64, units[exp])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_defaults() {
        let o = Options {
            url: "nats://token@localhost:4222".into(),
            topic: "events.tx".into(),
            ..Default::default()
        }
        .normalize("sub")
        .unwrap();
        assert_eq!(o.name, "natslink-sub-events.tx");
        assert_eq!(o.report_interval.duration(), Some(DEFAULT_REPORT_INTERVAL));
        assert_eq!(ReportInterval::Off.duration(), None);
        assert_eq!(ReportInterval::Every(Duration::ZERO).duration(), None);
    }

    #[test]
    fn normalize_required() {
        assert!(Options {
            topic: "t".into(),
            ..Default::default()
        }
        .normalize("pub")
        .is_err());
        assert!(Options {
            url: "nats://h".into(),
            ..Default::default()
        }
        .normalize("pub")
        .is_err());
    }

    #[test]
    fn topic_validation() {
        assert!(validate_topic("events.block", "pub").is_ok());
        assert!(validate_topic("events.*", "sub").is_ok());
        assert!(validate_topic("events.*", "pub").is_err());
        assert!(validate_topic("events..block", "sub").is_err());
        assert!(validate_topic("events. block", "sub").is_err());
        assert!(validate_topic(".gz", "sub").is_err());
        assert!(validate_topic("events.>.block", "sub").is_err());
        assert!(validate_topic("events.>", "sub").is_ok());
    }

    #[test]
    fn split_auth_forms() {
        let (s, a) = split_auth("nats://tok@h:1, nats://tok@h2:2").unwrap();
        assert_eq!(s, vec!["nats://h:1", "nats://h2:2"]);
        assert!(a == Auth::Token("tok".into()));
        let (s, a) = split_auth("nats://h:1,nats://h2:2").unwrap();
        assert_eq!(s, vec!["nats://h:1", "nats://h2:2"]);
        assert!(a == Auth::None);
        // Differing credentials per entry: async-nats has a single global credential and cannot
        // match nats.go's per-server userinfo.
        for mixed in [
            "nats://tok@h:1,nats://h2:2",
            "nats://h:1,nats://tok@h2:2",
            "nats://tok@h:1,nats://other@h2:2",
            "nats://u:p@h:1,nats://u:q@h2:2",
            "nats://tok@h:1,nats://tok:x@h2:2",
        ] {
            let Err(e) = split_auth(mixed) else {
                panic!("{mixed}: mixed credentials must be rejected");
            };
            let e = e.to_string();
            assert!(e.contains("different credentials"), "{mixed}: {e}");
            for secret in ["tok", "other", "u:p", "u:q"] {
                assert!(!e.contains(secret), "{mixed}: {secret} leaked in {e}");
            }
        }
        let (s, a) = split_auth("u:p@h:1").unwrap();
        assert_eq!(s, vec!["nats://h:1"]);
        assert!(matches!(a, Auth::UserPass(ref u, ref p) if u == "u" && p == "p"));
        let (_, a) = split_auth("tls://h:1").unwrap();
        assert!(matches!(a, Auth::None));
        assert!(split_auth(" , ").is_err());
        assert!(split_auth("nats://tok@").is_err());
    }

    #[test]
    fn pretty() {
        assert_eq!(pretty_size(512), "512B");
        assert_eq!(pretty_size(1536), "1.50KB");
        assert_eq!(pretty_size(16 * 1024 * 1024), "16.00MB");
    }
}
