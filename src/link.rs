//! `Link` is the single-connection supervisor shared by `Subscriber` and `Publisher`:
//! dialing, event callbacks, self-healing rebuild after a permanent close, and statistics
//! that accumulate across rebuilds. Mirrors `link.go` in the Go module.
//!
//! Disconnects come in two layers:
//! - Ordinary disconnects (server restart, network hiccup, a stalled connection caught by
//!   the ping): async-nats reconnects on its own (unlimited attempts) and restores
//!   subscriptions; this crate does nothing.
//! - Permanent close (`Event::Closed` / `ClientError::MaxReconnects`, or the subscription
//!   stream ending unexpectedly): triggers a rebuild, which redials the whole connection
//!   (re-subscribing on subscriber links) and swaps it in atomically.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};

use async_nats::connection::State;
use async_nats::{Client, ClientError, ConnectOptions, Event, ServerError};
use futures::future::BoxFuture;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info, warn};

use crate::redact::redact_error;
use crate::{
    apply_auth, pretty_size, split_auth, Error, Options, Stats, DEFAULT_PUBLISH_QUEUE,
    DIAL_TIMEOUT, FLUSH_ON_STOP, PING_INTERVAL, REBUILD_BACKOFF, REBUILD_BACKOFF_MAX,
    RECONNECT_WAIT, RECONNECT_WAIT_MAX, STOP_GRACE, SUBSCRIPTION_CAPACITY, VERSION,
};

/// Called after every successful dial and before the connection is installed (subscriber
/// links subscribe here); returning an error discards that connection and retries.
pub(crate) type SetupFn =
    Arc<dyn Fn(Client, u64) -> BoxFuture<'static, Result<(), Error>> + Send + Sync>;
/// Synchronous event handoff only: the owner must coalesce/spawn work, never wait here.
pub(crate) type SubRejectedFn = Arc<dyn Fn(u64, String) + Send + Sync>;
/// Lets the owner inject shared fields into this connection's `Stats` snapshot (the
/// subscriber's queue depth, drop count, paused state).
pub(crate) type StatsHook = Arc<dyn Fn(&mut Stats) + Send + Sync>;
#[cfg(test)]
type StopTestHook = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

#[derive(Default)]
struct RebuildState {
    active: bool,
    pending: bool,
}

pub(crate) struct Link {
    pub(crate) o: Options,
    pub(crate) role: &'static str,

    client: RwLock<Option<Arc<Client>>>,
    /// Incremented on every dial; event callbacks carry their own generation, so late
    /// events from a replaced connection never trigger a rebuild.
    generation: AtomicU64,
    connects_by_gen: Mutex<HashMap<u64, u64>>,

    /// Per-subscription receive buffer limit (async-nats `subscription_capacity`; a link has
    /// exactly one subscription, so this is the equivalent of Go's `PendingMsgLimit`).
    subscription_capacity: usize,
    /// Outbound command queue capacity (async-nats `client_capacity`). Publishers configure
    /// it per topic, see `PublisherOptions::queue_capacity`.
    pub(crate) client_capacity: usize,

    running: AtomicBool,
    stopped: AtomicBool,
    rebuild_state: Mutex<RebuildState>,
    /// Generation for which a permanent-close rebuild has already been triggered: `Closed`
    /// and `ClientError::MaxReconnects` arrive back to back for the same generation and
    /// must rebuild only once.
    last_closed_gen: AtomicU64,
    done: CancellationToken,
    stop_complete: CancellationToken,
    /// Serialize initial setup and rebuilding without holding a synchronous lock across await.
    connect_lock: tokio::sync::Mutex<()>,
    tasks: TaskTracker,
    #[cfg(test)]
    stop_test_hook: Mutex<Option<StopTestHook>>,

    rebuilds: AtomicU64,
    pub(crate) dropped: AtomicU64,
    slow_consumers: AtomicU64,

    prev_reconnects: AtomicU64,
    in_msgs: AtomicU64,
    out_msgs: AtomicU64,
    in_bytes: AtomicU64,
    out_bytes: AtomicU64,

    setup: Mutex<Option<SetupFn>>,
    sub_rejected: Mutex<Option<SubRejectedFn>>,
    stats_hook: Mutex<Option<StatsHook>>,
}

impl Link {
    pub(crate) fn new(
        o: Options,
        role: &'static str,
        subscription_capacity: usize,
        client_capacity: usize,
    ) -> Arc<Link> {
        Arc::new(Link {
            o,
            role,
            subscription_capacity: if subscription_capacity == 0 {
                SUBSCRIPTION_CAPACITY
            } else {
                subscription_capacity
            },
            client_capacity: if client_capacity == 0 {
                DEFAULT_PUBLISH_QUEUE
            } else {
                client_capacity
            },
            client: RwLock::new(None),
            generation: AtomicU64::new(0),
            connects_by_gen: Mutex::new(HashMap::new()),
            running: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            rebuild_state: Mutex::new(RebuildState::default()),
            last_closed_gen: AtomicU64::new(0),
            done: CancellationToken::new(),
            stop_complete: CancellationToken::new(),
            connect_lock: tokio::sync::Mutex::new(()),
            tasks: TaskTracker::new(),
            #[cfg(test)]
            stop_test_hook: Mutex::new(None),
            rebuilds: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            slow_consumers: AtomicU64::new(0),
            prev_reconnects: AtomicU64::new(0),
            in_msgs: AtomicU64::new(0),
            out_msgs: AtomicU64::new(0),
            in_bytes: AtomicU64::new(0),
            out_bytes: AtomicU64::new(0),
            setup: Mutex::new(None),
            sub_rejected: Mutex::new(None),
            stats_hook: Mutex::new(None),
        })
    }

    pub(crate) fn set_setup(&self, f: SetupFn) {
        *self.setup.lock().unwrap() = Some(f);
    }

    pub(crate) fn set_sub_rejected(&self, f: SubRejectedFn) {
        *self.sub_rejected.lock().unwrap() = Some(f);
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub(crate) fn set_stats_hook(&self, f: StatsHook) {
        *self.stats_hook.lock().unwrap() = Some(f);
    }

    pub(crate) fn client(&self) -> Option<Client> {
        self.client_snapshot().map(|client| (*client).clone())
    }

    /// Clone only the Arc on the data path: Client::clone also allocates its
    /// PollSender future. A snapshot remains valid during a connection rebuild.
    pub(crate) fn client_snapshot(&self) -> Option<Arc<Client>> {
        let slot = self.client.read().unwrap();
        if self.is_stopped() {
            None
        } else {
            slot.clone()
        }
    }

    pub(crate) fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.client_snapshot()
            .map(|c| c.connection_state() == State::Connected)
            .unwrap_or(false)
    }

    pub(crate) fn topic(&self) -> &str {
        &self.o.topic
    }

    pub(crate) fn record_publish(&self, bytes: usize) {
        self.out_msgs.fetch_add(1, Ordering::Relaxed);
        self.out_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_receive(&self, message: &async_nats::Message) {
        // async-nats 0.50 records subject + reply + the complete payload/header
        // block in Message::length. Remove addressing, retaining encoded headers.
        let bytes = message.length.saturating_sub(
            message.subject.len() + message.reply.as_ref().map_or(0, |reply| reply.len()),
        );
        self.in_msgs.fetch_add(1, Ordering::Relaxed);
        self.in_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Reconnect backoff: doubles from `RECONNECT_WAIT` up to `RECONNECT_WAIT_MAX`, plus up to
    /// +50% jitter so that many connections reconnect out of phase instead of hammering a
    /// server that just restarted and is still warming up.
    pub(crate) fn reconnect_delay(&self, attempts: usize) -> Duration {
        // async-nats invokes this before every attempt, including the very first connect
        // (attempts == 1): a healthy start must not wait.
        if attempts <= 1 {
            return Duration::ZERO;
        }
        let mut d = RECONNECT_WAIT;
        for _ in 0..attempts - 2 {
            if d >= RECONNECT_WAIT_MAX {
                break;
            }
            d *= 2;
        }
        d = d.min(RECONNECT_WAIT_MAX);
        let jitter = rand::random_range(0..=(d.as_millis() as u64 / 2));
        d += Duration::from_millis(jitter);
        if attempts <= 6 || attempts.is_multiple_of(10) {
            warn!(client = %self.o.name, topic = %self.o.topic, attempt = attempts - 1, next_wait = ?d, "[natslink] 🔄 reconnecting");
        }
        d
    }

    /// Dial: policy options (unlimited reconnects, ordered endpoints, ignore gossip, ping,
    /// capacities) → caller's `tweak` → event callback (not overridable).
    /// With `retry_on_initial_connect` enabled this returns `Ok` immediately even when the
    /// server is temporarily unreachable (retries continue in the background); an `Err`
    /// essentially means a URL or option configuration error.
    async fn dial(self: &Arc<Self>) -> Result<Client, Error> {
        let (servers, auth) = split_auth(&self.o.url)?;
        let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        self.connects_by_gen.lock().unwrap().clear();
        let delay_link = Arc::downgrade(self);
        let mut co = ConnectOptions::new()
            .name(self.o.name.clone())
            .max_reconnects(None)
            .retry_on_initial_connect()
            .reconnect_delay_callback(move |attempts| {
                delay_link
                    .upgrade()
                    .map_or(RECONNECT_WAIT_MAX, |l| l.reconnect_delay(attempts))
            })
            .retain_servers_order()
            .ignore_discovered_servers()
            .connection_timeout(DIAL_TIMEOUT)
            .ping_interval(PING_INTERVAL)
            .client_capacity(self.client_capacity)
            .subscription_capacity(self.subscription_capacity);
        co = apply_auth(co, &auth);
        if let Some(t) = &self.o.tweak {
            co = t(co);
        }
        let weak: Weak<Link> = Arc::downgrade(self);
        co = co.event_callback(move |ev| {
            let weak = weak.clone();
            async move {
                if let Some(l) = weak.upgrade() {
                    l.on_event(generation, ev);
                }
            }
        });
        // Must pass a Vec: async-nats parses a `String` as a single address, so a
        // comma-separated list would always fail with "invalid port number".
        co.connect(servers)
            .await
            .map_err(|e| Error::Nats(redact_error(&self.o.url, &e.to_string())))
    }

    fn on_event(self: &Arc<Self>, generation: u64, ev: Event) {
        if generation != self.generation() || !self.is_running() || self.is_stopped() {
            return;
        }
        let name = self.o.name.as_str();
        let topic = self.o.topic.as_str();
        match ev {
            Event::Connected => {
                let n = {
                    let mut m = self.connects_by_gen.lock().unwrap();
                    let c = m.entry(generation).or_insert(0);
                    *c += 1;
                    *c
                };
                let server = self
                    .client()
                    .map(|c| c.server_info().server_name)
                    .unwrap_or_default();
                if n == 1 {
                    info!(client = %name, topic = %topic, server = %server, "[natslink] connected");
                } else {
                    info!(client = %name, topic = %topic, server = %server, "[natslink] 🔄 reconnected");
                }
            }
            Event::Disconnected => {
                warn!(client = %name, topic = %topic, "[natslink] ⚠️ disconnected");
            }
            Event::SlowConsumer(sid) => {
                // A slow consumer silently drops messages (the connection stays healthy and
                // looks fine while losing data), so count it separately for observability.
                self.slow_consumers.fetch_add(1, Ordering::Relaxed);
                warn!(client = %name, topic = %topic, sid, "[natslink] ⚠️ async error: slow consumer, messages dropped");
            }
            Event::ServerError(e) => {
                warn!(client = %name, topic = %topic, err = %redact_error(&self.o.url, &e.to_string()), "[natslink] ⚠️ async error");
                // async-nats keeps the server's subject (including case/quoting)
                // only in Other. Publish errors and subject-less limits do not
                // mean a subscription was rejected. The owner checks the exact
                // decoded subject and current subscription generation.
                if let ServerError::Other(message) = e {
                    if is_subscription_rejection(&message) {
                        let hook = self.sub_rejected.lock().unwrap().clone();
                        if let Some(hook) = hook {
                            hook(generation, message);
                        }
                    }
                }
            }
            Event::ClientError(ClientError::ServerNotInPool) => {
                warn!(client = %name, topic = %topic, "[natslink] ⚠️ async error: server not in pool");
            }
            Event::ClientError(ClientError::Other(s)) => {
                warn!(client = %name, topic = %topic, err = %redact_error(&self.o.url, &s), "[natslink] ⚠️ async error");
            }
            Event::LameDuckMode => {
                info!(client = %name, topic = %topic, "[natslink] server entering lame duck mode");
            }
            Event::Draining => {}
            Event::Closed | Event::ClientError(ClientError::MaxReconnects) => {
                // Reaching here means the connection is permanently closed. Only the
                // current generation is worth rebuilding: a replaced connection also emits
                // Closed when dropped, which the generation check filters out.
                if generation != self.generation.load(Ordering::Acquire) || !self.is_running() {
                    return;
                }
                if self.last_closed_gen.swap(generation, Ordering::AcqRel) == generation {
                    return; // second close event of the same generation (Closed right after MaxReconnects); already rebuilding
                }
                error!(client = %name, topic = %topic, "[natslink] ⛔ connection permanently closed, rebuilding");
                self.request_rebuild();
            }
        }
    }

    /// Request a self-healing rebuild (idempotent: while a rebuild loop is already running
    /// this only sets `pending`, and that loop runs again when it finishes).
    pub(crate) fn request_rebuild(self: &Arc<Self>) {
        self.spawn_rebuild(REBUILD_BACKOFF);
    }

    /// The subscription supervisor has already waited its rejection backoff.
    pub(crate) fn request_rebuild_now(self: &Arc<Self>) {
        self.spawn_rebuild(Duration::ZERO);
    }

    fn spawn_rebuild(self: &Arc<Self>, initial_wait: Duration) {
        let _slot = self.client.read().unwrap();
        if !self.is_running() || self.is_stopped() {
            return;
        }
        let mut state = self.rebuild_state.lock().unwrap();
        if state.active {
            state.pending = true;
            return;
        }
        state.active = true;
        let l = Arc::clone(self);
        self.tasks
            .spawn(async move { l.rebuild(initial_wait).await });
    }

    /// Establish the initial connection. Stop and installation are ordered by
    /// the client lock, so a late dial/setup can never resurrect a stopped link.
    pub(crate) async fn start(self: &Arc<Self>) -> Result<(), Error> {
        let result = {
            let _slot = self.client.write().unwrap();
            if self.is_stopped() {
                return Err(Error::Stopped);
            }
            if self.running.swap(true, Ordering::AcqRel) {
                return Err(Error::AlreadyStarted);
            }
            // Cancellation of a caller's wait must not leave running=true
            // without an initializer. Stop cancels and waits for this task.
            let (tx, rx) = tokio::sync::oneshot::channel();
            let l = Arc::clone(self);
            self.tasks.spawn(async move {
                let result = l.start_inner().await;
                let _ = tx.send(result);
            });
            rx
        };
        result.await.unwrap_or_else(|_| {
            Err(if self.is_stopped() {
                Error::Stopped
            } else {
                Error::Nats("connection initialization task terminated".into())
            })
        })
    }

    async fn start_inner(self: &Arc<Self>) -> Result<(), Error> {
        info!(version = VERSION, role = self.role, client = %self.o.name, topic = %self.o.topic, "[natslink] starting");
        let _connecting = tokio::select! {
            biased;
            _ = self.done.cancelled() => return Err(Error::Stopped),
            guard = self.connect_lock.lock() => guard,
        };
        let prepared = tokio::select! {
            biased;
            _ = self.done.cancelled() => Err(Error::Stopped),
            result = self.prepare_client() => result,
        };
        let client = match prepared {
            Ok(client) => client,
            Err(err) => {
                self.running.store(false, Ordering::Release);
                return Err(if self.is_stopped() {
                    Error::Stopped
                } else {
                    err
                });
            }
        };
        self.install_client(client)?;
        // Stop cannot finish waiting for tracked tasks before this last spawn.
        let _slot = self.client.read().unwrap();
        if self.is_stopped() {
            return Err(Error::Stopped);
        }
        if let Some(interval) = self.o.report_interval.duration() {
            let l = Arc::clone(self);
            self.tasks
                .spawn(async move { l.report_loop(interval).await });
        }
        Ok(())
    }

    async fn prepare_client(self: &Arc<Self>) -> Result<Client, Error> {
        let client = self.dial().await?;
        let setup = self.setup.lock().unwrap().clone();
        if let Some(setup) = setup {
            setup(client.clone(), self.generation()).await?;
        }
        Ok(client)
    }

    fn install_client(&self, client: Client) -> Result<(), Error> {
        let mut slot = self.client.write().unwrap();
        if self.is_stopped() || !self.is_running() {
            return Err(Error::Stopped);
        }
        if let Some(old) = slot.replace(Arc::new(client)) {
            self.absorb(&old);
        }
        Ok(())
    }

    /// All callers wait for the same owned shutdown, even if the first caller
    /// is cancelled. The async-nats drain is best effort: during an offline
    /// reconnect its command queue is not serviced and no public abort exists.
    pub(crate) async fn stop(self: &Arc<Self>, flush: bool) {
        let first = {
            let mut slot = self.client.write().unwrap();
            self.running.store(false, Ordering::Release);
            if self.stopped.swap(true, Ordering::AcqRel) {
                None
            } else {
                self.done.cancel();
                let client = slot.take();
                if let Some(client) = &client {
                    self.absorb(client);
                }
                Some(client)
            }
        };
        if let Some(client) = first {
            self.tasks.close();
            let l = Arc::clone(self);
            tokio::spawn(async move {
                let _completion = l.stop_complete.clone().drop_guard();
                let closing = async {
                    #[cfg(test)]
                    {
                        let hook = l.stop_test_hook.lock().unwrap().clone();
                        if let Some(hook) = hook {
                            hook().await;
                        }
                    }
                    if let Some(client) = client {
                        if flush {
                            let result = tokio::time::timeout(FLUSH_ON_STOP, client.flush()).await;
                            match result {
                                Ok(Ok(())) => {}
                                Ok(Err(err)) => {
                                    warn!(client = %l.o.name, err = %redact_error(&l.o.url, &err.to_string()), "[natslink] ⚠️ flush failed on stop, buffered messages lost")
                                }
                                Err(err) => {
                                    warn!(client = %l.o.name, err = %err, "[natslink] ⚠️ flush failed on stop, buffered messages lost")
                                }
                            }
                        }
                        if let Err(err) = client.drain().await {
                            warn!(client = %l.o.name, err = %redact_error(&l.o.url, &err.to_string()), "[natslink] connection drain failed on stop");
                        }
                    }
                    // Serialize against a dial/setup before awaiting all tasks.
                    let _connecting = l.connect_lock.lock().await;
                    l.tasks.wait().await;
                };
                if tokio::time::timeout(STOP_GRACE, closing).await.is_err() {
                    warn!(client = %l.o.name, topic = %l.o.topic, grace = ?STOP_GRACE, "[natslink] ⚠️ stop grace exceeded, abandoning connection close");
                }
            });
        }
        self.stop_complete.cancelled().await;
    }

    /// Redial plus setup after a permanent close, bounded by our stop token.
    async fn rebuild(self: Arc<Self>, initial_wait: Duration) {
        let mut backoff = initial_wait;
        while self.is_running() && !self.is_stopped() {
            tokio::select! {
                biased;
                _ = self.done.cancelled() => break,
                _ = tokio::time::sleep(backoff) => {},
            }
            backoff = if backoff.is_zero() {
                REBUILD_BACKOFF
            } else {
                (backoff * 2).min(REBUILD_BACKOFF_MAX)
            };
            let _connecting = tokio::select! {
                biased;
                _ = self.done.cancelled() => break,
                guard = self.connect_lock.lock() => guard,
            };
            let prepared = tokio::select! {
                biased;
                _ = self.done.cancelled() => break,
                result = self.prepare_client() => result,
            };
            let client = match prepared {
                Ok(client) => client,
                Err(err) => {
                    warn!(client = %self.o.name, topic = %self.o.topic, err = %err, "[natslink] ❌ rebuild dial/setup failed, retrying");
                    continue;
                }
            };
            if self.install_client(client).is_err() {
                break;
            }
            self.rebuilds.fetch_add(1, Ordering::Relaxed);
            info!(client = %self.o.name, topic = %self.o.topic, "[natslink] ♻️ connection rebuilt");
            break;
        }
        // Updating both flags together prevents a request arriving during the
        // exit path from leaving pending=true with no active rebuild owner.
        let pending = {
            let mut state = self.rebuild_state.lock().unwrap();
            state.active = false;
            std::mem::take(&mut state.pending)
        };
        if pending && self.is_running() {
            self.request_rebuild();
        }
    }

    /// Only reconnects come from the client. Traffic is counted at the wrapper's
    /// acceptance/delivery boundaries, so a flush during Stop cannot lose its tail
    /// from the final snapshot and rebuilds never reset traffic counters.
    fn absorb(&self, old: &Client) {
        let st = old.statistics();
        let connects = st.connects.load(Ordering::Relaxed);
        self.prev_reconnects
            .fetch_add(connects.saturating_sub(1), Ordering::Relaxed);
    }

    /// Per-connection counters only. Mirrored subscribers add their shared
    /// counters once after aggregating these snapshots.
    pub(crate) fn raw_stats(&self) -> Stats {
        // Keep connection replacement/absorption atomic to this snapshot.
        let slot = self.client.read().unwrap();
        let mut s = Stats {
            name: self.o.name.clone(),
            running: self.is_running(),
            status: "no-conn".into(),
            rebuilds: self.rebuilds.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            slow_consumers: self.slow_consumers.load(Ordering::Relaxed),
            reconnects: self.prev_reconnects.load(Ordering::Relaxed),
            in_msgs: self.in_msgs.load(Ordering::Relaxed),
            out_msgs: self.out_msgs.load(Ordering::Relaxed),
            in_bytes: self.in_bytes.load(Ordering::Relaxed),
            out_bytes: self.out_bytes.load(Ordering::Relaxed),
            ..Stats::default()
        };
        if let Some(c) = slot.as_ref().filter(|_| !self.is_stopped()) {
            let state = c.connection_state();
            s.connected = state == State::Connected;
            s.status = match state {
                State::Connected => "CONNECTED",
                State::Disconnected => "DISCONNECTED",
                State::Pending => "PENDING",
            }
            .into();
            if s.connected {
                let info = c.server_info();
                s.server = info.server_name;
                s.cluster = info.cluster.unwrap_or_default();
            }
            let st = c.statistics();
            s.reconnects += st.connects.load(Ordering::Relaxed).saturating_sub(1);
        }
        s
    }

    pub(crate) fn stats(&self) -> Stats {
        let mut s = self.raw_stats();
        let hook = self.stats_hook.lock().unwrap().clone();
        if let Some(h) = hook {
            h(&mut s);
        }
        s
    }

    /// Block until the connection is ready, returning an error on timeout. For fail-fast
    /// checks at startup use `probe` instead.
    pub(crate) async fn wait_connected(&self, timeout: Duration) -> Result<(), Error> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.is_stopped() {
                return Err(Error::Stopped);
            }
            if self.is_connected() {
                return if self.is_stopped() {
                    Err(Error::Stopped)
                } else {
                    Ok(())
                };
            }
            if !self.is_running() {
                return Err(if self.is_stopped() {
                    Error::Stopped
                } else {
                    Error::NotRunning
                });
            }
            if Instant::now() >= deadline {
                let status = self.stats().status;
                return Err(Error::Nats(format!(
                    "{} not connected within {:?} (status={status})",
                    self.o.name, timeout
                )));
            }
            tokio::select! {
                biased;
                _ = self.done.cancelled() => return Err(Error::Stopped),
                _ = tokio::time::sleep(Duration::from_millis(50)) => {},
            }
        }
    }

    async fn report_loop(self: Arc<Self>, interval: Duration) {
        self.report();
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = self.done.cancelled() => return,
                _ = ticker.tick() => self.report(),
            }
        }
    }

    fn report(&self) {
        let s = self.stats();
        info!(
            role = self.role,
            client = %self.o.name,
            topic = %self.o.topic,
            status = %s.status,
            server = %s.server,
            reconnects = s.reconnects,
            rebuilds = s.rebuilds,
            dropped = s.dropped,
            in_msgs = s.in_msgs,
            in_bytes = %pretty_size(s.in_bytes),
            out_msgs = s.out_msgs,
            out_bytes = %pretty_size(s.out_bytes),
            queue = %if s.queue_cap > 0 { format!("{}/{}", s.queue_len, s.queue_cap) } else { String::new() },
            gated = s.gated,
            deduped = s.deduped,
            queue_dropped = s.queue_dropped,
            paused = s.paused,
            pauses = s.pauses,
            diverged = s.diverged,
            slow_consumers = s.slow_consumers,
            "[natslink] status"
        );
        if !s.connected {
            warn!(client = %self.o.name, topic = %self.o.topic, status = %s.status, "[natslink] ⚠️ degraded: not connected");
        }
    }
}

fn is_subscription_rejection(message: &str) -> bool {
    message
        .to_ascii_lowercase()
        .starts_with("permissions violation for subscription to ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_link() -> Arc<Link> {
        Link::new(
            Options {
                url: "nats://127.0.0.1:1".into(),
                topic: "Mixed.Case".into(),
                report_interval: crate::ReportInterval::Off,
                // No real server or unbounded retry tasks in offline unit tests.
                tweak: Some(Arc::new(|options| options.max_reconnects(0))),
                ..Default::default()
            }
            .normalize("sub")
            .unwrap(),
            "sub",
            0,
            0,
        )
    }

    #[tokio::test]
    async fn stop_wins_before_start_and_wait_connected() {
        let link = test_link();
        assert!(matches!(
            link.wait_connected(Duration::ZERO).await,
            Err(Error::NotRunning)
        ));
        link.stop(false).await;
        assert!(matches!(link.start().await, Err(Error::Stopped)));
        assert!(matches!(
            link.wait_connected(Duration::ZERO).await,
            Err(Error::Stopped)
        ));
        assert!(!link.stats().running && !link.stats().connected);
    }

    #[tokio::test]
    async fn stop_during_initial_setup_cancels_setup_and_prevents_install() {
        struct SetupDropped(Arc<AtomicBool>);
        impl Drop for SetupDropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let link = test_link();
        let entered = CancellationToken::new();
        let dropped = Arc::new(AtomicBool::new(false));
        link.set_setup(Arc::new({
            let entered = entered.clone();
            let dropped = dropped.clone();
            move |_client, _generation| {
                let entered = entered.clone();
                let dropped = dropped.clone();
                Box::pin(async move {
                    let _guard = SetupDropped(dropped);
                    entered.cancel();
                    std::future::pending::<Result<(), Error>>().await
                })
            }
        }));
        let start = tokio::spawn({
            let link = link.clone();
            async move { link.start().await }
        });
        tokio::time::timeout(Duration::from_secs(1), entered.cancelled())
            .await
            .unwrap();
        link.stop(false).await;
        // Stop itself waits for cancellation, not a later join of Start.
        assert!(dropped.load(Ordering::SeqCst));
        assert!(matches!(start.await.unwrap(), Err(Error::Stopped)));
        assert!(link.client().is_none());
        assert!(!link.is_running());
        assert!(matches!(
            link.wait_connected(Duration::ZERO).await,
            Err(Error::Stopped)
        ));
        assert!(link.tasks.is_empty());
    }

    #[tokio::test]
    async fn cancelling_start_waiter_does_not_abandon_initialization() {
        let link = test_link();
        let entered = CancellationToken::new();
        let release = CancellationToken::new();
        link.set_setup(Arc::new({
            let entered = entered.clone();
            let release = release.clone();
            move |_client, _generation| {
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    entered.cancel();
                    release.cancelled().await;
                    Ok(())
                })
            }
        }));
        let first = tokio::spawn({
            let link = link.clone();
            async move { link.start().await }
        });
        entered.cancelled().await;
        first.abort();
        let _ = first.await;
        assert!(matches!(link.start().await, Err(Error::AlreadyStarted)));
        release.cancel();
        tokio::time::timeout(Duration::from_secs(1), async {
            while link.client().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(link.is_running());
        link.stop(false).await;
        assert!(link.tasks.is_empty());
    }

    #[tokio::test]
    async fn failed_plain_link_start_can_be_retried_like_go() {
        let link = test_link();
        let calls = Arc::new(AtomicU64::new(0));
        link.set_setup(Arc::new({
            let calls = calls.clone();
            move |_client, _generation| {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    if call == 0 {
                        Err(Error::Nats("synthetic setup failure".into()))
                    } else {
                        Ok(())
                    }
                })
            }
        }));
        assert!(matches!(link.start().await, Err(Error::Nats(_))));
        assert!(!link.is_running());
        link.start().await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(link.client().is_some());
        link.stop(false).await;
    }

    #[tokio::test]
    async fn rebuild_requests_coalesce_while_initial_setup_holds_the_lock() {
        let link = test_link();
        link.running.store(true, Ordering::Release);
        let connecting = link.connect_lock.lock().await;
        for _ in 0..100 {
            link.request_rebuild_now();
        }
        assert_eq!(link.tasks.len(), 1);
        {
            let state = link.rebuild_state.lock().unwrap();
            assert!(state.active && state.pending);
        }
        drop(connecting);
        link.stop(false).await;
        assert!(link.tasks.is_empty());
        let state = link.rebuild_state.lock().unwrap();
        assert!(!state.active && !state.pending);
    }

    #[tokio::test]
    async fn stopped_link_rejects_client_at_the_install_boundary() {
        let link = test_link();
        link.running.store(true, Ordering::Release);
        let client = link.dial().await.unwrap();
        link.stop(false).await;
        assert!(matches!(link.install_client(client), Err(Error::Stopped)));
        assert!(link.client.read().unwrap().is_none());
        assert!(!link.is_running());
    }

    #[tokio::test]
    async fn connector_callbacks_do_not_retain_the_link() {
        let link = test_link();
        let weak = Arc::downgrade(&link);
        let client = link.dial().await.unwrap();
        drop(link);
        assert!(weak.upgrade().is_none());
        drop(client);
    }

    #[tokio::test]
    async fn waiting_for_connect_is_interrupted_by_stop() {
        let link = test_link();
        link.running.store(true, Ordering::Release);
        let waiter = tokio::spawn({
            let link = link.clone();
            async move { link.wait_connected(Duration::from_secs(30)).await }
        });
        tokio::task::yield_now().await;
        link.stop(false).await;
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), waiter)
                .await
                .unwrap()
                .unwrap(),
            Err(Error::Stopped)
        ));
    }

    #[test]
    fn raw_snapshot_excludes_shared_stats_hook() {
        let link = test_link();
        let calls = Arc::new(AtomicU64::new(0));
        link.set_stats_hook(Arc::new({
            let calls = calls.clone();
            move |stats| {
                calls.fetch_add(1, Ordering::SeqCst);
                stats.queue_dropped = 7;
            }
        }));
        assert_eq!(link.raw_stats().queue_dropped, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(link.stats().queue_dropped, 7);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_stop_waits_for_owned_shutdown_even_if_first_waiter_is_cancelled() {
        let link = test_link();
        let entered = CancellationToken::new();
        let release = CancellationToken::new();
        let calls = Arc::new(AtomicU64::new(0));
        *link.stop_test_hook.lock().unwrap() = Some(Arc::new({
            let entered = entered.clone();
            let release = release.clone();
            let calls = calls.clone();
            move || {
                let entered = entered.clone();
                let release = release.clone();
                let calls = calls.clone();
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    entered.cancel();
                    release.cancelled().await;
                })
            }
        }));
        let first = tokio::spawn({
            let link = link.clone();
            async move { link.stop(false).await }
        });
        entered.cancelled().await;
        first.abort();
        let _ = first.await;
        let second = link.stop(false);
        tokio::pin!(second);
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut second)
            .await
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        release.cancel();
        tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .unwrap();
        assert!(link.stop_complete.is_cancelled());
        link.stop(false).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn stale_or_stopped_events_never_reach_the_rejection_owner() {
        let link = test_link();
        link.running.store(true, Ordering::Release);
        link.generation.store(2, Ordering::Release);
        let rejected = Arc::new(Mutex::new(Vec::new()));
        link.set_sub_rejected(Arc::new({
            let rejected = rejected.clone();
            move |generation, text| {
                rejected.lock().unwrap().push((generation, text));
            }
        }));
        let denial = "Permissions Violation for Subscription to \"Mixed.Case\"".to_string();
        link.on_event(1, Event::ServerError(ServerError::Other(denial.clone())));
        link.on_event(
            2,
            Event::ServerError(ServerError::Other(
                "Permissions Violation for Publish to \"Mixed.Case\"".into(),
            )),
        );
        link.on_event(
            2,
            Event::ServerError(ServerError::Other("Maximum Subscriptions Exceeded".into())),
        );
        assert!(rejected.lock().unwrap().is_empty());
        link.on_event(2, Event::ServerError(ServerError::Other(denial.clone())));
        assert_eq!(*rejected.lock().unwrap(), vec![(2, denial.clone())]);
        link.stop(false).await;
        link.on_event(2, Event::ServerError(ServerError::Other(denial)));
        assert_eq!(rejected.lock().unwrap().len(), 1);
    }

    #[test]
    fn reconnect_delay_backoff_bounds() {
        let l = Link::new(
            Options {
                url: "nats://localhost:4222".into(),
                topic: "t".into(),
                ..Default::default()
            }
            .normalize("sub")
            .unwrap(),
            "sub",
            0,
            0,
        );
        assert_eq!(l.reconnect_delay(0), Duration::ZERO);
        assert_eq!(
            l.reconnect_delay(1),
            Duration::ZERO,
            "first connect must not wait"
        );
        for attempts in 2..=100 {
            let d = l.reconnect_delay(attempts);
            assert!(
                d >= RECONNECT_WAIT && d <= RECONNECT_WAIT_MAX + RECONNECT_WAIT_MAX / 2,
                "attempt {attempts}: {d:?}"
            );
        }
        assert!(
            l.reconnect_delay(2) < RECONNECT_WAIT * 2,
            "second attempt starts at the base wait"
        );
        assert!(l.reconnect_delay(1 << 20) > Duration::ZERO);
    }
}
