//! Subscriber side: a self-healing unit that subscribes one topic on one connection (the mirrored
//! constructor holds one connection per endpoint and shares one worker pool). Mirrors the Go
//! `subscriber.go`: three dispatch modes, the Gate, Pause / Resume (v1.5.0), merged cross-endpoint stats.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_nats::{Client, Message};
use futures::future::BoxFuture;
use futures::StreamExt;
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::link::{Link, SetupFn, StatsHook, SubRejectedFn};
use crate::mirror::{mirror_endpoint_name, validate_mirror_urls};
use crate::redact::redact_error;
use crate::{
    DedupSet, Error, Options, Stats, DEFAULT_QUEUE_SIZE, DEFAULT_WORKERS, REBUILD_BACKOFF,
    REBUILD_BACKOFF_MAX, STOP_GRACE,
};

/// Subscription message callback.
///
/// Concurrency and ordering contract: in worker-pool mode the callback runs concurrently on up to
/// `Workers::Pool(n)` tasks and processing order is not guaranteed to match publish order; in
/// per-message mode every message gets a fresh task, concurrency is unbounded and order is likewise
/// not guaranteed. For strict ordering use `Workers::Pool(1)` (ordered, never blocks delivery) or
/// `Workers::Sync` (ordered, but blocks the delivery task).
/// Under a mirrored subscription the callback receives every copy from every endpoint and the
/// ordering contract only holds within a single endpoint; **mirror + Sync is no longer serial**
/// (each of the M endpoints has its own delivery task, so the callback is invoked concurrently).
///
/// `Message.payload` is a `Bytes` owned by that message alone; the callback may keep it after
/// returning without copying.
pub type Handler = Arc<dyn Fn(Message) -> BoxFuture<'static, ()> + Send + Sync>;

/// Callback gate: called once per message at the delivery entry; `false` drops the message on the
/// spot and counts it (`Stats::gated`). Runs on the NATS delivery task, so it must be
/// concurrency-safe and very cheap (an atomic load at most).
pub type Gate = Arc<dyn Fn() -> bool + Send + Sync>;

/// Optional content key. `None` bypasses dedup for this message. Called after Gate;
/// keep it fast and concurrency-safe. Keys in a shared set must use one key space.
pub type DedupKey = Arc<dyn Fn(&Message) -> Option<u64> + Send + Sync>;

/// Convenience wrapper that turns an `Fn(Message) -> impl Future` into a [`Handler`].
pub fn handler<F, Fut>(f: F) -> Handler
where
    F: Fn(Message) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    Arc::new(move |m| Box::pin(f(m)))
}

/// Dispatch mode (the three settings of the Go `Workers` field).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Workers {
    /// Bounded worker pool: n tasks plus a bounded queue (default 8 / 4096). The callback never
    /// blocks the delivery task; when it cannot keep up, new messages are dropped and counted
    /// (`Stats::dropped`, so backpressure is visible). `Pool(0)` = the defaults.
    Pool(usize),
    /// Synchronous: the callback runs directly on the NATS delivery task. Lowest latency; only
    /// suitable for non-blocking, very light handlers.
    Sync,
    /// Per-message: every message is `tokio::spawn`ed as its own task. Unbounded concurrency and
    /// **no backpressure signal at all**. Choose it only for traffic where most messages are cheap
    /// but a few carry blocking work and must not be dropped.
    PerMessage,
}

impl Default for Workers {
    fn default() -> Self {
        Workers::Pool(0)
    }
}

/// Configures one subscription connection.
#[derive(Clone)]
pub struct SubscriberOptions {
    pub options: Options,
    /// Required. Message callback (concurrency contract in [`Handler`]).
    pub handler: Handler,
    /// Optional gate, see [`Gate`]. `None` = always pass. Evaluated before enqueue / dispatch,
    /// identically in all three modes.
    pub gate: Option<Gate>,
    /// Shared FIFO dedup, disabled by default. Runs before queue admission: a dropped
    /// first copy remains marked seen. With no custom key, empty payloads bypass it.
    pub dedup: Option<Arc<DedupSet>>,
    /// Custom key requires `dedup`; `None` from the callback bypasses dedup.
    pub dedup_key: Option<DedupKey>,
    pub workers: Workers,
    /// Buffer length of the worker-pool queue (backpressure bound); 0 = default 4096. A mirrored
    /// subscription shares one queue.
    pub queue_size: usize,
    /// Non-empty selects a queue subscription: subscribers in the same group compete for messages.
    /// Applies per endpoint under mirroring.
    pub queue_group: String,
    /// Receive-buffer cap of the NATS client subscription (a second layer of protection ahead of
    /// this crate's callback; past it async-nats drops messages and bumps `slow_consumers`).
    /// 0 = default 65536. In practice only reached in Sync mode with a slow callback (or an
    /// extreme burst). Counterpart of Go's PendingMsgLimit; async-nats has no byte limit.
    /// Applies per endpoint under mirroring.
    pub pending_msg_limit: usize,
}

impl SubscriberOptions {
    pub fn new(options: Options, handler: Handler) -> Self {
        SubscriberOptions {
            options,
            handler,
            gate: None,
            dedup: None,
            dedup_key: None,
            workers: Workers::default(),
            queue_size: 0,
            queue_group: String::new(),
            pending_msg_limit: 0,
        }
    }
}

enum Dispatch {
    Pool {
        tx: mpsc::Sender<Message>,
        cap: usize,
    },
    Sync,
    PerMessage,
}

struct Reader {
    cancel: CancellationToken,
    join: JoinHandle<()>,
}

/// Pause, resume and each link's setup all read and write the subscription handles; this lock
/// serializes the three.
struct SubState {
    paused: bool,
    /// The most recent connection whose setup succeeded, per link (it may not yet have been swapped
    /// into link.client by the rebuild; during those few microseconds resume can only find it here).
    conns: HashMap<usize, Client>,
    generations: HashMap<usize, u64>,
    epochs: HashMap<usize, u64>,
    rejection_retries: HashMap<usize, CancellationToken>,
    rejections: HashMap<usize, u32>,
    /// The live reader task per link (empty while paused).
    readers: HashMap<usize, Reader>,
    /// Whether the reconcile task is running (same semantics as Go v1.5.1, see `Inner::heal`).
    healing: bool,
}

struct Inner {
    links: Vec<Arc<Link>>,
    topic: String,
    queue_group: String,
    handler: Handler,
    gate: Option<Gate>,
    dedup: Option<Arc<DedupSet>>,
    dedup_key: Option<DedupKey>,
    dispatch: Dispatch,
    workers: usize,
    worker_handles: std::sync::Mutex<Vec<JoinHandle<()>>>,
    /// Start registration and Stop's cancellation must not pass each other.
    lifecycle: std::sync::Mutex<bool>,
    /// Shutdown outlives any caller that cancels its Stop future.
    stop_done: CancellationToken,
    rx: Option<Arc<AsyncMutex<mpsc::Receiver<Message>>>>,
    done: CancellationToken,
    dropped: AtomicU64,
    gated: AtomicU64,
    deduped: AtomicU64,
    pauses: AtomicU64,
    state: AsyncMutex<SubState>,
    paused_flag: std::sync::atomic::AtomicBool,
    /// Endpoints that are not paused and have a registered connection but no reader task
    /// (`Stats::diverged`); recomputed after every state change.
    diverged: AtomicUsize,
    /// Test hook: while >0 the next subscribe_on fails outright and decrements it. Always 0 in
    /// production.
    test_fail_subscribes: AtomicU64,
}

/// Self-healing unit subscribing a single topic. `new` holds one connection; `new_mirrored` holds
/// one connection per endpoint and shares one worker pool and callback. Usage: `new` →
/// `start().await` → `stats()` / `pause()` / `resume()` at any time while running → `stop().await`.
#[derive(Clone)]
pub struct Subscriber {
    inner: Arc<Inner>,
}

impl Subscriber {
    /// Creates the subscription connection (not yet connected; call `start`).
    pub fn new(o: SubscriberOptions) -> Result<Subscriber, Error> {
        let url = o.options.url.clone();
        Self::build(vec![url], o, false)
    }

    /// Creates one subscription connection per endpoint (M endpoints) sharing one worker pool and
    /// callback: the Handler receives every copy from every endpoint, with no dedup by default;
    /// configuring `dedup` filters before dispatch. `workers` is the total concurrency bound across
    /// endpoints. Multi-endpoint connection names are `<name>-e<endpoint index>`.
    pub fn new_mirrored(urls: Vec<String>, o: SubscriberOptions) -> Result<Subscriber, Error> {
        validate_mirror_urls(&urls)?;
        let mirrored = urls.len() > 1;
        Self::build(urls, o, mirrored)
    }

    fn build(urls: Vec<String>, o: SubscriberOptions, mirrored: bool) -> Result<Subscriber, Error> {
        if o.dedup_key.is_some() && o.dedup.is_none() {
            return Err(Error::Config("DedupKey requires Dedup".into()));
        }
        let (dispatch, workers, rx) = match o.workers {
            Workers::Pool(n) => {
                let workers = if n == 0 { DEFAULT_WORKERS } else { n };
                let cap = if o.queue_size == 0 {
                    DEFAULT_QUEUE_SIZE
                } else {
                    o.queue_size
                };
                let (tx, rx) = mpsc::channel(cap);
                (
                    Dispatch::Pool { tx, cap },
                    workers,
                    Some(Arc::new(AsyncMutex::new(rx))),
                )
            }
            Workers::Sync => (Dispatch::Sync, 0, None),
            Workers::PerMessage => (Dispatch::PerMessage, 0, None),
        };

        let base = o.options.name.clone();
        let mut links = Vec::with_capacity(urls.len());
        for (i, u) in urls.iter().enumerate() {
            let mut eo = o.options.clone();
            eo.url = u.clone();
            if mirrored {
                eo.name = mirror_endpoint_name(&base, "sub", &o.options.topic, i);
            }
            links.push(Link::new(
                eo.normalize("sub")?,
                "sub",
                o.pending_msg_limit,
                0,
            ));
        }
        let topic = links[0].o.topic.clone();

        let inner = Arc::new(Inner {
            links,
            topic,
            queue_group: o.queue_group,
            handler: o.handler,
            gate: o.gate,
            dedup: o.dedup,
            dedup_key: o.dedup_key,
            dispatch,
            workers,
            worker_handles: std::sync::Mutex::new(Vec::new()),
            lifecycle: std::sync::Mutex::new(false),
            stop_done: CancellationToken::new(),
            rx,
            done: CancellationToken::new(),
            dropped: AtomicU64::new(0),
            gated: AtomicU64::new(0),
            deduped: AtomicU64::new(0),
            pauses: AtomicU64::new(0),
            state: AsyncMutex::new(SubState {
                paused: false,
                conns: HashMap::new(),
                generations: HashMap::new(),
                epochs: HashMap::new(),
                rejection_retries: HashMap::new(),
                rejections: HashMap::new(),
                readers: HashMap::new(),
                healing: false,
            }),
            paused_flag: std::sync::atomic::AtomicBool::new(false),
            diverged: AtomicUsize::new(0),
            test_fail_subscribes: AtomicU64::new(0),
        });

        for (idx, l) in inner.links.iter().enumerate() {
            let weak = Arc::downgrade(&inner);
            let setup: SetupFn = Arc::new(move |client: Client, generation: u64| {
                let weak = weak.clone();
                Box::pin(async move {
                    match weak.upgrade() {
                        Some(inner) => inner.setup_conn(idx, client, generation).await,
                        None => Err(Error::Stopped),
                    }
                })
            });
            l.set_setup(setup);
            let weak = Arc::downgrade(&inner);
            let rejected: SubRejectedFn = Arc::new(move |generation, err| {
                if let Some(inner) = weak.upgrade() {
                    if rejection_matches(&err, &inner.topic, &inner.queue_group) {
                        tokio::spawn(inner.subscription_rejected(idx, generation));
                    }
                }
            });
            l.set_sub_rejected(rejected);
            // Let each link's built-in status report reflect real subscriber-side backpressure:
            // queue depth, drops, gating and paused state are shared Subscriber-level values.
            let weak = Arc::downgrade(&inner);
            let hook: StatsHook = Arc::new(move |st: &mut Stats| {
                if let Some(inner) = weak.upgrade() {
                    inner.inject_shared_stats(st);
                }
            });
            l.set_stats_hook(hook);
        }
        Ok(Subscriber { inner })
    }

    /// Connects every endpoint and subscribes. A temporarily unreachable server is not an error
    /// (retried in the background; the subscription takes effect once connected); an error from
    /// any endpoint (a configuration error) rolls back the endpoints already started.
    pub async fn start(&self) -> Result<(), Error> {
        let inner = &self.inner;
        {
            let mut started = inner.lifecycle.lock().unwrap();
            if inner.done.is_cancelled() {
                return Err(Error::Stopped);
            }
            if *started {
                return Err(Error::AlreadyStarted);
            }
            *started = true;
            // A healthy endpoint can deliver while another endpoint is still dialing.
            if let Some(rx) = &inner.rx {
                let mut handles = inner.worker_handles.lock().unwrap();
                for _ in 0..inner.workers {
                    let (inner, rx) = (inner.clone(), rx.clone());
                    handles.push(tokio::spawn(async move { inner.worker(rx).await }));
                }
            }
        }
        let subscriber = self.clone();
        let (send, receive) = tokio::sync::oneshot::channel();
        // Cancelling one Start waiter must not abandon endpoints beyond the
        // parallelism window or skip rollback after another endpoint fails.
        tokio::spawn(async move {
            let result = subscriber.start_links().await;
            let _ = send.send(result);
        });
        receive
            .await
            .unwrap_or_else(|_| Err(Error::Nats("subscriber startup task failed".into())))
    }

    async fn start_links(&self) -> Result<(), Error> {
        let inner = &self.inner;
        let results = futures::stream::iter(
            inner
                .links
                .clone()
                .into_iter()
                .map(|link| async move { link.start().await }),
        )
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await;
        let errors: Vec<_> = results.into_iter().filter_map(Result::err).collect();
        if !errors.is_empty() {
            self.stop().await;
            return Err(if errors.len() == 1 {
                errors.into_iter().next().unwrap()
            } else {
                Error::Multiple(errors)
            });
        }
        if inner.done.is_cancelled() {
            return Err(Error::Stopped);
        }
        Ok(())
    }

    /// Same as start, but panics on failure. Note that must_start succeeding does not mean
    /// connected; for fail-fast startup call `must_probe` first.
    pub async fn must_start(&self) {
        if let Err(e) = self.start().await {
            panic!("{e}");
        }
    }

    /// Closes every endpoint connection, ends the reader and worker tasks and waits for callbacks
    /// in flight in the worker pool to return (Sync / PerMessage callbacks are not awaited). Each
    /// endpoint close waits at most STOP_GRACE. Idempotent; the instance cannot be reused afterwards.
    pub async fn stop(&self) {
        let inner = &self.inner;
        {
            let _lifecycle = inner.lifecycle.lock().unwrap();
            if !inner.done.is_cancelled() {
                inner.done.cancel();
                let inner = inner.clone();
                tokio::spawn(async move {
                    inner.shutdown().await;
                    inner.stop_done.cancel();
                });
            }
        }
        inner.stop_done.cancelled().await;
    }

    /// Number of endpoints (the number of urls for the mirrored constructor, always 1 otherwise).
    pub fn endpoints(&self) -> usize {
        self.inner.links.len()
    }

    pub fn topic(&self) -> &str {
        &self.inner.topic
    }

    /// Whether **all** endpoints are connected; a degraded endpoint must be visible.
    pub fn is_connected(&self) -> bool {
        !self.inner.links.is_empty() && self.inner.links.iter().all(|l| l.is_connected())
    }

    /// Blocks until every endpoint is connected; on timeout returns the error of the first endpoint
    /// not yet ready.
    pub async fn wait_connected(&self, timeout: Duration) -> Result<(), Error> {
        let deadline = tokio::time::Instant::now() + timeout;
        for l in &self.inner.links {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            l.wait_connected(left).await?;
        }
        Ok(())
    }

    /// Underlying connection of the first endpoint (escape hatch).
    pub fn client(&self) -> Option<Client> {
        self.inner.links[0].client()
    }

    /// Per-endpoint stats snapshots (same order as urls; always one entry for the plain
    /// constructor). Use it to attribute a degraded endpoint under mirroring.
    pub fn endpoint_stats(&self) -> Vec<Stats> {
        self.inner.links.iter().map(|l| l.stats()).collect()
    }

    /// Stats snapshot merged across endpoints: connected is true only when every endpoint is up;
    /// counters are summed; dropped / gated / paused are shared Subscriber-level values (counted
    /// per copy under mirroring).
    pub fn stats(&self) -> Stats {
        let inner = &self.inner;
        let mut out = Stats {
            status: "no-conn".into(),
            ..Stats::default()
        };
        inner.inject_shared_stats(&mut out);
        if let Some(l) = inner.links.first() {
            out.name = l.o.name.clone();
            out.running = l.is_running();
        }
        let mut all_connected = !inner.links.is_empty();
        for l in &inner.links {
            let st = l.raw_stats();
            if st.connected {
                if !out.connected {
                    out.connected = true;
                    out.status = st.status.clone();
                    out.server = st.server.clone();
                    out.cluster = st.cluster.clone();
                }
            } else {
                all_connected = false;
                if out.status == "no-conn" {
                    out.status = st.status.clone();
                }
            }
            out.reconnects += st.reconnects;
            out.rebuilds += st.rebuilds;
            out.in_msgs += st.in_msgs;
            out.out_msgs += st.out_msgs;
            out.in_bytes += st.in_bytes;
            out.out_bytes += st.out_bytes;
            out.slow_consumers += st.slow_consumers;
        }
        out.connected = out.connected && all_connected;
        out
    }

    /// Pauses the subscription while keeping the connection (v1.5.0): unsubscribes on each
    /// endpoint's current connection, so the server stops delivering to this connection at once.
    /// Interest is withdrawn hop by hop along leafnode and route connections and upstream servers
    /// stop forwarding this stream for this client, which saves real bandwidth (a Gate only drops
    /// at the delivery entry; the bytes still reach the NIC). Connection, heartbeats, auto-reconnect
    /// and self-healing rebuilds all continue. Idempotent; returns `Error::Stopped` after stop.
    /// A self-healing rebuild during the pause still swaps in a new connection, it just does not
    /// subscribe; auto-reconnect replays only subscriptions still on record.
    /// Messages already in flight before the UNSUB and messages already queued for the worker pool
    /// are still delivered to the callback.
    pub async fn pause(&self) -> Result<(), Error> {
        let inner = &self.inner;
        let mut st = inner.state.lock().await;
        if inner.done.is_cancelled() {
            return Err(Error::Stopped);
        }
        if st.paused {
            return Ok(());
        }
        st.paused = true;
        inner.paused_flag.store(true, Ordering::Release);
        inner.pauses.fetch_add(1, Ordering::Relaxed);
        for epoch in st.epochs.values_mut() {
            *epoch += 1;
        }
        for (_, retry) in st.rejection_retries.drain() {
            retry.cancel();
        }
        let readers: Vec<Reader> = st.readers.drain().map(|(_, r)| r).collect();
        inner.recount(&st);
        // Cancel every reader before the first await: dropping a Pause waiter
        // must not leave later endpoints alive after their handles were removed.
        for reader in &readers {
            reader.cancel.cancel();
        }
        drop(st);
        for r in readers {
            let _ = tokio::time::timeout(STOP_GRACE, r.join).await; // the reader task unsubscribes before it exits
        }
        Ok(())
    }

    /// Resumes (v1.5.0): re-subscribes on each endpoint's most recent successfully set-up
    /// connection and delivery resumes to the same Handler. Idempotent; returns `Error::Stopped`
    /// after stop. An endpoint inside a rebuild window is skipped (the setup of the swapped-in
    /// connection subscribes as not-paused). If an endpoint fails to subscribe, the paused state is
    /// still cleared and the error still returned; a reconcile task inside the crate retries that
    /// endpoint with the rebuild backoff (2s doubling, capped at 30s) until it succeeds, a new
    /// pause, or stop (same semantics as Go v1.5.1; before that a healthy connection that was never
    /// rebuilt left the endpoint unsubscribed for good). Logs
    /// `[natslink] subscribe retried after failed resume`; `stats().diverged > 0` until it
    /// converges. Callers need not retry.
    pub async fn resume(&self) -> Result<(), Error> {
        let subscriber = self.clone();
        let (send, receive) = tokio::sync::oneshot::channel();
        // Reconcile and healer registration are one operation. Cancelling the
        // caller must not leave paused=false with partially restored readers.
        tokio::spawn(async move {
            let result = subscriber.resume_inner().await;
            let _ = send.send(result);
        });
        receive
            .await
            .unwrap_or_else(|_| Err(Error::Nats("subscriber resume task failed".into())))
    }

    async fn resume_inner(&self) -> Result<(), Error> {
        let inner = &self.inner;
        let mut st = inner.state.lock().await;
        if inner.done.is_cancelled() {
            return Err(Error::Stopped);
        }
        if !st.paused {
            return Ok(());
        }
        st.paused = false;
        inner.paused_flag.store(false, Ordering::Release);
        let mut errs = Vec::new();
        for idx in 0..inner.links.len() {
            if st.readers.contains_key(&idx) {
                continue; // should not happen; defensive guard against a double subscription
            }
            let Some(client) = st.conns.get(&idx).cloned() else {
                continue;
            };
            match inner.subscribe_on(idx, &client).await {
                Ok(r) => {
                    *st.epochs.entry(idx).or_default() += 1;
                    st.readers.insert(idx, r);
                }
                Err(e) => errs.push(e),
            }
        }
        inner.recount(&st);
        if !errs.is_empty() && !st.healing {
            st.healing = true;
            tokio::spawn(Inner::heal(Arc::downgrade(inner)));
        }
        if errs.is_empty() {
            Ok(())
        } else if errs.len() == 1 {
            Err(errs.pop().unwrap())
        } else {
            Err(Error::Multiple(errs))
        }
    }

    /// Whether currently paused.
    pub fn is_paused(&self) -> bool {
        self.inner.paused_flag.load(Ordering::Acquire)
    }

    /// Test hook: makes the next n subscribes fail outright (used by integration tests to inject
    /// resume failures). Do not call in production.
    #[doc(hidden)]
    pub fn fail_next_subscribes_for_test(&self, n: u64) {
        self.inner.test_fail_subscribes.store(n, Ordering::Release);
    }
}

impl Inner {
    async fn shutdown(&self) {
        let readers = {
            let mut st = self.state.lock().await;
            for (_, retry) in st.rejection_retries.drain() {
                retry.cancel();
            }
            let readers: Vec<Reader> = st.readers.drain().map(|(_, r)| r).collect();
            for reader in &readers {
                reader.cancel.cancel();
            }
            st.conns.clear();
            st.generations.clear();
            self.recount(&st);
            readers
        };
        futures::join!(
            futures::future::join_all(readers.into_iter().map(|r| async move {
                let _ = tokio::time::timeout(STOP_GRACE, r.join).await;
            })),
            futures::future::join_all(self.links.iter().map(|l| l.stop(false))),
        );
        let handles = std::mem::take(&mut *self.worker_handles.lock().unwrap());
        for handle in handles {
            let _ = handle.await;
        }
    }

    /// Completes the subscription on every new connection (initial or self-healing rebuild); while
    /// paused it only registers the connection without subscribing.
    async fn setup_conn(
        self: Arc<Self>,
        idx: usize,
        client: Client,
        generation: u64,
    ) -> Result<(), Error> {
        let mut st = self.state.lock().await;
        if self.done.is_cancelled() {
            return Err(Error::Stopped);
        }
        if let Some(retry) = st.rejection_retries.remove(&idx) {
            retry.cancel();
        }
        st.generations.insert(idx, generation);
        *st.epochs.entry(idx).or_default() += 1;
        st.conns.insert(idx, client.clone());
        // The reader on the old connection dies with it: remove it first so pause does not wait on
        // a dead connection.
        if let Some(old) = st.readers.remove(&idx) {
            old.cancel.cancel();
        }
        if st.paused {
            self.recount(&st);
            return Ok(());
        }
        let res = self.subscribe_on(idx, &client).await;
        if let Ok(r) = res {
            st.readers.insert(idx, r);
            self.recount(&st);
            return Ok(());
        }
        // A connection whose subscribe failed is discarded and redialed by the link: keep it out of
        // conns so resume / reconcile do not retry on a dead connection.
        st.conns.remove(&idx);
        self.recount(&st);
        res.map(|_| ())
    }

    /// async-nats has no read-loop LastError / PING-PONG probe API. A matching
    /// rejection therefore schedules only this endpoint's reconnection, preserving
    /// the current reader until replacement. A delayed same-generation error can
    /// cause an unnecessary reconnect; unlike Go we cannot prove ACL state here.
    async fn subscription_rejected(self: Arc<Self>, idx: usize, generation: u64) {
        let mut st = self.state.lock().await;
        if self.done.is_cancelled()
            || st.paused
            || st.generations.get(&idx) != Some(&generation)
            || self.links[idx].generation() != generation
            || st.rejection_retries.contains_key(&idx)
        {
            return;
        }
        let count = st.rejections.entry(idx).or_default();
        *count = count.saturating_add(1);
        let count = *count;
        let wait = rejection_backoff(count);
        let epoch = st.epochs[&idx];
        let cancel = CancellationToken::new();
        st.rejection_retries.insert(idx, cancel.clone());
        self.recount(&st);
        let link = &self.links[idx];
        warn!(client = %link.o.name, topic = %self.topic, rejections = count,
            next_wait = ?wait, reason = "subscription_permission",
            "[natslink] subscription rejected by server, retrying");
        drop(st);
        tokio::select! {
            biased;
            _ = self.done.cancelled() => return,
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(wait) => {},
        }
        let st = self.state.lock().await;
        if !self.done.is_cancelled()
            && !st.paused
            && !cancel.is_cancelled()
            && st.generations.get(&idx) == Some(&generation)
            && st.epochs.get(&idx) == Some(&epoch)
            && self.links[idx].generation() == generation
        {
            self.links[idx].request_rebuild_now();
        }
    }

    /// Recomputes the number of unaligned endpoints. Caller holds the state lock.
    fn recount(&self, st: &SubState) {
        let n = if st.paused {
            0 // pause cancels the readers directly; there is no failure path
        } else {
            st.conns
                .keys()
                .filter(|idx| {
                    !st.readers.contains_key(idx) || st.rejection_retries.contains_key(idx)
                })
                .count()
        };
        self.diverged.store(n, Ordering::Release);
    }

    /// Reconcile task, started only when resume leaves endpoints unsubscribed (single flight:
    /// `healing` is maintained under the state lock). Retries with the rebuild backoff and
    /// re-checks the paused state each round: exits at once if paused again; exits once every
    /// endpoint is restored, or after stop / the Subscriber being dropped. Holds only a Weak, so it
    /// does not extend Inner's lifetime. Like resume / setup_conn it holds the state lock across the
    /// subscribe await, so pause / resume / setup_conn queue behind it while it runs (serialized,
    /// matching Go's subMu).
    async fn heal(weak: Weak<Self>) {
        let mut backoff = REBUILD_BACKOFF;
        loop {
            let done = match weak.upgrade() {
                Some(inner) => inner.done.clone(),
                None => return,
            };
            tokio::select! {
                _ = done.cancelled() => {}
                _ = tokio::time::sleep(backoff) => {}
            }
            let Some(inner) = weak.upgrade() else { return };
            let mut st = inner.state.lock().await;
            if inner.done.is_cancelled() || st.paused {
                st.healing = false;
                inner.recount(&st);
                return;
            }
            let mut failed = 0usize;
            let next_wait = (backoff * 2).min(REBUILD_BACKOFF_MAX);
            for idx in 0..inner.links.len() {
                if st.readers.contains_key(&idx) {
                    continue;
                }
                let Some(client) = st.conns.get(&idx).cloned() else {
                    continue;
                };
                match inner.subscribe_on(idx, &client).await {
                    Ok(r) => {
                        *st.epochs.entry(idx).or_default() += 1;
                        st.readers.insert(idx, r);
                        let link = &inner.links[idx];
                        info!(client = %link.o.name, topic = %link.o.topic, "[natslink] subscribe retried after failed resume");
                    }
                    Err(e) => {
                        failed += 1;
                        // One line per failed endpoint (as in Go): under mirroring only client identifies
                        // which end. Reconcile retries only while not paused (it exits once paused), so
                        // paused is always false; the field is kept under the same key as Go.
                        let link = &inner.links[idx];
                        warn!(client = %link.o.name, topic = %link.o.topic, paused = false,
                            next_wait = ?next_wait, err = %e, "[natslink] subscription reconcile failed, retrying");
                    }
                }
            }
            inner.recount(&st);
            if failed == 0 {
                st.healing = false;
                return;
            }
            drop(st);
            backoff = next_wait;
        }
    }

    /// Subscribes this topic on client and spawns a reader task. Caller holds the state lock.
    // `fetch_update` is deprecated in favour of `try_update` on newer toolchains; the
    // replacement is not available on the minimum supported Rust version yet.
    #[allow(deprecated)]
    async fn subscribe_on(self: &Arc<Self>, idx: usize, client: &Client) -> Result<Reader, Error> {
        let link = &self.links[idx];
        if self
            .test_fail_subscribes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(Error::Nats(format!(
                "{} subscribe: injected failure",
                link.o.name
            )));
        }
        let subscribe = async {
            if self.queue_group.is_empty() {
                client.subscribe(self.topic.clone()).await
            } else {
                client
                    .queue_subscribe(self.topic.clone(), self.queue_group.clone())
                    .await
            }
        };
        let sub = tokio::select! {
            biased;
            _ = self.done.cancelled() => return Err(Error::Stopped),
            result = subscribe => result,
        }
        .map_err(|e| {
            Error::Nats(format!(
                "{} subscribe: {}",
                link.o.name,
                redact_error(&link.o.url, &e.to_string())
            ))
        })?;
        let cancel = CancellationToken::new();
        let inner = Arc::clone(self);
        let c2 = cancel.clone();
        let generation = link.generation();
        let join = tokio::spawn(async move { inner.reader(idx, generation, sub, c2).await });
        Ok(Reader { cancel, join })
    }

    /// One reader task per endpoint: hands messages to dispatch; unsubscribes before exiting when
    /// cancelled; an unexpectedly ended stream (connection closed for good) triggers a rebuild of
    /// that link.
    async fn reader(
        self: Arc<Self>,
        idx: usize,
        generation: u64,
        mut sub: async_nats::Subscriber,
        cancel: CancellationToken,
    ) {
        let mut saw_message = false;
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    // A disconnected, full async-nats command queue can stall
                    // UNSUB. Bound our reader's lifetime; upstream Drop may still
                    // enqueue its own best-effort unsubscribe in the background.
                    let _ = tokio::time::timeout(STOP_GRACE, sub.unsubscribe()).await;
                    return;
                }
                next = sub.next() => match next {
                    Some(msg) => {
                        self.links[idx].record_receive(&msg);
                        if !saw_message {
                            saw_message = true;
                            let mut st = self.state.lock().await;
                            if st.generations.get(&idx) == Some(&generation) {
                                st.rejections.remove(&idx);
                            }
                        }
                        self.dispatch(msg).await;
                    },
                    None => {
                        if !cancel.is_cancelled() && !self.done.is_cancelled() && !self.paused_flag.load(Ordering::Acquire) {
                            let link = &self.links[idx];
                            warn!(client = %link.o.name, topic = %link.o.topic, "[natslink] ⛔ subscription stream ended, rebuilding");
                            link.request_rebuild();
                        }
                        return;
                    }
                },
            }
        }
    }

    /// The gate decides first, identically in all three modes: a gated message is not queued,
    /// spawns no task and reaches no callback; it is only counted.
    async fn dispatch(&self, msg: Message) {
        if let Some(g) = &self.gate {
            if !g() {
                self.gated.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        if let Some(dedup) = &self.dedup {
            let seen = if let Some(key) = &self.dedup_key {
                key(&msg).is_some_and(|key| dedup.seen_key(key))
            } else {
                !msg.payload.is_empty() && dedup.seen(&msg.payload)
            };
            if seen {
                self.deduped.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        match &self.dispatch {
            Dispatch::Pool { tx, .. } => {
                if tx.try_send(msg).is_err() {
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
            Dispatch::PerMessage => {
                if self.done.is_cancelled() {
                    return;
                }
                let fut = (self.handler)(msg);
                tokio::spawn(fut);
            }
            Dispatch::Sync => {
                if self.done.is_cancelled() {
                    return;
                }
                (self.handler)(msg).await;
            }
        }
    }

    async fn worker(self: Arc<Self>, rx: Arc<AsyncMutex<mpsc::Receiver<Message>>>) {
        loop {
            if self.done.is_cancelled() {
                return;
            }
            let msg = {
                let mut guard = rx.lock().await;
                tokio::select! {
                    _ = self.done.cancelled() => return,
                    m = guard.recv() => m,
                }
            };
            match msg {
                Some(m) => (self.handler)(m).await,
                None => return,
            }
        }
    }

    fn inject_shared_stats(&self, st: &mut Stats) {
        st.dropped = self.dropped.load(Ordering::Relaxed);
        st.gated = self.gated.load(Ordering::Relaxed);
        st.deduped = self.deduped.load(Ordering::Relaxed);
        st.paused = self.paused_flag.load(Ordering::Acquire);
        st.pauses = self.pauses.load(Ordering::Relaxed);
        st.diverged = self.diverged.load(Ordering::Acquire);
        if let Dispatch::Pool { tx, cap } = &self.dispatch {
            st.queue_cap = *cap;
            st.queue_len = cap.saturating_sub(tx.capacity());
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.done.cancel();
        info!(topic = %self.topic, "[natslink] subscriber dropped");
    }
}

fn rejection_backoff(count: u32) -> Duration {
    REBUILD_BACKOFF
        .saturating_mul(1u32 << count.saturating_sub(1).min(4))
        .min(REBUILD_BACKOFF_MAX)
}

/// NATS permission errors quote subject/queue with Go's %q, not JSON escaping.
/// Match the entire grammar, so publish errors, foreign queues and limits cannot
/// disconnect a healthy endpoint. Values may contain literal backslashes/quotes.
fn rejection_matches(error: &str, topic: &str, queue: &str) -> bool {
    let Some(rest) = error.strip_prefix("Permissions Violation for Subscription to ") else {
        return false;
    };
    let Some((subject, rest)) = unquote_go(rest) else {
        return false;
    };
    if subject != topic {
        return false;
    }
    if queue.is_empty() {
        return rest.is_empty();
    }
    let Some(rest) = rest.strip_prefix(" using queue ") else {
        return false;
    };
    let Some((actual_queue, rest)) = unquote_go(rest) else {
        return false;
    };
    actual_queue == queue && rest.is_empty()
}

fn unquote_go(input: &str) -> Option<(String, &str)> {
    let mut chars = input.strip_prefix('"')?.char_indices();
    let mut out = Vec::new();
    while let Some((offset, ch)) = chars.next() {
        match ch {
            '"' => return Some((String::from_utf8(out).ok()?, &input[offset + 2..])),
            '\n' | '\r' => return None,
            '\\' => {
                let (_, escaped) = chars.next()?;
                match escaped {
                    'a' => out.push(7),
                    'b' => out.push(8),
                    'f' => out.push(12),
                    'n' => out.push(b'\n'),
                    'r' => out.push(b'\r'),
                    't' => out.push(b'\t'),
                    'v' => out.push(11),
                    '\\' => out.push(b'\\'),
                    '"' => out.push(b'"'),
                    'x' | 'u' | 'U' => {
                        let count = match escaped {
                            'x' => 2,
                            'u' => 4,
                            _ => 8,
                        };
                        let mut value = 0u32;
                        for _ in 0..count {
                            value = value.checked_mul(16)? + chars.next()?.1.to_digit(16)?;
                        }
                        if escaped == 'x' {
                            out.push(value as u8);
                        } else {
                            out.extend_from_slice(
                                char::from_u32(value)?.encode_utf8(&mut [0; 4]).as_bytes(),
                            );
                        }
                    }
                    '0'..='7' => {
                        let mut value = escaped.to_digit(8)?;
                        for _ in 0..2 {
                            value = value * 8 + chars.next()?.1.to_digit(8)?;
                        }
                        if value > 255 {
                            return None;
                        }
                        out.push(value as u8);
                    }
                    _ => return None,
                }
            }
            ch => out.extend_from_slice(ch.encode_utf8(&mut [0; 4]).as_bytes()),
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use tokio::sync::Notify;

    fn options(topic: &str, count: Arc<AtomicU64>) -> SubscriberOptions {
        let mut options = SubscriberOptions::new(
            Options {
                url: "nats://127.0.0.1:1".into(),
                topic: topic.into(),
                ..Options::default()
            },
            handler(move |_| {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::Relaxed);
                }
            }),
        );
        options.workers = Workers::Sync;
        options
    }

    fn message(subject: &str, data: &'static [u8]) -> Message {
        Message {
            subject: subject.to_owned().into(),
            reply: None,
            payload: data.into(),
            headers: None,
            status: None,
            description: None,
            length: data.len(),
        }
    }

    #[tokio::test]
    async fn gate_precedes_shared_dedup_across_subjects() {
        let count = Arc::new(AtomicU64::new(0));
        let gate = Arc::new(AtomicBool::new(false));
        let dedup = Arc::new(DedupSet::new(4, Duration::ZERO));
        let mut a = options("a", count.clone());
        a.dedup = Some(dedup.clone());
        a.gate = Some({
            let gate = gate.clone();
            Arc::new(move || gate.load(Ordering::Relaxed))
        });
        let a = Subscriber::new(a).unwrap();
        let mut b = options("b", count.clone());
        b.dedup = Some(dedup);
        let b = Subscriber::new(b).unwrap();
        a.inner.dispatch(message("a", b"same")).await;
        b.inner.dispatch(message("b", b"same")).await;
        gate.store(true, Ordering::Relaxed);
        a.inner.dispatch(message("a", b"same")).await;
        a.inner.dispatch(message("a", b"different")).await;
        assert_eq!(count.load(Ordering::Relaxed), 2);
        assert_eq!(a.stats().gated, 1);
        assert_eq!(a.stats().deduped, 1);
        assert_eq!(b.stats().deduped, 0);
        a.stop().await;
        b.stop().await;
    }

    #[tokio::test]
    async fn full_queue_marks_before_enqueue_and_empty_default_bypasses() {
        let count = Arc::new(AtomicU64::new(0));
        let mut o = options("a", count);
        o.dedup = Some(Arc::new(DedupSet::new(4, Duration::ZERO)));
        o.workers = Workers::Pool(1);
        o.queue_size = 1;
        let sub = Subscriber::new(o).unwrap(); // no workers yet: fill deterministically
        sub.inner.dispatch(message("a", b"first")).await;
        sub.inner.dispatch(message("a", b"dropped")).await;
        sub.inner.dispatch(message("a", b"dropped")).await;
        sub.inner.dispatch(message("a", b"")).await;
        sub.inner.dispatch(message("a", b"")).await;
        assert_eq!(sub.stats().queue_len, 1);
        assert_eq!(sub.stats().dropped, 3);
        assert_eq!(sub.stats().deduped, 1);
        sub.stop().await;
    }

    #[tokio::test]
    async fn custom_key_bypass_and_empty_payload_in_all_dispatch_modes() {
        for mode in [Workers::Sync, Workers::PerMessage, Workers::Pool(1)] {
            let count = Arc::new(AtomicU64::new(0));
            let mut o = options("a", count.clone());
            o.workers = mode;
            o.dedup_key = Some(Arc::new(|m| {
                if m.payload == b"skip"[..] {
                    None
                } else {
                    Some(7)
                }
            }));
            assert!(matches!(Subscriber::new(o.clone()), Err(Error::Config(_))));
            o.dedup = Some(Arc::new(DedupSet::new(4, Duration::ZERO)));
            let sub = Subscriber::new(o).unwrap();
            if let Some(rx) = &sub.inner.rx {
                let (inner, rx) = (sub.inner.clone(), rx.clone());
                sub.inner
                    .worker_handles
                    .lock()
                    .unwrap()
                    .push(tokio::spawn(async move { inner.worker(rx).await }));
            }
            for data in [b"".as_slice(), b"different", b"skip", b"skip"] {
                sub.inner.dispatch(message("a", data)).await;
            }
            tokio::time::timeout(Duration::from_secs(1), async {
                while count.load(Ordering::Relaxed) != 3 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(sub.stats().deduped, 1);
            sub.stop().await;
        }
    }

    #[tokio::test]
    async fn singleton_mirror_degrades_and_stop_before_start_is_terminal() {
        let o = options("a", Arc::new(AtomicU64::new(0)));
        let direct = Subscriber::new(o.clone()).unwrap();
        let mirror = Subscriber::new_mirrored(vec![o.options.url.clone()], o).unwrap();
        assert_eq!(mirror.stats().name, direct.stats().name);
        mirror.stop().await;
        assert!(matches!(mirror.start().await, Err(Error::Stopped)));
        direct.stop().await;
    }

    #[tokio::test]
    async fn cancelled_first_stop_still_waits_for_inflight_worker() {
        let sub = Subscriber::new(options("a", Arc::new(AtomicU64::new(0)))).unwrap();
        let finish = Arc::new(Notify::new());
        let notified = finish.clone();
        sub.inner
            .worker_handles
            .lock()
            .unwrap()
            .push(tokio::spawn(async move {
                notified.notified().await;
            }));
        let first = tokio::spawn({
            let sub = sub.clone();
            async move { sub.stop().await }
        });
        sub.inner.done.cancelled().await;
        first.abort();
        let mut second = Box::pin(sub.stop());
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut second)
            .await
            .is_err());
        finish.notify_one();
        tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .unwrap();
    }

    #[test]
    fn permissions_match_only_exact_subscription_and_queue() {
        assert!(rejection_matches(
            r#"Permissions Violation for Subscription to "foo""#,
            "foo",
            ""
        ));
        assert!(rejection_matches(
            r#"Permissions Violation for Subscription to "a\\b" using queue "q\"g""#,
            "a\\b",
            "q\"g"
        ));
        for error in [
            r#"Permissions Violation for Publish to "foo""#,
            r#"Permissions Violation for Subscription to "other""#,
            r#"Permissions Violation for Subscription to "foo" using queue "x""#,
            r#"Permissions Violation for Subscription to "foo" suffix"#,
            "Maximum Subscriptions Exceeded",
            "Slow Consumer",
        ] {
            assert!(!rejection_matches(error, "foo", ""), "{error}");
        }
        assert!(!rejection_matches(
            r#"Permissions Violation for Subscription to "foo""#,
            "foo",
            "q"
        ));
        assert!(rejection_matches(
            r#"Permissions Violation for Subscription to "\xc3\xa9""#,
            "é",
            ""
        ));
        assert!(!rejection_matches(
            r#"Permissions Violation for Subscription to "\xzz""#,
            "a",
            ""
        ));
        assert!(!rejection_matches(
            r#"Permissions Violation for Subscription to "\uD800""#,
            "a",
            ""
        ));
        assert_eq!(rejection_backoff(1), Duration::from_secs(2));
        assert_eq!(rejection_backoff(2), Duration::from_secs(4));
        assert_eq!(rejection_backoff(3), Duration::from_secs(8));
        assert_eq!(rejection_backoff(4), Duration::from_secs(16));
        assert_eq!(rejection_backoff(1000), Duration::from_secs(30));
    }

    #[tokio::test]
    async fn rejection_generation_coalescing_and_pause_cancel() {
        let sub = Subscriber::new(options("a", Arc::new(AtomicU64::new(0)))).unwrap();
        {
            let mut st = sub.inner.state.lock().await;
            st.generations.insert(0, 0);
            st.epochs.insert(0, 1);
        }
        sub.inner.clone().subscription_rejected(0, 10).await;
        assert!(sub.inner.state.lock().await.rejection_retries.is_empty());
        let pending = tokio::spawn(sub.inner.clone().subscription_rejected(0, 0));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !sub.inner.state.lock().await.rejection_retries.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        sub.inner.clone().subscription_rejected(0, 0).await;
        assert_eq!(sub.inner.state.lock().await.rejections[&0], 1);
        sub.pause().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .unwrap()
            .unwrap();
        assert!(sub.inner.state.lock().await.rejection_retries.is_empty());
        sub.stop().await;
    }
    #[tokio::test]
    async fn cancelled_start_waiter_does_not_abandon_remaining_endpoints() {
        let urls = (1..=9)
            .map(|port| format!("nats://127.0.0.1:{port}"))
            .collect();
        let sub =
            Subscriber::new_mirrored(urls, options("a", Arc::new(AtomicU64::new(0)))).unwrap();
        let arrived = Arc::new(AtomicU64::new(0));
        let release = Arc::new(Notify::new());
        for link in &sub.inner.links {
            link.set_setup(Arc::new({
                let (arrived, release) = (arrived.clone(), release.clone());
                move |_, _| {
                    let (arrived, release) = (arrived.clone(), release.clone());
                    Box::pin(async move {
                        let index = arrived.fetch_add(1, Ordering::Relaxed);
                        if index < 8 {
                            release.notified().await;
                        }
                        Ok(())
                    })
                }
            }));
        }
        let start = tokio::spawn({
            let sub = sub.clone();
            async move { sub.start().await }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while arrived.load(Ordering::Relaxed) < 8 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        start.abort();
        for _ in 0..8 {
            release.notify_one();
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while arrived.load(Ordering::Relaxed) < 9 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(sub.start().await, Err(Error::AlreadyStarted)));
        sub.stop().await;
    }
    #[tokio::test]
    async fn cancelled_pause_cancels_every_removed_reader() {
        let sub = Subscriber::new(options("a", Arc::new(AtomicU64::new(0)))).unwrap();
        let mut tokens = Vec::new();
        {
            let mut st = sub.inner.state.lock().await;
            for index in 0..3 {
                let cancel = CancellationToken::new();
                tokens.push(cancel.clone());
                let wait = cancel.clone();
                st.readers.insert(
                    index,
                    Reader {
                        cancel,
                        join: tokio::spawn(async move {
                            wait.cancelled().await;
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }),
                    },
                );
            }
        }
        let pause = tokio::spawn({
            let sub = sub.clone();
            async move { sub.pause().await }
        });
        while !sub.is_paused() {
            tokio::task::yield_now().await;
        }
        pause.abort();
        assert!(tokens.iter().all(CancellationToken::is_cancelled));
        assert!(sub.inner.state.lock().await.readers.is_empty());
        sub.stop().await;
    }

    #[tokio::test]
    async fn cancelled_resume_waiter_does_not_abandon_reconciliation() {
        let sub = Subscriber::new(options("a", Arc::new(AtomicU64::new(0)))).unwrap();
        sub.pause().await.unwrap();
        let state = sub.inner.state.lock().await;
        let resume = tokio::spawn({
            let sub = sub.clone();
            async move { sub.resume().await }
        });
        // On this single-thread runtime the waiter reaches its oneshot await;
        // the owned operation then waits for the state lock held by this test.
        tokio::task::yield_now().await;
        resume.abort();
        drop(state);
        tokio::time::timeout(Duration::from_secs(1), async {
            while sub.is_paused() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        sub.stop().await;
    }

    include!("../tests/support/subscriber_contract.rs");
}
