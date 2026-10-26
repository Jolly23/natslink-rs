//! Publishing side: one connection publishing one topic (`Publisher`), and `PublisherPool`,
//! N connections with round-robin routing and health-first failover (mirrored construction:
//! one group per endpoint, `publish` sends once to every endpoint). Mirrors `publisher.go`
//! in the Go module.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_nats::client::PublishErrorKind;
use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::link::Link;
use crate::mirror::{mirror_endpoint_name, validate_mirror_urls};
use crate::redact::redact_error;
use crate::{Error, Options, Stats, STOP_GRACE};

/// Configuration for one publishing connection.
#[derive(Clone, Default)]
pub struct PublisherOptions {
    pub options: Options,
    /// When > 0, messages larger than this many bytes are rejected outright
    /// (`Error::Oversized`, counted as dropped). 0 = no application-level limit.
    pub max_payload: usize,
    /// Outbound command queue capacity, in messages. 0 = default 1024. This is how many
    /// messages can pile up while disconnected and therefore a multiplier on the memory
    /// bound: topics with large payloads should use a small value, otherwise a handful of
    /// queued multi-megabyte messages pin that much memory.
    pub queue_capacity: usize,
}

/// A self-supervising connection dedicated to publishing on a single topic.
/// Usage: `new` → `start().await` → `publish(...)` → `stop().await`.
#[derive(Clone)]
pub struct Publisher {
    link: Arc<Link>,
    subject: async_nats::Subject,
    max_payload: usize,
}

impl Publisher {
    pub fn new(o: PublisherOptions) -> Result<Publisher, Error> {
        let opts = o.options.normalize("pub")?;
        Ok(Publisher {
            subject: async_nats::Subject::from(opts.topic.clone()),
            link: Link::new(opts, "pub", 0, o.queue_capacity),
            max_payload: o.max_payload,
        })
    }

    /// One publishing connection per independent endpoint (at most five).
    /// A singleton URL is exactly `PublisherPool::new(1, ...)`, including naming
    /// and synchronous publishing; multiple endpoints have independent send lines.
    pub fn new_mirrored(urls: Vec<String>, o: PublisherOptions) -> Result<PublisherPool, Error> {
        PublisherPool::mirrored(urls, 1, o, true)
    }

    /// Establish the connection. A temporarily unreachable server is not an error (retries
    /// continue in the background); an `Err` essentially means a configuration error such
    /// as a bad URL.
    pub async fn start(&self) -> Result<(), Error> {
        self.link.start().await
    }

    pub async fn must_start(&self) {
        if let Err(e) = self.start().await {
            panic!("{e}");
        }
    }

    /// Best-effort flush of the pending buffer (1 s timeout), then close the connection;
    /// blocks for at most `STOP_GRACE` overall. Idempotent; the instance cannot be reused
    /// afterwards.
    pub async fn stop(&self) {
        self.link.stop(true).await
    }

    /// Publish one message on the bound topic. **Non-blocking**: makes a single enqueue
    /// attempt and returns (same semantics as Go's "return once written to the buffer, error
    /// when the buffer is full"), so callers are never stalled by NATS backpressure. `Ok`
    /// means the connection accepted the message (sent immediately when connected; while
    /// disconnected it stays in the outbound queue and is sent on reconnect). `Err` means
    /// the message was not accepted:
    ///
    /// - `Error::Stopped` / `NotRunning`: wrong instance state (precheck, not counted as dropped);
    /// - `Error::Oversized`: larger than `max_payload`;
    /// - `Error::BufferFull`: outbound queue is full (stalled peer / long disconnect backlog);
    /// - `Error::NoConn`: the connection is inside a self-healing rebuild window, or the
    ///   underlying outbound channel is closed;
    /// - other underlying errors (e.g. the server's max_payload; text is redacted).
    ///
    /// Safe to call concurrently from multiple tasks. `data` is `Bytes`: enqueueing holds a
    /// reference, nothing is copied.
    ///
    /// `Ok` means queued, not written: the woken connection task sits in the calling
    /// worker's (non-stealable) LIFO slot until the caller yields. Yield right after
    /// publishing before doing synchronous work; see README "Publishing".
    pub fn publish(&self, data: Bytes) -> Result<(), Error> {
        self.try_publish(data, true)
    }

    // A mirror's capacity probe is not a failed send. Count a full queue only
    // when the caller actually rejects the message, not before its sender waits.
    fn try_publish(&self, data: Bytes, count_full: bool) -> Result<(), Error> {
        if self.link.is_stopped() {
            return Err(Error::Stopped);
        }
        if !self.link.is_running() {
            return Err(Error::NotRunning);
        }
        if self.max_payload > 0 && data.len() > self.max_payload {
            self.link.dropped.fetch_add(1, Ordering::Relaxed);
            return Err(Error::Oversized {
                size: data.len(),
                max: self.max_payload,
            });
        }
        let Some(client) = self.link.client_snapshot() else {
            self.link.dropped.fetch_add(1, Ordering::Relaxed);
            return Err(Error::NoConn);
        };
        // async-nats' publish only sends into a bounded command channel; poll it once here:
        // Pending = queue full. Wrapped in `unconstrained` so an exhausted tokio cooperative
        // budget cannot make a free slot look like a full queue.
        let bytes = data.len();
        match tokio::task::unconstrained(client.publish(self.subject.clone(), data)).now_or_never()
        {
            Some(Ok(())) => {
                self.link.record_publish(bytes);
                Ok(())
            }
            Some(Err(e)) => {
                self.link.dropped.fetch_add(1, Ordering::Relaxed);
                if e.kind() == PublishErrorKind::Send {
                    Err(Error::NoConn)
                } else if matches!(
                    e.kind(),
                    PublishErrorKind::MaxPayloadExceeded | PublishErrorKind::InvalidSubject
                ) {
                    // async-nats 0.50 supplies only numeric sizes or static
                    // SubjectError text here, never a URL or credential. Keep
                    // the exact text without re-parsing our URL on overload.
                    Err(Error::Nats(e.to_string()))
                } else {
                    // Retain redaction for any future error kinds.
                    Err(Error::Nats(redact_error(&self.link.o.url, &e.to_string())))
                }
            }
            None => {
                if count_full {
                    self.link.dropped.fetch_add(1, Ordering::Relaxed);
                }
                Err(Error::BufferFull(self.link.client_capacity))
            }
        }
    }

    /// Only an endpoint's owned send task waits here; the public API remains
    /// synchronous. Cancelling this future before enqueue cannot publish a copy.
    async fn publish_wait(&self, data: Bytes, cancel: &CancellationToken) -> Result<(), Error> {
        let Some(client) = self.link.client_snapshot() else {
            self.link.dropped.fetch_add(1, Ordering::Relaxed);
            return Err(Error::NoConn);
        };
        let bytes = data.len();
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(Error::Stopped),
            result = tokio::time::timeout(STOP_GRACE, client.publish(self.subject.clone(), data)) => {
                match result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) if error.kind() == PublishErrorKind::Send => Err(Error::NoConn),
                    Ok(Err(error)) => Err(Error::Nats(redact_error(&self.link.o.url, &error.to_string()))),
                    Err(_) => Err(Error::BufferFull(self.link.client_capacity)),
                }
            }
        };
        if result.is_ok() {
            self.link.record_publish(bytes);
        } else {
            self.link.dropped.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    pub fn is_connected(&self) -> bool {
        self.link.is_connected()
    }

    pub async fn wait_connected(&self, timeout: Duration) -> Result<(), Error> {
        self.link.wait_connected(timeout).await
    }

    pub fn stats(&self) -> Stats {
        self.link.stats()
    }

    pub fn topic(&self) -> &str {
        self.link.topic()
    }

    pub fn client(&self) -> Option<async_nats::Client> {
        self.link.client()
    }
}

// These bound only the mirror hand-off, independently of async-nats' own
// command queue. Like Go v1.8.3, an in-flight send has already released its
// reservation; a stalled endpoint may additionally retain that one payload.
const MIRROR_QUEUE_ENTRIES: usize = 4096;
const MIRROR_QUEUE_BYTES: usize = 16 * 1024 * 1024;
const MAX_CONCURRENT_DIALS: usize = 8;

/// One shared immutable payload; every endpoint settles exactly once.
struct MirrorMessage {
    data: Bytes,
    remaining: AtomicUsize,
    failed: AtomicUsize,
    endpoints: usize,
    dropped: Arc<AtomicU64>,
}

impl MirrorMessage {
    fn settle(&self, failed: bool) {
        if failed {
            self.failed.fetch_add(1, Ordering::Relaxed);
        }
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1
            && self.failed.load(Ordering::Acquire) == self.endpoints
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Owns one endpoint's outcome, including task cancellation/panic. A queued
/// copy is not counted as queue-dropped here: only admission/drain knows that.
struct MirrorCopy(Option<Arc<MirrorMessage>>);

impl MirrorCopy {
    fn data(&self) -> &Bytes {
        &self.0.as_ref().unwrap().data
    }

    fn finish(mut self, failed: bool) {
        self.0.take().unwrap().settle(failed);
    }
}

impl Drop for MirrorCopy {
    fn drop(&mut self) {
        if let Some(message) = self.0.take() {
            message.settle(true);
        }
    }
}

struct SendQueue {
    tx: mpsc::Sender<MirrorCopy>,
    // Only held for a nonblocking poll/try_recv, never across a send or await.
    // The stop drain can therefore discard a stalled sender's queued copies.
    rx: Mutex<mpsc::Receiver<MirrorCopy>>,
    entries: AtomicUsize,
    bytes: AtomicUsize,
    capacity: usize,
    max_bytes: usize,
    dropped: AtomicU64,
}

impl SendQueue {
    fn new(capacity: usize, max_bytes: usize) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(capacity);
        Arc::new(Self {
            tx,
            rx: Mutex::new(rx),
            entries: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            capacity,
            max_bytes,
            dropped: AtomicU64::new(0),
        })
    }

    // `fetch_update` is deprecated in favour of `try_update` on newer toolchains; the
    // replacement is not available on the minimum supported Rust version yet.
    #[allow(deprecated)]
    fn reserve(&self, bytes: usize) -> bool {
        if self.tx.is_closed() {
            return false;
        }
        if self
            .entries
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |n| {
                n.checked_add(1).filter(|next| *next <= self.capacity)
            })
            .is_err()
        {
            return false;
        }
        if self
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |n| {
                n.checked_add(bytes).filter(|next| *next <= self.max_bytes)
            })
            .is_err()
        {
            self.entries.fetch_sub(1, Ordering::Release);
            return false;
        }
        true
    }

    fn release(&self, bytes: usize) {
        self.bytes.fetch_sub(bytes, Ordering::Release);
        self.entries.fetch_sub(1, Ordering::Release);
    }

    /// On rejection the returned copy still needs its endpoint outcome settled.
    fn enqueue(&self, copy: MirrorCopy) -> Result<(), MirrorCopy> {
        self.tx.try_send(copy).map_err(|err| {
            let copy = err.into_inner();
            self.release(copy.data().len());
            copy
        })
    }

    async fn recv(&self) -> MirrorCopy {
        std::future::poll_fn(|cx| self.rx.lock().unwrap().poll_recv(cx))
            .await
            .expect("send queue retains its sender")
    }

    fn try_recv(&self) -> Option<MirrorCopy> {
        self.rx.lock().unwrap().try_recv().ok()
    }

    fn len(&self) -> usize {
        self.rx.lock().unwrap().len()
    }

    fn close_and_drain(&self) {
        // Close first: a publisher that reserved just before closure must get
        // its copy back from try_send, release the reservation, and settle it.
        self.rx.lock().unwrap().close();
        self.drain();
    }

    fn drain(&self) {
        while let Some(copy) = self.try_recv() {
            self.release(copy.data().len());
            self.dropped.fetch_add(1, Ordering::Relaxed);
            copy.finish(true);
        }
    }
}

#[cfg(test)]
type TestSend =
    Arc<dyn Fn(Bytes) -> futures::future::BoxFuture<'static, Result<(), Error>> + Send + Sync>;

/// One endpoint's independent routing group and optional asynchronous send line.
struct PubGroup {
    pubs: Vec<Publisher>,
    cursor: AtomicU64,
    queue: Option<Arc<SendQueue>>,
    stop: CancellationToken,
    cancel_send: CancellationToken,
    finished: CancellationToken,
    worker_started: AtomicBool,
    #[cfg(test)]
    test_send: Mutex<Option<TestSend>>,
}

impl PubGroup {
    fn new(n: usize, o: &PublisherOptions) -> Result<Self, Error> {
        if n == 0 {
            return Err(Error::Config(format!(
                "pool size must be positive, got {n}"
            )));
        }
        let base = o.options.clone().normalize("pub")?;
        let mut pubs = Vec::with_capacity(n);
        for i in 0..n {
            let mut po = o.clone();
            po.options = base.clone();
            po.options.name = format!("{}-{i}", base.name);
            pubs.push(Publisher::new(po)?);
        }
        Ok(Self::from_publishers(pubs))
    }

    fn from_publishers(pubs: Vec<Publisher>) -> Self {
        Self {
            pubs,
            cursor: AtomicU64::new(0),
            queue: None,
            stop: CancellationToken::new(),
            cancel_send: CancellationToken::new(),
            finished: CancellationToken::new(),
            worker_started: AtomicBool::new(false),
            #[cfg(test)]
            test_send: Mutex::new(None),
        }
    }

    fn enable_async(&mut self, capacity: usize, max_bytes: usize) {
        let queue = SendQueue::new(capacity, max_bytes);
        for publisher in &self.pubs {
            let queue = queue.clone();
            publisher.link.set_stats_hook(Arc::new(move |stats| {
                stats.queue_len = queue.len();
                stats.queue_cap = queue.capacity;
                stats.queue_dropped = queue.dropped.load(Ordering::Relaxed);
            }));
        }
        self.queue = Some(queue);
    }

    /// Healthy first, then the disconnected members observed in the first pass.
    /// Probe each member once; an asynchronous sender may subsequently wait on
    /// the first full candidate, whose probe did not enqueue a message.
    fn publish(&self, data: &Bytes) -> Result<(), Error> {
        self.try_publish(data, true).0
    }

    fn try_publish(
        &self,
        data: &Bytes,
        count_full: bool,
    ) -> (Result<(), Error>, Option<&Publisher>) {
        let n = self.pubs.len() as u64;
        let start = self.cursor.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        let mut last_err = Error::NoConn;
        let mut fallback = [None; 8];
        let mut extra = Vec::new();
        let mut disconnected = 0;
        let mut waiting = None;
        for i in 0..n {
            let p = &self.pubs[(start.wrapping_add(i) % n) as usize];
            if !p.is_connected() {
                if disconnected < fallback.len() {
                    fallback[disconnected] = Some(p);
                } else {
                    extra.push(p);
                }
                disconnected += 1;
                continue;
            }
            match p.try_publish(data.clone(), count_full) {
                Ok(()) => return (Ok(()), None),
                Err(err @ Error::BufferFull(_)) => {
                    waiting.get_or_insert(p);
                    last_err = err;
                }
                Err(err) => last_err = err,
            }
        }
        for p in fallback.into_iter().flatten().chain(extra) {
            match p.try_publish(data.clone(), count_full) {
                Ok(()) => return (Ok(()), None),
                Err(err @ Error::BufferFull(_)) => {
                    waiting.get_or_insert(p);
                    last_err = err;
                }
                Err(err) => last_err = err,
            }
        }
        (Err(last_err), waiting)
    }

    async fn send(&self, copy: MirrorCopy) {
        self.queue.as_ref().unwrap().release(copy.data().len());
        #[cfg(test)]
        let test_send = self.test_send.lock().unwrap().clone();
        #[cfg(test)]
        if let Some(send) = test_send {
            let result = send(copy.data().clone()).await;
            copy.finish(result.is_err());
            return;
        }
        let (result, waiting) = self.try_publish(copy.data(), false);
        let failed = if let Some(publisher) = waiting {
            publisher
                .publish_wait(copy.data().clone(), &self.cancel_send)
                .await
                .is_err()
        } else {
            result.is_err()
        };
        copy.finish(failed);
    }

    async fn run(self: Arc<Self>) {
        let _finished = self.finished.clone().drop_guard();
        if std::panic::AssertUnwindSafe(self.run_loop())
            .catch_unwind()
            .await
            .is_err()
        {
            tracing::error!(client = %self.pubs[0].link.o.name,
                "[natslink] endpoint sender panicked, rejecting queued copies");
        }
        self.queue.as_ref().unwrap().close_and_drain();
    }

    async fn run_loop(&self) {
        let queue = self.queue.as_ref().unwrap();
        loop {
            tokio::select! {
                biased;
                _ = self.stop.cancelled() => {
                    while let Some(copy) = queue.try_recv() {
                        self.send(copy).await;
                    }
                    return;
                }
                copy = queue.recv() => self.send(copy).await,
            }
        }
    }
}

struct PoolInner {
    pubs: Vec<Publisher>,
    groups: Vec<Arc<PubGroup>>,
    max_payload: usize,
    dropped: Arc<AtomicU64>,
    asynchronous: bool,
    running: AtomicBool,
    stopped: AtomicBool,
    started: AtomicBool,
    // Only short, synchronous sections: Publish admission, sender registration,
    // and the stopped transition. No network/await under this lock.
    lifecycle: RwLock<()>,
    stop_started: AtomicBool,
    stop_done: CancellationToken,
}

/// A plain pool routes synchronously to one endpoint. A mirror with at least
/// two endpoints admits the immutable payload to independent bounded send
/// lines; `Ok` means at least one queue accepted, not server acknowledgement.
/// Use `dropped()` for eventual all-endpoint failures, not summed member drops.
pub struct PublisherPool {
    inner: Arc<PoolInner>,
}

impl PublisherPool {
    pub fn new(n: usize, o: PublisherOptions) -> Result<Self, Error> {
        let group = PubGroup::new(n, &o)?;
        Ok(Self::from_groups(vec![group], o.max_payload, false))
    }

    /// M endpoints, each with n connections (at most 64 endpoints). A single
    /// URL degrades exactly to the ordinary synchronous pool and plain names.
    pub fn new_mirrored(urls: Vec<String>, n: usize, o: PublisherOptions) -> Result<Self, Error> {
        Self::mirrored(urls, n, o, false)
    }

    fn mirrored(
        urls: Vec<String>,
        n: usize,
        mut o: PublisherOptions,
        single_member: bool,
    ) -> Result<Self, Error> {
        validate_mirror_urls(&urls)?;
        let maximum = if single_member { 5 } else { 64 };
        if urls.len() > maximum {
            return Err(Error::Config(format!(
                "mirrored publisher{} allows at most {maximum} endpoints, got {}",
                if single_member { "" } else { " pool" },
                urls.len()
            )));
        }
        if urls.len() == 1 {
            o.options.url = urls.into_iter().next().unwrap();
            return Self::new(n, o);
        }
        let mut groups = Vec::with_capacity(urls.len());
        for (endpoint, url) in urls.into_iter().enumerate() {
            let mut options = o.clone();
            options.options.url = url;
            options.options.name =
                mirror_endpoint_name(&o.options.name, "pub", &o.options.topic, endpoint);
            let group = if single_member {
                PubGroup::from_publishers(vec![Publisher::new(options)?])
            } else {
                PubGroup::new(n, &options)?
            };
            groups.push(group);
        }
        Ok(Self::from_groups(groups, o.max_payload, true))
    }

    fn from_groups(groups: Vec<PubGroup>, max_payload: usize, asynchronous: bool) -> Self {
        Self::with_queue_limits(
            groups,
            max_payload,
            asynchronous,
            MIRROR_QUEUE_ENTRIES,
            MIRROR_QUEUE_BYTES,
        )
    }

    fn with_queue_limits(
        mut groups: Vec<PubGroup>,
        max_payload: usize,
        asynchronous: bool,
        entries: usize,
        bytes: usize,
    ) -> Self {
        if asynchronous {
            for group in &mut groups {
                group.enable_async(entries, bytes);
            }
        }
        let pubs = groups
            .iter()
            .flat_map(|group| group.pubs.iter().cloned())
            .collect();
        Self {
            inner: Arc::new(PoolInner {
                pubs,
                groups: groups.into_iter().map(Arc::new).collect(),
                max_payload,
                dropped: Arc::new(AtomicU64::new(0)),
                asynchronous,
                running: AtomicBool::new(false),
                stopped: AtomicBool::new(false),
                started: AtomicBool::new(false),
                lifecycle: RwLock::new(()),
                stop_started: AtomicBool::new(false),
                stop_done: CancellationToken::new(),
            }),
        }
    }

    /// At most eight dials in flight, interleaved across endpoints. Failure
    /// waits for late finishers and rolls back every successful member.
    pub async fn start(&self) -> Result<(), Error> {
        if self.inner.stopped.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        if self.inner.started.swap(true, Ordering::AcqRel) {
            return Err(Error::AlreadyStarted);
        }
        // Starting is single-shot, so cancellation of this waiter must not
        // abandon late dials, successful members or sender registration. The
        // owned initializer completes the same success/rollback sequence.
        let pool = Self {
            inner: self.inner.clone(),
        };
        tokio::spawn(async move { pool.start_owned().await })
            .await
            .map_err(|_| Error::Nats("publisher pool initialization task failed".into()))?
    }

    async fn start_owned(&self) -> Result<(), Error> {
        let order = self.dial_order();
        let mut starts = futures::stream::iter(order.into_iter().map(|publisher| async move {
            let result = publisher.start().await;
            (publisher, result)
        }))
        .buffer_unordered(MAX_CONCURRENT_DIALS);
        let mut started = Vec::with_capacity(self.inner.pubs.len());
        let mut first_error = None;
        while let Some((publisher, result)) = starts.next().await {
            match result {
                Ok(()) => started.push(publisher),
                Err(err) if first_error.is_none() => first_error = Some(err),
                Err(_) => {}
            }
        }
        if let Some(error) = first_error {
            futures::future::join_all(started.iter().map(|publisher| publisher.stop())).await;
            return Err(error);
        }
        self.register_senders()
    }

    fn dial_order(&self) -> Vec<Publisher> {
        let mut order = Vec::with_capacity(self.inner.pubs.len());
        let max_members = self
            .inner
            .groups
            .iter()
            .map(|group| group.pubs.len())
            .max()
            .unwrap_or(0);
        for member in 0..max_members {
            for group in &self.inner.groups {
                if let Some(publisher) = group.pubs.get(member) {
                    order.push(publisher.clone());
                }
            }
        }
        order
    }

    fn register_senders(&self) -> Result<(), Error> {
        let _guard = self.inner.lifecycle.write().unwrap();
        if self.inner.stopped.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        if self.inner.asynchronous {
            for group in &self.inner.groups {
                group.worker_started.store(true, Ordering::Release);
                tokio::spawn(group.clone().run());
            }
        }
        self.inner.running.store(true, Ordering::Release);
        Ok(())
    }

    pub async fn must_start(&self) {
        if let Err(error) = self.start().await {
            panic!("{error}");
        }
    }

    pub async fn wait_connected(&self, timeout: Duration) -> Result<(), Error> {
        let deadline = tokio::time::Instant::now() + timeout;
        for publisher in &self.inner.pubs {
            publisher
                .wait_connected(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await?;
        }
        Ok(())
    }

    /// One ordered sequence shared by concurrent callers, even if the first
    /// caller is cancelled: stop admission, drain send lines for at most 5s,
    /// then stop all members and await cancelled senders concurrently (at most
    /// another 5s). A send that outlives both budgets may settle later; queued
    /// leftovers are dropped immediately and counted separately from member
    /// send failures.
    pub async fn stop(&self) {
        {
            let _guard = self.inner.lifecycle.write().unwrap();
            self.inner.stopped.store(true, Ordering::Release);
            self.inner.running.store(false, Ordering::Release);
            if !self.inner.stop_started.swap(true, Ordering::AcqRel) {
                let inner = self.inner.clone();
                tokio::spawn(async move {
                    let _done = inner.stop_done.clone().drop_guard();
                    if inner.asynchronous {
                        for group in &inner.groups {
                            group.stop.cancel();
                        }
                        let deadline = tokio::time::Instant::now() + STOP_GRACE;
                        futures::future::join_all(inner.groups.iter().map(|group| async move {
                            if group.worker_started.load(Ordering::Acquire)
                                && tokio::time::timeout_at(deadline, group.finished.cancelled()).await.is_err()
                            {
                                let queue = group.queue.as_ref().unwrap();
                                tracing::warn!(client = %group.pubs[0].link.o.name, queued = queue.len(),
                                    "[natslink] endpoint send stop grace exceeded, discarding queued copies");
                                group.cancel_send.cancel();
                                queue.drain();
                            }
                        })).await;
                    }
                    // Closing members and settling cancelled sends share one
                    // deadline. A member closing first does not prove its
                    // sender has observed cancellation and counted its copy.
                    let deadline = tokio::time::Instant::now() + STOP_GRACE;
                    let members = futures::future::join_all(
                        inner.pubs.iter().map(|publisher| publisher.stop()),
                    );
                    let senders = futures::future::join_all(
                        inner
                            .groups
                            .iter()
                            .filter(|group| group.worker_started.load(Ordering::Acquire))
                            .map(|group| group.finished.cancelled()),
                    );
                    // Link shutdown is owned independently of this waiter, so
                    // reaching the shared deadline does not cancel cleanup.
                    let _ =
                        tokio::time::timeout_at(deadline, futures::future::join(members, senders))
                            .await;
                });
            }
        }
        self.inner.stop_done.cancelled().await;
    }

    /// Nonblocking. Single endpoint: synchronous routing; multiple endpoints:
    /// independent queue admission with asynchronous eventual loss accounting.
    ///
    /// Either way `Ok` means queued, not written; the send happens after the caller
    /// yields (see [`Publisher::publish`] and README "Publishing").
    pub fn publish(&self, data: Bytes) -> Result<(), Error> {
        if self.inner.max_payload > 0 && data.len() > self.inner.max_payload {
            self.inner.dropped.fetch_add(1, Ordering::Relaxed);
            return Err(Error::Oversized {
                size: data.len(),
                max: self.inner.max_payload,
            });
        }
        if self.inner.asynchronous {
            return self.publish_async(data);
        }
        match self.inner.groups[0].publish(&data) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.inner.dropped.fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
        }
    }

    fn publish_async(&self, data: Bytes) -> Result<(), Error> {
        let _guard = self.inner.lifecycle.read().unwrap();
        let error = if self.inner.stopped.load(Ordering::Acquire) {
            Some(Error::Stopped)
        } else if !self.inner.running.load(Ordering::Acquire) {
            Some(Error::NotRunning)
        } else {
            None
        };
        if let Some(error) = error {
            self.inner.dropped.fetch_add(1, Ordering::Relaxed);
            return Err(error);
        }
        // Reserve both limits before allocating shared bookkeeping. No payload
        // copy even on success: Bytes is immutable and safe for all send lines.
        let mut reserved = 0_u64;
        for (index, group) in self.inner.groups.iter().enumerate() {
            if group.queue.as_ref().unwrap().reserve(data.len()) {
                reserved |= 1_u64 << index;
            }
        }
        if reserved == 0 {
            for group in &self.inner.groups {
                group
                    .queue
                    .as_ref()
                    .unwrap()
                    .dropped
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.inner.dropped.fetch_add(1, Ordering::Relaxed);
            return Err(Error::QueueFull);
        }
        let message = Arc::new(MirrorMessage {
            data,
            remaining: AtomicUsize::new(self.inner.groups.len()),
            failed: AtomicUsize::new(0),
            endpoints: self.inner.groups.len(),
            dropped: self.inner.dropped.clone(),
        });
        let mut accepted = 0;
        for (index, group) in self.inner.groups.iter().enumerate() {
            let queue = group.queue.as_ref().unwrap();
            if reserved & (1_u64 << index) != 0 {
                match queue.enqueue(MirrorCopy(Some(message.clone()))) {
                    Ok(()) => {
                        accepted += 1;
                        continue;
                    }
                    Err(copy) => copy.finish(true),
                }
            } else {
                message.settle(true);
            }
            queue.dropped.fetch_add(1, Ordering::Relaxed);
        }
        if accepted == 0 {
            Err(Error::QueueFull)
        } else {
            Ok(())
        }
    }

    pub fn connected_count(&self) -> usize {
        self.inner
            .pubs
            .iter()
            .filter(|publisher| publisher.is_connected())
            .count()
    }

    pub fn size(&self) -> usize {
        self.inner.pubs.len()
    }
    pub fn endpoints(&self) -> usize {
        self.inner.groups.len()
    }

    /// A logical message lost on every endpoint, settled asynchronously for
    /// mirrors. Member `dropped` counts failed attempts; `queue_dropped` counts
    /// endpoint copies rejected on admission or discarded during stop.
    pub fn dropped(&self) -> u64 {
        self.inner.dropped.load(Ordering::Relaxed)
    }
    pub fn stats(&self) -> Vec<Stats> {
        self.inner.pubs.iter().map(Publisher::stats).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ReportInterval;

    struct Sender {
        gate: CancellationToken,
        entered: AtomicUsize,
        fail: AtomicBool,
        sent: Mutex<Vec<Bytes>>,
    }

    impl Sender {
        fn new(blocked: bool) -> Arc<Self> {
            let gate = CancellationToken::new();
            if !blocked {
                gate.cancel();
            }
            Arc::new(Self {
                gate,
                entered: AtomicUsize::new(0),
                fail: AtomicBool::new(false),
                sent: Mutex::new(Vec::new()),
            })
        }

        fn hook(self: &Arc<Self>) -> TestSend {
            let sender = self.clone();
            Arc::new(move |data| {
                let sender = sender.clone();
                Box::pin(async move {
                    sender.entered.fetch_add(1, Ordering::Release);
                    sender.gate.cancelled().await;
                    sender.sent.lock().unwrap().push(data);
                    if sender.fail.load(Ordering::Acquire) {
                        Err(Error::NoConn)
                    } else {
                        Ok(())
                    }
                })
            })
        }

        fn count(&self) -> usize {
            self.sent.lock().unwrap().len()
        }
    }

    fn options() -> PublisherOptions {
        PublisherOptions {
            options: Options {
                url: "nats://127.0.0.1:1".into(),
                topic: "mirror.test".into(),
                name: "test".into(),
                report_interval: ReportInterval::Off,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn fixture(
        entries: usize,
        bytes: usize,
        blocked: [bool; 2],
    ) -> (Arc<PublisherPool>, [Arc<Sender>; 2]) {
        let groups = (0..2)
            .map(|index| {
                let mut options = options();
                options.options.name = format!("test-e{index}");
                PubGroup::new(1, &options).unwrap()
            })
            .collect();
        let pool = Arc::new(PublisherPool::with_queue_limits(
            groups, 0, true, entries, bytes,
        ));
        let senders = [Sender::new(blocked[0]), Sender::new(blocked[1])];
        for (group, sender) in pool.inner.groups.iter().zip(&senders) {
            *group.test_send.lock().unwrap() = Some(sender.hook());
        }
        (pool, senders)
    }

    async fn until(mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("condition did not become true");
    }

    #[tokio::test]
    async fn stalled_endpoint_is_independent_and_recovers_in_order_without_copying_bytes() {
        let (pool, senders) = fixture(16, 1024, [true, false]);
        pool.register_senders().unwrap();
        let payload = Bytes::from(vec![1_u8; 10]);
        let pointer = payload.as_ptr();
        pool.publish(payload).unwrap();
        for byte in 2..=10 {
            pool.publish(Bytes::from(vec![byte; 10])).unwrap();
        }
        until(|| senders[1].count() == 10 && pool.stats()[0].queue_len == 9).await;
        assert_eq!(senders[0].count(), 0);
        assert_eq!(pool.dropped(), 0);
        assert_eq!(senders[1].sent.lock().unwrap()[0].as_ptr(), pointer);
        senders[0].gate.cancel();
        until(|| senders[0].count() == 10).await;
        assert_eq!(
            senders[0]
                .sent
                .lock()
                .unwrap()
                .iter()
                .map(|data| data[0])
                .collect::<Vec<_>>(),
            (1..=10).collect::<Vec<_>>()
        );
        pool.stop().await;
        assert!(pool
            .stats()
            .iter()
            .all(|stats| stats.queue_len == 0 && stats.queue_dropped == 0));
    }

    #[tokio::test]
    async fn queue_entry_byte_limits_roll_back_and_account_endpoint_vs_message_loss() {
        let (pool, senders) = fixture(2, 10, [true, true]);
        pool.register_senders().unwrap();
        pool.publish(Bytes::from_static(b"in-flight")).unwrap();
        until(|| {
            senders
                .iter()
                .all(|sender| sender.entered.load(Ordering::Acquire) == 1)
        })
        .await;
        pool.publish(Bytes::from_static(b"12345678")).unwrap();
        assert!(matches!(
            pool.publish(Bytes::from_static(b"123")),
            Err(Error::QueueFull)
        ));
        for group in &pool.inner.groups {
            let queue = group.queue.as_ref().unwrap();
            assert_eq!(queue.entries.load(Ordering::Acquire), 1);
            assert_eq!(queue.bytes.load(Ordering::Acquire), 8);
        }
        // Fill the remaining entry without using the remaining byte budget.
        pool.publish(Bytes::new()).unwrap();
        assert!(matches!(pool.publish(Bytes::new()), Err(Error::QueueFull)));
        assert_eq!(pool.dropped(), 2);
        for stats in pool.stats() {
            assert_eq!(stats.queue_len, 2);
            assert_eq!(stats.queue_dropped, 2);
            assert_eq!(stats.dropped, 0);
        }
        for sender in &senders {
            sender.gate.cancel();
        }
        pool.stop().await;
        for group in &pool.inner.groups {
            let queue = group.queue.as_ref().unwrap();
            assert_eq!(queue.entries.load(Ordering::Acquire), 0);
            assert_eq!(queue.bytes.load(Ordering::Acquire), 0);
        }
        assert_eq!(pool.dropped(), 2);
    }

    #[tokio::test]
    async fn one_full_endpoint_does_not_drop_the_logical_message() {
        let (pool, senders) = fixture(1, 1024, [true, false]);
        pool.register_senders().unwrap();
        pool.publish(Bytes::from_static(b"a")).unwrap();
        until(|| senders[0].entered.load(Ordering::Acquire) == 1 && senders[1].count() == 1).await;
        pool.publish(Bytes::from_static(b"b")).unwrap();
        until(|| senders[1].count() == 2).await;
        pool.publish(Bytes::from_static(b"c")).unwrap();
        until(|| senders[1].count() == 3).await;
        assert_eq!(pool.stats()[0].queue_dropped, 1);
        assert_eq!(pool.stats()[1].queue_dropped, 0);
        assert_eq!(pool.dropped(), 0);
        senders[0].gate.cancel();
        pool.stop().await;
        assert_eq!(pool.dropped(), 0);
    }

    #[tokio::test]
    async fn asynchronous_all_endpoint_failure_is_counted_once() {
        let (pool, senders) = fixture(4, 1024, [false, false]);
        senders[0].fail.store(true, Ordering::Release);
        pool.register_senders().unwrap();
        pool.publish(Bytes::from_static(b"one path succeeds"))
            .unwrap();
        until(|| senders.iter().all(|sender| sender.count() == 1)).await;
        assert_eq!(pool.dropped(), 0);
        senders[1].fail.store(true, Ordering::Release);
        pool.publish(Bytes::from_static(b"all paths fail")).unwrap();
        until(|| pool.dropped() == 1).await;
        assert!(pool.stats().iter().all(|stats| stats.queue_dropped == 0));
        pool.stop().await;
        assert_eq!(pool.dropped(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn stop_bounds_both_phases_when_an_inflight_send_ignores_cancellation() {
        let (pool, senders) = fixture(8, 1024, [true, true]);
        for sender in &senders {
            sender.fail.store(true, Ordering::Release);
        }
        pool.register_senders().unwrap();
        for _ in 0..5 {
            pool.publish(Bytes::from_static(b"message")).unwrap();
        }
        until(|| {
            senders
                .iter()
                .all(|sender| sender.entered.load(Ordering::Acquire) == 1)
        })
        .await;
        let start = tokio::time::Instant::now();
        futures::future::join(pool.stop(), pool.stop()).await;
        assert_eq!(start.elapsed(), 2 * STOP_GRACE);
        assert_eq!(
            pool.dropped(),
            4,
            "four queued logical messages failed on both endpoints"
        );
        for stats in pool.stats() {
            assert_eq!(stats.queue_len, 0);
            assert_eq!(stats.queue_dropped, 4);
        }
        for sender in &senders {
            sender.gate.cancel();
        }
        until(|| pool.dropped() == 5).await;
        assert!(matches!(
            pool.publish(Bytes::from_static(b"late")),
            Err(Error::Stopped)
        ));
        assert_eq!(pool.dropped(), 6);
    }

    #[tokio::test(start_paused = true)]
    async fn stop_waits_for_cancelled_sender_settlement_after_members_close() {
        let (pool, _) = fixture(8, 1024, [true, true]);
        let entered = Arc::new(AtomicUsize::new(0));
        for group in &pool.inner.groups {
            let entered = entered.clone();
            let cancel = group.cancel_send.clone();
            *group.test_send.lock().unwrap() = Some(Arc::new(move |_| {
                let entered = entered.clone();
                let cancel = cancel.clone();
                Box::pin(async move {
                    entered.fetch_add(1, Ordering::Release);
                    cancel.cancelled().await;
                    // Model a sender that needs another turn after member
                    // shutdown to finish cancellation and settle its copy.
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    Err(Error::Stopped)
                })
            }));
        }
        pool.register_senders().unwrap();
        for _ in 0..3 {
            pool.publish(Bytes::from_static(b"message")).unwrap();
        }
        until(|| entered.load(Ordering::Acquire) == 2).await;
        let start = tokio::time::Instant::now();
        pool.stop().await;
        assert_eq!(start.elapsed(), STOP_GRACE + Duration::from_secs(1));
        assert_eq!(pool.dropped(), 3);
        for group in &pool.inner.groups {
            assert!(group.finished.is_cancelled());
        }
        for stats in pool.stats() {
            assert_eq!(stats.queue_len, 0);
            assert_eq!(stats.queue_dropped, 2);
        }
    }

    #[tokio::test]
    async fn concurrent_stop_waits_for_drain_and_survives_first_waiter_cancellation() {
        let (pool, senders) = fixture(8, 1024, [true, true]);
        pool.register_senders().unwrap();
        for _ in 0..5 {
            pool.publish(Bytes::from_static(b"message")).unwrap();
        }
        until(|| {
            senders
                .iter()
                .all(|sender| sender.entered.load(Ordering::Acquire) == 1)
        })
        .await;
        let first = tokio::spawn({
            let pool = pool.clone();
            async move { pool.stop().await }
        });
        until(|| pool.inner.stopped.load(Ordering::Acquire)).await;
        first.abort();
        assert!(
            pool.inner
                .pubs
                .iter()
                .all(|publisher| !publisher.link.is_stopped()),
            "members must stay open until send lines drain"
        );
        let second = tokio::spawn({
            let pool = pool.clone();
            async move { pool.stop().await }
        });
        tokio::task::yield_now().await;
        assert!(!second.is_finished());
        for sender in &senders {
            sender.gate.cancel();
        }
        second.await.unwrap();
        assert!(senders.iter().all(|sender| sender.count() == 5));
        assert_eq!(pool.dropped(), 0);
        assert!(pool.stats().iter().all(|stats| stats.queue_dropped == 0));
        assert!(pool
            .inner
            .pubs
            .iter()
            .all(|publisher| publisher.link.is_stopped()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publish_racing_stop_leaves_no_accepted_copy_behind() {
        let (pool, senders) = fixture(4096, 16 * 1024 * 1024, [true, true]);
        pool.register_senders().unwrap();
        pool.publish(Bytes::from_static(b"first")).unwrap();
        let accepted = Arc::new(AtomicUsize::new(1));
        let writers: Vec<_> = (0..8)
            .map(|_| {
                let pool = pool.clone();
                let accepted = accepted.clone();
                tokio::spawn(async move {
                    for index in 0..100 {
                        match pool.publish(Bytes::from_static(b"race")) {
                            Ok(()) => {
                                accepted.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(Error::Stopped) => {}
                            other => panic!("unexpected admission: {other:?}"),
                        }
                        if index % 10 == 0 {
                            tokio::task::yield_now().await;
                        }
                    }
                })
            })
            .collect();
        let stopping = tokio::spawn({
            let pool = pool.clone();
            async move { pool.stop().await }
        });
        for writer in writers {
            writer.await.unwrap();
        }
        for sender in &senders {
            sender.gate.cancel();
        }
        stopping.await.unwrap();
        assert_eq!(
            senders.iter().map(|sender| sender.count()).sum::<usize>(),
            2 * accepted.load(Ordering::Acquire)
        );
        assert!(pool
            .stats()
            .iter()
            .all(|stats| stats.queue_len == 0 && stats.queue_dropped == 0));
        for group in &pool.inner.groups {
            let queue = group.queue.as_ref().unwrap();
            assert_eq!(queue.entries.load(Ordering::Acquire), 0);
            assert_eq!(queue.bytes.load(Ordering::Acquire), 0);
        }
    }

    #[test]
    fn reservations_are_bounded_overflow_safe_and_rollback_under_contention() {
        let queue = SendQueue::new(8, 32);
        assert!(!queue.reserve(usize::MAX));
        assert_eq!(queue.entries.load(Ordering::Acquire), 0);
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let queue = queue.clone();
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        if queue.reserve(7) {
                            assert!(queue.entries.load(Ordering::Acquire) <= 8);
                            assert!(queue.bytes.load(Ordering::Acquire) <= 32);
                            queue.release(7);
                        }
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(queue.entries.load(Ordering::Acquire), 0);
        assert_eq!(queue.bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn constructors_preserve_singleton_names_and_enforce_mirror_caps() {
        let urls = |count| {
            (0..count)
                .map(|index| format!("nats://host{index}:4222"))
                .collect::<Vec<_>>()
        };
        let single = Publisher::new_mirrored(urls(1), options()).unwrap();
        assert!(!single.inner.asynchronous);
        assert_eq!(single.stats()[0].name, "test-0");
        let single_pool = PublisherPool::new_mirrored(urls(1), 3, options()).unwrap();
        assert!(!single_pool.inner.asynchronous);
        assert_eq!(single_pool.stats()[2].name, "test-2");
        let mirror = Publisher::new_mirrored(urls(5), options()).unwrap();
        assert_eq!(mirror.stats()[0].name, "test-e0");
        assert_eq!(mirror.size(), 5);
        assert!(Publisher::new_mirrored(urls(6), options()).is_err());
        assert!(PublisherPool::new_mirrored(urls(64), 1, options()).is_ok());
        assert!(PublisherPool::new_mirrored(urls(65), 1, options()).is_err());
        assert!(PublisherPool::new_mirrored(urls(2), 0, options()).is_err());
        let pool = PublisherPool::new_mirrored(urls(2), 10, options()).unwrap();
        assert_eq!(
            pool.dial_order()
                .iter()
                .take(4)
                .map(|publisher| publisher.stats().name)
                .collect::<Vec<_>>(),
            ["test-e0-0", "test-e1-0", "test-e0-1", "test-e1-1"]
        );
        assert!(matches!(
            mirror.publish(Bytes::new()),
            Err(Error::NotRunning)
        ));
        assert_eq!(mirror.dropped(), 1);
    }

    #[tokio::test]
    async fn stop_before_sender_registration_and_double_start_do_not_resurrect_pool() {
        let (pool, _) = fixture(2, 1024, [false, false]);
        pool.stop().await;
        assert!(matches!(pool.register_senders(), Err(Error::Stopped)));
        assert!(matches!(pool.start().await, Err(Error::Stopped)));
        assert!(pool
            .inner
            .groups
            .iter()
            .all(|group| !group.worker_started.load(Ordering::Acquire)));

        let pool = PublisherPool::new_mirrored(
            vec!["nats://127.0.0.1:1".into(), "nats://127.0.0.1:2".into()],
            1,
            options(),
        )
        .unwrap();
        pool.start().await.unwrap();
        assert!(matches!(pool.start().await, Err(Error::AlreadyStarted)));
        assert!(pool
            .inner
            .pubs
            .iter()
            .all(|publisher| publisher.link.is_running()));
        pool.stop().await;
    }

    #[tokio::test]
    async fn cancelled_start_waiter_still_completes_or_rolls_back_every_member() {
        for fail in [false, true] {
            let pool = Arc::new(
                PublisherPool::new_mirrored(
                    vec!["nats://127.0.0.1:1".into(), "nats://127.0.0.1:2".into()],
                    1,
                    options(),
                )
                .unwrap(),
            );
            let gate = CancellationToken::new();
            let entered = Arc::new(AtomicUsize::new(0));
            for (index, publisher) in pool.inner.pubs.iter().enumerate() {
                let (gate, entered) = (gate.clone(), entered.clone());
                publisher.link.set_setup(Arc::new(move |_, _| {
                    let (gate, entered) = (gate.clone(), entered.clone());
                    Box::pin(async move {
                        entered.fetch_add(1, Ordering::Release);
                        gate.cancelled().await;
                        if fail && index == 0 {
                            Err(Error::Nats("synthetic setup failure".into()))
                        } else {
                            Ok(())
                        }
                    })
                }));
            }
            let waiter = tokio::spawn({
                let pool = pool.clone();
                async move { pool.start().await }
            });
            until(|| entered.load(Ordering::Acquire) == 2).await;
            waiter.abort();
            assert!(waiter.await.unwrap_err().is_cancelled());
            gate.cancel();
            if fail {
                until(|| {
                    pool.inner
                        .pubs
                        .iter()
                        .all(|publisher| !publisher.link.is_running())
                })
                .await;
                assert!(!pool.inner.running.load(Ordering::Acquire));
                assert!(pool
                    .inner
                    .groups
                    .iter()
                    .all(|group| { !group.worker_started.load(Ordering::Acquire) }));
            } else {
                until(|| pool.inner.running.load(Ordering::Acquire)).await;
                assert!(pool
                    .inner
                    .groups
                    .iter()
                    .all(|group| { group.worker_started.load(Ordering::Acquire) }));
            }
            assert!(matches!(pool.start().await, Err(Error::AlreadyStarted)));
            pool.stop().await;
        }
    }

    #[tokio::test]
    async fn panicked_sender_closes_admission_and_settles_every_queued_copy() {
        let (pool, senders) = fixture(8, 1024, [false, false]);
        for group in &pool.inner.groups {
            *group.test_send.lock().unwrap() = Some(Arc::new(|_| {
                Box::pin(async {
                    panic!("synthetic send panic");
                })
            }));
        }
        pool.register_senders().unwrap();
        for _ in 0..3 {
            pool.publish(Bytes::from_static(b"message")).unwrap();
        }
        until(|| {
            pool.inner
                .groups
                .iter()
                .all(|group| group.finished.is_cancelled())
        })
        .await;
        assert_eq!(pool.dropped(), 3);
        assert!(pool
            .stats()
            .iter()
            .all(|stats| stats.queue_len == 0 && stats.queue_dropped == 2));
        assert!(matches!(pool.publish(Bytes::new()), Err(Error::QueueFull)));
        assert_eq!(pool.dropped(), 4);
        for group in &pool.inner.groups {
            let queue = group.queue.as_ref().unwrap();
            assert_eq!(queue.entries.load(Ordering::Acquire), 0);
            assert_eq!(queue.bytes.load(Ordering::Acquire), 0);
        }
        pool.stop().await;
        assert!(senders.iter().all(|sender| sender.count() == 0));
    }

    #[test]
    fn receiver_closure_rejects_a_previously_reserved_copy_without_leaking_budget() {
        let queue = SendQueue::new(1, 8);
        let dropped = Arc::new(AtomicU64::new(0));
        assert!(queue.reserve(4));
        queue.close_and_drain();
        let message = Arc::new(MirrorMessage {
            data: Bytes::from_static(b"test"),
            remaining: AtomicUsize::new(1),
            failed: AtomicUsize::new(0),
            endpoints: 1,
            dropped: dropped.clone(),
        });
        queue
            .enqueue(MirrorCopy(Some(message)))
            .expect_err("closed queue must reject")
            .finish(true);
        assert_eq!(queue.entries.load(Ordering::Acquire), 0);
        assert_eq!(queue.bytes.load(Ordering::Acquire), 0);
        assert_eq!(dropped.load(Ordering::Acquire), 1);
        assert!(!queue.reserve(0));
    }

    #[tokio::test]
    async fn start_accepts_comma_list() {
        for url in [
            "nats://127.0.0.1:1,nats://127.0.0.1:2",
            "nats://tok@127.0.0.1:1, nats://tok@127.0.0.1:2",
        ] {
            let mut options = options();
            options.options.url = url.into();
            let publisher = Publisher::new(options).unwrap();
            publisher.start().await.unwrap();
            publisher.stop().await;
        }
    }
}
