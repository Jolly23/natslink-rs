//! Integration tests against a real NATS server. Run them against throw-away local docker
//! servers only:
//!
//!     docker run -d --name natslink-nats-1 -p 127.0.0.1:4222:4222 nats:latest --auth natslink-itest-token
//!     docker run -d --name natslink-nats-2 -p 127.0.0.1:4223:4222 nats:latest --auth natslink-itest-token
//!     NATS_TEST_URL=nats://natslink-itest-token@127.0.0.1:4222 NATS_TEST_URL2=nats://natslink-itest-token@127.0.0.1:4223 \
//!     NATS_TEST_CONTAINER=natslink-nats-1 cargo test --test integration
//!
//! When `NATS_TEST_URL` is unset every test returns immediately (effectively skipped), so a
//! plain `cargo test` passes without a NATS server.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use natslink::{
    handler, Error, Options, Publisher, PublisherOptions, PublisherPool, ReportInterval,
    Subscriber, SubscriberOptions, Workers,
};

/// Tests that `docker stop/restart` the shared test server take the write lock, all others
/// take the read lock, so parallel runs do not disrupt each other.
static DISRUPTION: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

fn url() -> Option<String> {
    std::env::var("NATS_TEST_URL")
        .ok()
        .filter(|s| !s.is_empty())
}
fn url2() -> Option<String> {
    std::env::var("NATS_TEST_URL2")
        .ok()
        .filter(|s| !s.is_empty())
}
fn container() -> Option<String> {
    std::env::var("NATS_TEST_CONTAINER")
        .ok()
        .filter(|s| !s.is_empty())
}

fn opts(url: &str, topic: &str) -> Options {
    Options {
        url: url.into(),
        topic: topic.into(),
        name: String::new(),
        report_interval: ReportInterval::Off,
        tweak: None,
    }
}

fn unique(prefix: &str) -> String {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("natslink.rs.test.{prefix}.{n}")
}

struct Fixture {
    sub: Subscriber,
    publ: Publisher,
    got: Arc<AtomicU64>,
}

async fn fixture(url: &str, topic: &str, workers: Workers) -> Fixture {
    let got = Arc::new(AtomicU64::new(0));
    let g = Arc::clone(&got);
    let mut so = SubscriberOptions::new(
        opts(url, topic),
        handler(move |_m| {
            let g = Arc::clone(&g);
            async move {
                g.fetch_add(1, Ordering::Relaxed);
            }
        }),
    );
    so.workers = workers;
    let sub = Subscriber::new(so).unwrap();
    sub.start().await.unwrap();
    sub.wait_connected(Duration::from_secs(5)).await.unwrap();
    let publ = Publisher::new(PublisherOptions {
        options: opts(url, topic),
        max_payload: 0,
        queue_capacity: 0,
    })
    .unwrap();
    publ.start().await.unwrap();
    publ.wait_connected(Duration::from_secs(5)).await.unwrap();
    Fixture { sub, publ, got }
}

/// Force a real round trip between the subscriber connection and the server, guaranteeing
/// that previously queued SUB / UNSUB commands have been processed.
/// Note that async-nats' `flush()` only guarantees the write buffer reached the socket; it is
/// **not** a PING/PONG round trip (Go's `Flush` is). So this sends a request to a subject
/// nobody subscribes to: the server answers no-responders synchronously, and receiving that
/// means every earlier command has been handled.
async fn round_trip(sub: &Subscriber) {
    if let Some(c) = sub.client() {
        let _ = tokio::time::timeout(
            Duration::from_secs(3),
            c.request(unique("rt"), Bytes::new()),
        )
        .await;
    }
}

async fn publish_n(p: &Publisher, n: usize) {
    for i in 0..n {
        p.publish(Bytes::from(format!("m{i}"))).unwrap();
    }
    p.client().unwrap().flush().await.unwrap();
}

async fn wait_count(got: &AtomicU64, want: u64, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    while got.load(Ordering::Relaxed) < want {
        assert!(
            tokio::time::Instant::now() < deadline,
            "received {}, want {want}",
            got.load(Ordering::Relaxed)
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn probe_reports_environment_errors() {
    let Some(u) = url() else { return };
    let _quiet = DISRUPTION.read().await;
    natslink::probe(&u).await.unwrap();
    // Wrong token: must fail within DIAL_TIMEOUT, and the error text must not contain the token.
    let hostport = u.rsplit('@').next().unwrap().to_string();
    let bad = format!("nats://wrongtoken000@{hostport}");
    let err = natslink::probe(&bad)
        .await
        .expect_err("bad token must fail");
    assert!(!err.to_string().contains("wrongtoken000"), "{err}");
    let err = natslink::probe("nats://x@127.0.0.1:1")
        .await
        .expect_err("closed port must fail");
    assert!(err.to_string().contains("nats://***@127.0.0.1:1"), "{err}");
}

#[tokio::test]
async fn pubsub_roundtrip_all_modes() {
    let Some(u) = url() else { return };
    let _quiet = DISRUPTION.read().await;
    for mode in [Workers::Pool(0), Workers::Sync, Workers::PerMessage] {
        let f = fixture(&u, &unique("modes"), mode).await;
        round_trip(&f.sub).await;
        publish_n(&f.publ, 5).await;
        wait_count(&f.got, 5, Duration::from_secs(3)).await;
        let st = f.sub.stats();
        assert!(st.connected && st.running && st.in_msgs >= 5, "{st:?}");
        assert_eq!(st.dropped, 0);
        let ps = f.publ.stats();
        assert!(ps.out_msgs >= 5, "{ps:?}");
        f.sub.stop().await;
        f.publ.stop().await;
    }
}

/// A comma-separated URL is ordered failover within one endpoint: when the first segment is
/// a dead port, both probe and start must fall through to the second and be able to send
/// and receive. Both segments carry the same token (credentials must match across segments,
/// see the README section on comma-separated URLs). Earlier releases always failed start
/// with `invalid port number`.
#[tokio::test]
async fn comma_list_fails_over_in_order() {
    let Some(u) = url() else { return };
    let _quiet = DISRUPTION.read().await;
    let (userinfo, _) = u
        .trim_start_matches("nats://")
        .rsplit_once('@')
        .expect("NATS_TEST_URL carries a token");
    let list = format!("nats://{userinfo}@127.0.0.1:1,{u}");
    natslink::probe(&list).await.unwrap();
    let f = fixture(&list, &unique("comma"), Workers::Pool(0)).await;
    round_trip(&f.sub).await;
    publish_n(&f.publ, 3).await;
    wait_count(&f.got, 3, Duration::from_secs(3)).await;
    assert!(f.publ.stats().connected && f.sub.stats().connected);
    f.sub.stop().await;
    f.publ.stop().await;
}

#[tokio::test]
async fn gate_blocks_at_entry() {
    let Some(u) = url() else { return };
    let _quiet = DISRUPTION.read().await;
    let topic = unique("gate");
    let got = Arc::new(AtomicU64::new(0));
    let g = Arc::clone(&got);
    let open = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let o2 = Arc::clone(&open);
    let mut so = SubscriberOptions::new(
        opts(&u, &topic),
        handler(move |_m| {
            let g = Arc::clone(&g);
            async move {
                g.fetch_add(1, Ordering::Relaxed);
            }
        }),
    );
    so.gate = Some(Arc::new(move || o2.load(Ordering::Relaxed)));
    let sub = Subscriber::new(so).unwrap();
    sub.start().await.unwrap();
    sub.wait_connected(Duration::from_secs(5)).await.unwrap();
    let publ = Publisher::new(PublisherOptions {
        options: opts(&u, &topic),
        max_payload: 0,
        queue_capacity: 0,
    })
    .unwrap();
    publ.start().await.unwrap();
    publ.wait_connected(Duration::from_secs(5)).await.unwrap();
    round_trip(&sub).await;
    publish_n(&publ, 3).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(got.load(Ordering::Relaxed), 0);
    assert_eq!(sub.stats().gated, 3);
    open.store(true, Ordering::Relaxed);
    publish_n(&publ, 2).await;
    wait_count(&got, 2, Duration::from_secs(3)).await;
    sub.stop().await;
    publ.stop().await;
}

#[tokio::test]
async fn pause_resume_stops_delivery_and_restores() {
    let Some(u) = url() else { return };
    let _quiet = DISRUPTION.read().await;
    let f = fixture(&u, &unique("pause"), Workers::Pool(0)).await;
    round_trip(&f.sub).await;
    publish_n(&f.publ, 3).await;
    wait_count(&f.got, 3, Duration::from_secs(3)).await;
    assert!(!f.sub.is_paused());

    f.sub.pause().await.unwrap();
    f.sub.pause().await.unwrap(); // idempotent
    round_trip(&f.sub).await;
    publish_n(&f.publ, 5).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        f.got.load(Ordering::Relaxed),
        3,
        "paused subscriber still received"
    );
    let st = f.sub.stats();
    assert!(st.paused && st.pauses == 1 && st.connected, "{st:?}");

    f.sub.resume().await.unwrap();
    f.sub.resume().await.unwrap();
    round_trip(&f.sub).await;
    publish_n(&f.publ, 2).await;
    wait_count(&f.got, 5, Duration::from_secs(3)).await;
    assert!(!f.sub.stats().paused);

    f.sub.stop().await;
    assert!(matches!(f.sub.pause().await, Err(Error::Stopped)));
    assert!(matches!(f.sub.resume().await, Err(Error::Stopped)));
    f.publ.stop().await;
}

/// Subscribe fails during resume (connection healthy, no rebuild): the paused state is still
/// cleared and the error returned, with `diverged = 1`; the repair task retries with backoff
/// (fails once more on the first round, succeeds on the second), after which delivery
/// resumes and `diverged` returns to zero (same-semantics regression as Go v1.5.1).
#[tokio::test]
async fn resume_subscribe_failure_heals() {
    let Some(u) = url() else { return };
    let _quiet = DISRUPTION.read().await;
    let f = fixture(&u, &unique("heal"), Workers::Pool(0)).await;
    f.sub.pause().await.unwrap();
    f.sub.fail_next_subscribes_for_test(2);
    assert!(f.sub.resume().await.is_err());
    let st = f.sub.stats();
    assert!(!st.paused && st.diverged == 1 && st.connected, "{st:?}");
    round_trip(&f.sub).await;
    publish_n(&f.publ, 2).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        f.got.load(Ordering::Relaxed),
        0,
        "delivered with no subscription"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while f.sub.stats().diverged != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "not healed: {:?}",
            f.sub.stats()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    round_trip(&f.sub).await;
    publish_n(&f.publ, 3).await;
    wait_count(&f.got, 3, Duration::from_secs(3)).await;

    // Pause again while the repair task is running: it exits according to the latest paused
    // state and does not restore the subscription.
    f.sub.pause().await.unwrap();
    f.sub.fail_next_subscribes_for_test(1);
    assert!(f.sub.resume().await.is_err());
    f.sub.pause().await.unwrap();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let st = f.sub.stats();
    assert!(st.paused && st.diverged == 0, "{st:?}");
    round_trip(&f.sub).await;
    publish_n(&f.publ, 2).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(f.got.load(Ordering::Relaxed), 3, "delivered while paused");

    f.sub.stop().await;
    f.publ.stop().await;
}

#[tokio::test]
async fn pause_before_start_then_resume() {
    let Some(u) = url() else { return };
    let _quiet = DISRUPTION.read().await;
    let topic = unique("prestart");
    let got = Arc::new(AtomicU64::new(0));
    let g = Arc::clone(&got);
    let sub = Subscriber::new(SubscriberOptions::new(
        opts(&u, &topic),
        handler(move |_m| {
            let g = Arc::clone(&g);
            async move {
                g.fetch_add(1, Ordering::Relaxed);
            }
        }),
    ))
    .unwrap();
    sub.pause().await.unwrap();
    sub.start().await.unwrap();
    sub.wait_connected(Duration::from_secs(5)).await.unwrap();
    let publ = Publisher::new(PublisherOptions {
        options: opts(&u, &topic),
        max_payload: 0,
        queue_capacity: 0,
    })
    .unwrap();
    publ.start().await.unwrap();
    publ.wait_connected(Duration::from_secs(5)).await.unwrap();
    round_trip(&sub).await;
    publish_n(&publ, 3).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(got.load(Ordering::Relaxed), 0);
    sub.resume().await.unwrap();
    round_trip(&sub).await;
    publish_n(&publ, 2).await;
    wait_count(&got, 2, Duration::from_secs(3)).await;
    sub.stop().await;
    publ.stop().await;
}

/// Backpressure in queue-group + worker-pool mode is observable: when the queue fills up,
/// `dropped` grows instead of an unbounded pile-up.
#[tokio::test]
async fn worker_pool_backpressure_is_visible() {
    let Some(u) = url() else { return };
    let _quiet = DISRUPTION.read().await;
    let topic = unique("backpressure");
    let got = Arc::new(AtomicU64::new(0));
    let g = Arc::clone(&got);
    let mut so = SubscriberOptions::new(
        opts(&u, &topic),
        handler(move |_m| {
            let g = Arc::clone(&g);
            async move {
                tokio::time::sleep(Duration::from_millis(200)).await; // slow handler
                g.fetch_add(1, Ordering::Relaxed);
            }
        }),
    );
    so.workers = Workers::Pool(1);
    so.queue_size = 4;
    let sub = Subscriber::new(so).unwrap();
    sub.start().await.unwrap();
    sub.wait_connected(Duration::from_secs(5)).await.unwrap();
    let publ = Publisher::new(PublisherOptions {
        options: opts(&u, &topic),
        max_payload: 0,
        queue_capacity: 0,
    })
    .unwrap();
    publ.start().await.unwrap();
    publ.wait_connected(Duration::from_secs(5)).await.unwrap();
    round_trip(&sub).await;
    publish_n(&publ, 50).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let st = sub.stats();
    assert!(st.dropped > 0, "expected drops under backpressure: {st:?}");
    assert_eq!(st.queue_cap, 4);
    sub.stop().await;
    publ.stop().await;
}

// Current-thread runtime keeps the socket task from draining the command queue
// between these synchronous calls. This exercises async-nats' actual enqueue
// boundary: rejection must neither enqueue a second copy nor block the caller.
#[tokio::test]
async fn publisher_full_queue_drops_once_and_recovers() {
    let Some(u) = url() else { return };
    let _quiet = DISRUPTION.read().await;
    let topic = unique("publish-full");
    let f = fixture(&u, &topic, Workers::Sync).await;
    f.publ.stop().await;
    round_trip(&f.sub).await;
    let p = Publisher::new(PublisherOptions {
        options: opts(&u, &topic),
        max_payload: 0,
        queue_capacity: 1,
    })
    .unwrap();
    p.start().await.unwrap();
    p.wait_connected(Duration::from_secs(5)).await.unwrap();
    p.publish(Bytes::from_static(b"accepted")).unwrap();
    assert!(matches!(
        p.publish(Bytes::from_static(b"rejected")),
        Err(Error::BufferFull(1))
    ));
    assert_eq!(p.stats().dropped, 1);
    p.client().unwrap().flush().await.unwrap();
    wait_count(&f.got, 1, Duration::from_secs(3)).await;
    assert_eq!(f.got.load(Ordering::Relaxed), 1);
    p.publish(Bytes::from_static(b"recovered")).unwrap();
    p.client().unwrap().flush().await.unwrap();
    wait_count(&f.got, 2, Duration::from_secs(3)).await;
    assert_eq!(f.got.load(Ordering::Relaxed), 2);
    assert_eq!(p.stats().dropped, 1);
    f.sub.stop().await;
    p.stop().await;
}

#[tokio::test]
async fn publisher_pool_health_first_and_oversized() {
    let Some(u) = url() else { return };
    let _quiet = DISRUPTION.read().await;
    let topic = unique("pool");
    let f = fixture(&u, &topic, Workers::Pool(0)).await;
    let pool = PublisherPool::new(
        3,
        PublisherOptions {
            options: opts(&u, &topic),
            max_payload: 64,
            queue_capacity: 0,
        },
    )
    .unwrap();
    pool.start().await.unwrap();
    pool.wait_connected(Duration::from_secs(5)).await.unwrap();
    assert_eq!(pool.size(), 3);
    assert_eq!(pool.endpoints(), 1);
    assert_eq!(pool.connected_count(), 3);
    round_trip(&f.sub).await;
    for i in 0..9 {
        pool.publish(Bytes::from(format!("p{i}"))).unwrap();
    }
    wait_count(&f.got, 9, Duration::from_secs(3)).await;
    let err = pool.publish(Bytes::from(vec![0u8; 65])).unwrap_err();
    assert!(matches!(err, Error::Oversized { .. }), "{err}");
    assert_eq!(pool.dropped(), 1);
    let names: Vec<String> = pool.stats().into_iter().map(|s| s.name).collect();
    assert!(
        names.iter().all(|n| n.starts_with("natslink-pub-")),
        "{names:?}"
    );
    pool.stop().await;
    f.sub.stop().await;
    f.publ.stop().await;
}

#[tokio::test]
async fn pause_survives_server_restart() {
    let (Some(u), Some(c)) = (url(), container()) else {
        return;
    };
    let _exclusive = DISRUPTION.write().await;
    let f = fixture(&u, &unique("restart"), Workers::Pool(0)).await;
    round_trip(&f.sub).await;
    publish_n(&f.publ, 1).await;
    wait_count(&f.got, 1, Duration::from_secs(3)).await;
    f.sub.pause().await.unwrap();
    round_trip(&f.sub).await;

    let out = std::process::Command::new("docker")
        .args(["restart", &c])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "docker restart: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // First wait until the clients have really gone through a reconnect (the socket may
    // drop after `docker restart` returns), then wait for CONNECTED again.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while f.sub.stats().reconnects < 1 || f.publ.stats().reconnects < 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no reconnect observed: {:?}",
            f.sub.stats()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    f.sub.wait_connected(Duration::from_secs(30)).await.unwrap();
    f.publ
        .wait_connected(Duration::from_secs(30))
        .await
        .unwrap();
    assert!(f.sub.is_paused(), "paused flag lost across reconnect");

    round_trip(&f.sub).await;
    publish_n(&f.publ, 3).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        f.got.load(Ordering::Relaxed),
        1,
        "paused subscription revived after reconnect"
    );

    f.sub.resume().await.unwrap();
    round_trip(&f.sub).await;
    publish_n(&f.publ, 2).await;
    wait_count(&f.got, 3, Duration::from_secs(5)).await;
    f.sub.stop().await;
    f.publ.stop().await;
}

#[tokio::test]
async fn mirrored_dual_servers_pause_resume() {
    let (Some(u1), Some(u2)) = (url(), url2()) else {
        return;
    };
    let _quiet = DISRUPTION.read().await;
    let topic = unique("mirror");
    let got = Arc::new(AtomicU64::new(0));
    let g = Arc::clone(&got);
    let sub = Subscriber::new_mirrored(
        vec![u1.clone(), u2.clone()],
        SubscriberOptions::new(
            opts(&u1, &topic),
            handler(move |_m| {
                let g = Arc::clone(&g);
                async move {
                    g.fetch_add(1, Ordering::Relaxed);
                }
            }),
        ),
    )
    .unwrap();
    sub.start().await.unwrap();
    sub.wait_connected(Duration::from_secs(5)).await.unwrap();
    assert_eq!(sub.endpoints(), 2);
    let pool = PublisherPool::new_mirrored(
        vec![u1.clone(), u2.clone()],
        2,
        PublisherOptions {
            options: opts(&u1, &topic),
            max_payload: 0,
            queue_capacity: 0,
        },
    )
    .unwrap();
    pool.start().await.unwrap();
    pool.wait_connected(Duration::from_secs(5)).await.unwrap();
    assert_eq!(pool.size(), 4);
    assert_eq!(pool.endpoints(), 2);

    // A round trip on one side cannot confirm when each server has processed its own SUB,
    // so warm both paths with probe messages until one message yields two copies before
    // counting (a mirrored publisher sends once per endpoint, a mirrored subscriber
    // receives once per endpoint).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        got.store(0, Ordering::Relaxed);
        pool.publish(Bytes::from("warm")).unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        if got.load(Ordering::Relaxed) >= 2 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "mirror never delivered both copies"
        );
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    got.store(0, Ordering::Relaxed);
    for i in 0..3 {
        pool.publish(Bytes::from(format!("m{i}"))).unwrap();
    }
    wait_count(&got, 6, Duration::from_secs(3)).await;

    sub.pause().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    for i in 0..3 {
        pool.publish(Bytes::from(format!("x{i}"))).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(got.load(Ordering::Relaxed), 6, "mirrored pause leaked");

    sub.resume().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let before = sub.endpoint_stats();
    pool.publish(Bytes::from("y")).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while got.load(Ordering::Relaxed) < 8 {
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "after resume received {} want 8\nbefore={:#?}\nafter={:#?}\npool={:#?}",
                got.load(Ordering::Relaxed),
                before,
                sub.endpoint_stats(),
                pool.stats()
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let names: Vec<String> = pool.stats().into_iter().map(|s| s.name).collect();
    assert!(names.iter().any(|n| n.contains("-e1-")), "{names:?}");
    pool.stop().await;
    sub.stop().await;
}

/// Permanent close → self-healing rebuild path. async-nats never gives up on its own, so
/// `tweak` forces `max_reconnects` to 1 on this subscriber connection: after `docker stop`
/// the client quickly gives up (`ClientError::MaxReconnects` + `Closed`), and this crate must
/// rebuild exactly once (`rebuilds == 1`; the two events must not each trigger one). The
/// rebuilt connection connects and re-subscribes on its own once the server is back, and
/// delivery resumes.
#[tokio::test]
async fn permanent_close_triggers_single_rebuild_and_resubscribe() {
    let (Some(u), Some(c)) = (url(), container()) else {
        return;
    };
    let _exclusive = DISRUPTION.write().await;
    let topic = unique("rebuild");
    let got = Arc::new(AtomicU64::new(0));
    let g = Arc::clone(&got);
    let mut o = opts(&u, &topic);
    o.tweak = Some(Arc::new(|co: async_nats::ConnectOptions| {
        co.max_reconnects(Some(1))
    }));
    let sub = Subscriber::new(SubscriberOptions::new(
        o,
        handler(move |_m| {
            let g = Arc::clone(&g);
            async move {
                g.fetch_add(1, Ordering::Relaxed);
            }
        }),
    ))
    .unwrap();
    sub.start().await.unwrap();
    sub.wait_connected(Duration::from_secs(5)).await.unwrap();
    let publ = Publisher::new(PublisherOptions {
        options: opts(&u, &topic),
        max_payload: 0,
        queue_capacity: 0,
    })
    .unwrap();
    publ.start().await.unwrap();
    publ.wait_connected(Duration::from_secs(5)).await.unwrap();
    round_trip(&sub).await;
    publish_n(&publ, 1).await;
    wait_count(&got, 1, Duration::from_secs(3)).await;

    let out = std::process::Command::new("docker")
        .args(["stop", &c])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "docker stop: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // Wait for at least one completed rebuild (the rebuild dial uses
    // retry_on_initial_connect, so a new connection is installed even before the server
    // is back).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(40);
    while sub.stats().rebuilds < 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no rebuild observed: {:?}",
            sub.stats()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let out = std::process::Command::new("docker")
        .args(["start", &c])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "docker start: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    sub.wait_connected(Duration::from_secs(60)).await.unwrap();
    publ.wait_connected(Duration::from_secs(60)).await.unwrap();
    // Give the rebuilt connection a moment to deliver its queued SUB (the new connection
    // only completes its handshake once the server is back).
    round_trip(&sub).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    publish_n(&publ, 2).await;
    wait_count(&got, 3, Duration::from_secs(10)).await;
    let st = sub.stats();
    assert!(st.rebuilds >= 1 && st.connected && !st.paused, "{st:?}");
    sub.stop().await;
    publ.stop().await;
}

/// v1.8.3: subscribers can share one FIFO across subjects and endpoints.
/// Gate refusal must not mark the payload; first admitted copy wins.
#[tokio::test]
async fn mirrored_shared_dedup_gate_and_custom_key() {
    use natslink::DedupSet;
    use std::sync::atomic::AtomicBool;
    let (Some(u), Some(v)) = (url(), url2()) else {
        return;
    };
    let _quiet = DISRUPTION.read().await;
    let topics = [unique("dedup_a"), unique("dedup_b")];
    let dedup = Arc::new(DedupSet::new(8, Duration::ZERO));
    let enabled = Arc::new(AtomicBool::new(false));
    let (send, mut recv) = tokio::sync::mpsc::unbounded_channel();
    let mut subs = Vec::new();
    for topic in &topics {
        let send = send.clone();
        let mut so = SubscriberOptions::new(
            opts(&u, topic),
            handler(move |msg| {
                let send = send.clone();
                async move {
                    send.send(msg.payload).unwrap();
                }
            }),
        );
        let enabled = Arc::clone(&enabled);
        so.gate = Some(Arc::new(move || enabled.load(Ordering::Acquire)));
        so.dedup = Some(Arc::clone(&dedup));
        // None is a deliberate opt-out, not a fallback payload hash.
        so.dedup_key = Some(Arc::new(|msg| {
            msg.payload
                .first()
                .copied()
                .filter(|v| *v != 0)
                .map(u64::from)
        }));
        let sub = Subscriber::new_mirrored(vec![u.clone(), v.clone()], so).unwrap();
        sub.start().await.unwrap();
        sub.wait_connected(Duration::from_secs(5)).await.unwrap();
        subs.push(sub);
    }
    let pool = Publisher::new_mirrored(
        vec![u.clone(), v],
        PublisherOptions {
            options: opts(&u, &topics[0]),
            ..Default::default()
        },
    )
    .unwrap();
    pool.start().await.unwrap();
    pool.wait_connected(Duration::from_secs(5)).await.unwrap();
    // Wait for the gated warm-up to traverse both subscription endpoints.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "gated mirror failed to become ready"
        );
        pool.publish(Bytes::from_static(b"ready")).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        if subs[0].stats().gated >= 2 {
            break;
        }
    }
    enabled.store(true, Ordering::Release);
    pool.publish(Bytes::from_static(b"ready")).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), recv.recv())
            .await
            .unwrap()
            .unwrap(),
        b"ready"[..]
    );
    let single = Publisher::new(PublisherOptions {
        options: opts(&u, &topics[1]),
        ..Default::default()
    })
    .unwrap();
    single.start().await.unwrap();
    single.wait_connected(Duration::from_secs(5)).await.unwrap();
    single
        .publish(Bytes::from_static(b"replacement-same-key"))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while subs[0].stats().deduped < 1 || subs[1].stats().deduped < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(recv.try_recv().is_err());
    single.publish(Bytes::from_static(b"\0bypass")).unwrap();
    single.publish(Bytes::from_static(b"\0bypass")).unwrap();
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(3), recv.recv())
            .await
            .unwrap()
            .unwrap();
    }
    single.stop().await;
    pool.stop().await;
    for sub in subs {
        sub.stop().await;
    }
}

#[tokio::test]
async fn mirror_stop_drains_accepted_messages_and_second_start_is_harmless() {
    let (Some(u), Some(v)) = (url(), url2()) else {
        return;
    };
    let _quiet = DISRUPTION.read().await;
    let topic = unique("drain183");
    let a = fixture(&u, &topic, Workers::Sync).await;
    let b = fixture(&v, &topic, Workers::Sync).await;
    round_trip(&a.sub).await;
    round_trip(&b.sub).await;
    let pool = PublisherPool::new_mirrored(
        vec![u.clone(), v],
        2,
        PublisherOptions {
            options: opts(&u, &topic),
            queue_capacity: 1024,
            ..Default::default()
        },
    )
    .unwrap();
    pool.start().await.unwrap();
    pool.wait_connected(Duration::from_secs(5)).await.unwrap();
    assert!(matches!(pool.start().await, Err(Error::AlreadyStarted)));
    for i in 0..100 {
        pool.publish(Bytes::from(format!("drain{i}"))).unwrap();
    }
    tokio::join!(pool.stop(), pool.stop());
    wait_count(&a.got, 100, Duration::from_secs(3)).await;
    wait_count(&b.got, 100, Duration::from_secs(3)).await;
    assert_eq!(pool.dropped(), 0);
    assert!(matches!(pool.publish(Bytes::new()), Err(Error::Stopped)));
    a.sub.stop().await;
    b.sub.stop().await;
    a.publ.stop().await;
    b.publ.stop().await;
}
