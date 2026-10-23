//! Local-broker regressions for mirror backpressure and wrapper traffic counters.
//! Set NATS_TEST_URL and NATS_TEST_URL2 to disposable servers to enable them.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use natslink::{
    handler, Bytes, Options, Publisher, PublisherOptions, PublisherPool, ReportInterval,
    Subscriber, SubscriberOptions, Workers,
};

fn url(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|url| !url.is_empty())
}

fn options(url: &str, topic: &str) -> Options {
    Options {
        url: url.into(),
        topic: topic.into(),
        report_interval: ReportInterval::Off,
        ..Default::default()
    }
}

fn topic(label: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "natslink.buffer.{label}.{}",
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("expected delivery did not arrive");
}

async fn subscriber(url: &str, topic: &str) -> (Subscriber, Arc<AtomicU64>) {
    let count = Arc::new(AtomicU64::new(0));
    let got = count.clone();
    let mut config = SubscriberOptions::new(
        options(url, topic),
        handler(move |_| {
            let got = got.clone();
            async move {
                got.fetch_add(1, Ordering::Relaxed);
            }
        }),
    );
    config.workers = Workers::Sync;
    let sub = Subscriber::new(config).unwrap();
    sub.start().await.unwrap();
    sub.wait_connected(Duration::from_secs(5)).await.unwrap();
    // A no-responder response fences the preceding SUB on this connection.
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        sub.client()
            .unwrap()
            .request("natslink.buffer.unanswered", Bytes::new()),
    )
    .await
    .unwrap();
    (sub, count)
}

async fn burst() {
    let (Some(a), Some(b)) = (url("NATS_TEST_URL"), url("NATS_TEST_URL2")) else {
        return;
    };
    let topic = topic("burst");
    let (sub_a, got_a) = subscriber(&a, &topic).await;
    let (sub_b, got_b) = subscriber(&b, &topic).await;
    let pool = PublisherPool::new_mirrored(
        vec![a.clone(), b],
        2,
        PublisherOptions {
            options: options(&a, &topic),
            queue_capacity: 1,
            max_payload: 0,
        },
    )
    .unwrap();
    pool.start().await.unwrap();
    pool.wait_connected(Duration::from_secs(5)).await.unwrap();
    for _ in 0..2000 {
        pool.publish(Bytes::from_static(&[7; 512])).unwrap();
    }
    // Stop must drain accepted mirror copies before closing the members. A
    // cancelled first waiter must not cancel this owned drain operation.
    let pool = Arc::new(pool);
    let first = tokio::spawn({
        let pool = pool.clone();
        async move { pool.stop().await }
    });
    tokio::task::yield_now().await;
    first.abort();
    tokio::time::timeout(Duration::from_secs(11), pool.stop())
        .await
        .unwrap();
    until(|| got_a.load(Ordering::Relaxed) == 2000 && got_b.load(Ordering::Relaxed) == 2000).await;
    assert_eq!(pool.dropped(), 0);
    assert!(pool
        .stats()
        .iter()
        .all(|stats| stats.dropped == 0 && stats.queue_dropped == 0));
    assert_eq!(
        pool.stats().iter().map(|stats| stats.out_msgs).sum::<u64>(),
        4000
    );
    sub_a.stop().await;
    sub_b.stop().await;
}

#[tokio::test]
async fn mirror_burst_waits_for_capacity_on_one_runtime_thread() {
    burst().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn mirror_burst_waits_for_capacity_on_multiple_runtime_threads() {
    burst().await;
}

#[tokio::test]
async fn offline_endpoint_does_not_delay_healthy_mirror_and_stop_discards_its_tail() {
    let Some(healthy) = url("NATS_TEST_URL") else {
        return;
    };
    let topic = topic("offline");
    let (sub, got) = subscriber(&healthy, &topic).await;
    // Keep a loopback port reserved but never accept/read a NATS handshake.
    let stalled = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let offline = format!("nats://{}", stalled.local_addr().unwrap());
    let pool = PublisherPool::new_mirrored(
        vec![offline, healthy.clone()],
        1,
        PublisherOptions {
            options: options(&healthy, &topic),
            queue_capacity: 1,
            max_payload: 0,
        },
    )
    .unwrap();
    pool.start().await.unwrap();
    until(|| pool.connected_count() == 1).await;
    for _ in 0..100 {
        pool.publish(Bytes::from_static(b"sample")).unwrap();
    }
    until(|| got.load(Ordering::Relaxed) == 100).await;
    assert_eq!(pool.dropped(), 0);
    assert!(
        pool.stats()[0].queue_len > 0,
        "stalled endpoint must retain its pending tail"
    );
    tokio::time::timeout(Duration::from_secs(11), pool.stop())
        .await
        .unwrap();
    assert_eq!(
        pool.dropped(),
        0,
        "the healthy endpoint delivered every logical message"
    );
    let stats = pool.stats();
    assert!(stats[0].queue_dropped > 0);
    assert_eq!(
        stats[0].out_msgs + stats[0].dropped + stats[0].queue_dropped,
        100
    );
    assert_eq!(stats[0].queue_len, 0);
    assert_eq!(stats[1].out_msgs, 100);
    sub.stop().await;
}

#[tokio::test]
async fn stopping_two_stalled_endpoints_settles_every_copy_exactly_once() {
    let a = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let b = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let urls = [a.local_addr().unwrap(), b.local_addr().unwrap()]
        .map(|address| format!("nats://{address}"));
    let pool = PublisherPool::new_mirrored(
        urls.to_vec(),
        1,
        PublisherOptions {
            options: options(&urls[0], &topic("settlement")),
            queue_capacity: 1,
            max_payload: 0,
        },
    )
    .unwrap();
    pool.start().await.unwrap();
    for _ in 0..10 {
        pool.publish(Bytes::from_static(b"sample")).unwrap();
    }
    until(|| {
        pool.stats()
            .iter()
            .all(|stats| stats.out_msgs == 1 && stats.queue_len == 8)
    })
    .await;
    tokio::time::timeout(Duration::from_secs(11), pool.stop())
        .await
        .unwrap();
    assert_eq!(
        pool.dropped(),
        9,
        "one logical message was accepted into both client queues"
    );
    let final_counts = pool
        .stats()
        .iter()
        .map(|stats| (stats.out_msgs, stats.dropped, stats.queue_dropped))
        .collect::<Vec<_>>();
    for stats in pool.stats() {
        assert_eq!(stats.out_msgs, 1);
        assert_eq!(stats.out_msgs + stats.dropped + stats.queue_dropped, 10);
        assert_eq!(stats.queue_len, 0);
    }
    // Sender cancellation must have settled before Stop returns, including on
    // a single-thread runtime where closing members can finish first.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(pool.dropped(), 9);
    assert_eq!(
        pool.stats()
            .iter()
            .map(|stats| (stats.out_msgs, stats.dropped, stats.queue_dropped))
            .collect::<Vec<_>>(),
        final_counts
    );
}

#[tokio::test]
async fn traffic_counts_payload_and_encoded_headers_and_survives_stop_flush() {
    let Some(url) = url("NATS_TEST_URL") else {
        return;
    };
    let topic = topic("counters");
    let got = Arc::new(AtomicU64::new(0));
    let received = got.clone();
    let mut config = SubscriberOptions::new(
        options(&url, &topic),
        handler(move |_| {
            let received = received.clone();
            async move {
                received.fetch_add(1, Ordering::Relaxed);
            }
        }),
    );
    config.workers = Workers::Sync;
    config.gate = Some(Arc::new(|| false));
    let sub = Subscriber::new(config).unwrap();
    sub.start().await.unwrap();
    sub.wait_connected(Duration::from_secs(5)).await.unwrap();
    let client = sub.client().unwrap();
    let _ = client
        .request("natslink.buffer.unanswered", Bytes::new())
        .await;
    let publisher = Publisher::new(PublisherOptions {
        options: options(&url, &topic),
        queue_capacity: 256,
        max_payload: 0,
    })
    .unwrap();
    publisher.start().await.unwrap();
    publisher
        .wait_connected(Duration::from_secs(5))
        .await
        .unwrap();
    for _ in 0..100 {
        publisher.publish(Bytes::from_static(b"sample")).unwrap();
    }
    assert_eq!(publisher.stats().out_msgs, 100);
    assert_eq!(publisher.stats().out_bytes, 600);
    publisher.stop().await;
    until(|| sub.stats().in_msgs == 100).await;
    assert_eq!(publisher.stats().out_msgs, 100);
    assert_eq!(publisher.stats().out_bytes, 600);
    assert_eq!(sub.stats().in_bytes, 600);
    assert_eq!(sub.stats().gated, 100);
    let mut headers = natslink::async_nats::HeaderMap::new();
    headers.insert("X-Test", "abc");
    client
        .publish_with_reply_and_headers(
            topic,
            "reply.address",
            headers,
            Bytes::from_static(b"body"),
        )
        .await
        .unwrap();
    until(|| sub.stats().in_msgs == 101).await;
    assert_eq!(
        sub.stats().in_bytes,
        600 + 4 + b"NATS/1.0\r\nX-Test: abc\r\n\r\n".len() as u64
    );
    assert_eq!(
        sub.stats().out_msgs,
        0,
        "escape-hatch traffic is outside wrapper accounting"
    );
    assert_eq!(got.load(Ordering::Relaxed), 0);
    sub.stop().await;
}
