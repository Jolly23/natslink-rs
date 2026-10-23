//! Opt-in loopback benchmark, no default server or production discovery.
//! NATS_TEST_URL / NATS_TEST_URL2: two independent local test servers.
//! NATS_BENCH_DEDUP=1 enables the v1.8.3 byte-copy filter.
use natslink::{
    handler, Bytes, DedupSet, Options, PublisherOptions, PublisherPool, ReportInterval, Subscriber,
    SubscriberOptions, Workers,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

fn options(url: String, topic: &str) -> Options {
    Options {
        url,
        topic: topic.into(),
        report_interval: ReportInterval::Off,
        ..Default::default()
    }
}
fn percentile(values: &mut [u128], numerator: usize) -> u128 {
    values.sort_unstable();
    values[(values.len() - 1) * numerator / 100]
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let urls = vec![
        std::env::var("NATS_TEST_URL").unwrap(),
        std::env::var("NATS_TEST_URL2").unwrap(),
    ];
    for u in &urls {
        assert!(
            u.contains("@127.0.0.1:"),
            "local authenticated fixtures only"
        );
    }
    let dedup = std::env::var("NATS_BENCH_DEDUP").as_deref() == Ok("1");
    let rounds: usize = std::env::var("NATS_BENCH_ROUNDS")
        .ok()
        .map(|s| s.parse().unwrap())
        .unwrap_or(1000);
    for size in [1024, 65536] {
        let topic = format!("natslink.bench.{}.{}", std::process::id(), size);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut so = SubscriberOptions::new(
            options(urls[0].clone(), &topic),
            handler(move |msg| {
                let tx = tx.clone();
                async move {
                    let seq = u64::from_le_bytes(msg.payload[..8].try_into().unwrap());
                    tx.send(seq).unwrap();
                }
            }),
        );
        so.workers = Workers::Pool(8);
        if dedup {
            so.dedup = Some(Arc::new(DedupSet::new(4096, Duration::ZERO)));
        }
        let sub = Subscriber::new_mirrored(urls.clone(), so).unwrap();
        sub.start().await.unwrap();
        sub.wait_connected(Duration::from_secs(5)).await.unwrap();
        let pool = PublisherPool::new_mirrored(
            urls.clone(),
            2,
            PublisherOptions {
                options: options(urls[0].clone(), &topic),
                max_payload: 16 << 20,
                queue_capacity: 16,
            },
        )
        .unwrap();
        pool.start().await.unwrap();
        pool.wait_connected(Duration::from_secs(5)).await.unwrap();
        // Warm subscription propagation before measuring; measured rounds are barrier-paced.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut publish = Vec::new();
        let mut first = Vec::new();
        let mut complete = Vec::new();
        let total = Instant::now();
        for seq in 0..rounds + 100 {
            let mut payload = vec![0x5a; size];
            payload[..8].copy_from_slice(&(seq as u64).to_le_bytes());
            let payload = Bytes::from(payload);
            let start = Instant::now();
            pool.publish(payload).unwrap();
            let enqueue = start.elapsed().as_nanos();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), rx.recv())
                    .await
                    .unwrap(),
                Some(seq as u64)
            );
            let arrived = start.elapsed().as_nanos();
            if !dedup {
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(5), rx.recv())
                        .await
                        .unwrap(),
                    Some(seq as u64)
                );
            }
            if seq >= 100 {
                publish.push(enqueue);
                first.push(arrived);
                complete.push(start.elapsed().as_nanos());
            }
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while sub.stats().in_msgs < 2 * (rounds + 100) as u64
                || (dedup && sub.stats().deduped != (rounds + 100) as u64)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        println!("version={} size={} rounds={} dedup={} publish_ns={}/{}/{} first_ns={}/{}/{} complete_ns={}/{}/{} elapsed_ms={} in_msgs={} deduped={} dropped={}",
            natslink::VERSION, size, rounds, dedup,
            percentile(&mut publish, 50), percentile(&mut publish, 95), percentile(&mut publish, 99),
            percentile(&mut first, 50), percentile(&mut first, 95), percentile(&mut first, 99),
            percentile(&mut complete, 50), percentile(&mut complete, 95), percentile(&mut complete, 99),
            total.elapsed().as_millis(), sub.stats().in_msgs, sub.stats().deduped, pool.dropped() + sub.stats().dropped);
        pool.stop().await;
        sub.stop().await;
    }
}
