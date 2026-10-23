//! Isolated ACL hot-reload integration test, using only disposable local Docker
//! containers and synthetic credentials. Run explicitly:
//! NATSLINK_ACL_DOCKER_TEST=1 cargo test --test rejection -- --ignored --nocapture

use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use natslink::{handler, Bytes, Options, ReportInterval, Subscriber, SubscriberOptions, Workers};

const TOPIC: &str = "natslink.acl.test";

struct Broker {
    name: String,
    dir: PathBuf,
    address: String,
}

impl Broker {
    fn start(label: &str, allow: bool) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!("natslink-acl-{label}-{}-{nonce}", std::process::id());
        let dir = std::env::temp_dir().join(&name);
        std::fs::create_dir(&dir).unwrap();
        let mut broker = Self {
            name,
            dir,
            address: String::new(),
        };
        broker.config(allow);
        let mount = format!("{}:/config:ro", broker.dir.display());
        let output = Command::new("docker")
            .args([
                "run",
                "--detach",
                "--rm",
                "--name",
                &broker.name,
                "--publish",
                "127.0.0.1::4222",
                "--volume",
                &mount,
                "nats:latest",
                "--config",
                "/config/server.conf",
            ])
            .output()
            .expect("Docker must be available locally");
        assert!(
            output.status.success(),
            "docker run: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let port = Command::new("docker")
            .args(["port", &broker.name, "4222/tcp"])
            .output()
            .unwrap();
        assert!(port.status.success());
        broker.address = String::from_utf8(port.stdout).unwrap().trim().to_owned();
        assert!(broker.address.starts_with("127.0.0.1:"));
        broker
    }

    fn config(&self, allow: bool) {
        let subject = if allow { TOPIC } else { "unrelated" };
        let body = format!(
            r#"
port: 4222
authorization {{
  users: [
    {{ user: "test-admin", password: "synthetic-only" }},
    {{ user: "test-observer", password: "synthetic-only",
       permissions: {{ publish: ">", subscribe: "{subject}" }} }}
  ]
}}
"#
        );
        let temp = self.dir.join("next.conf");
        std::fs::write(&temp, body).unwrap();
        std::fs::rename(temp, self.dir.join("server.conf")).unwrap();
    }

    fn allow_and_reload(&self) {
        self.config(true);
        let status = Command::new("docker")
            .args(["kill", "--signal", "HUP", &self.name])
            .output()
            .unwrap();
        assert!(
            status.status.success(),
            "{}",
            String::from_utf8_lossy(&status.stderr)
        );
    }

    fn subscriber_url(&self) -> String {
        format!("nats://test-observer:synthetic-only@{}", self.address)
    }

    async fn publisher(&self) -> natslink::async_nats::Client {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let result = natslink::async_nats::ConnectOptions::new()
                .user_and_password("test-admin".into(), "synthetic-only".into())
                .connect(format!("nats://{}", self.address))
                .await;
            if let Ok(client) = result {
                return client;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "local broker did not start"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.name])
            .output();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn until(timeout: Duration, mut check: impl FnMut() -> bool) {
    tokio::time::timeout(timeout, async {
        while !check() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition not reached");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires explicitly opted-in disposable local Docker brokers"]
async fn denied_subscription_recovers_after_acl_reload_without_rebuilding_healthy_mirror() {
    assert_eq!(
        std::env::var("NATSLINK_ACL_DOCKER_TEST").as_deref(),
        Ok("1")
    );
    let denied = Broker::start("denied", false);
    let healthy = Broker::start("healthy", true);
    let bad_pub = denied.publisher().await;
    let good_pub = healthy.publisher().await;
    let healthy_received = Arc::new(AtomicU64::new(0));
    let recovered_received = Arc::new(AtomicU64::new(0));
    let mut options = SubscriberOptions::new(
        Options {
            topic: TOPIC.into(),
            report_interval: ReportInterval::Off,
            ..Options::default()
        },
        handler({
            let healthy = healthy_received.clone();
            let recovered = recovered_received.clone();
            move |message| {
                let (healthy, recovered) = (healthy.clone(), recovered.clone());
                async move {
                    if message.payload == b"healthy"[..] {
                        healthy.fetch_add(1, Ordering::Relaxed);
                    }
                    if message.payload == b"recovered"[..] {
                        recovered.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }),
    );
    options.workers = Workers::Pool(1);
    let subscriber = Subscriber::new_mirrored(
        vec![denied.subscriber_url(), healthy.subscriber_url()],
        options,
    )
    .unwrap();
    subscriber.start().await.unwrap();
    subscriber
        .wait_connected(Duration::from_secs(5))
        .await
        .unwrap();
    until(Duration::from_secs(5), || subscriber.stats().diverged == 1).await;
    // Retry publishing until the healthy SUB is established; a write flush alone
    // does not prove the server processed the other connection's SUB.
    tokio::time::timeout(Duration::from_secs(5), async {
        while healthy_received.load(Ordering::Relaxed) == 0 {
            good_pub
                .publish(TOPIC, Bytes::from_static(b"healthy"))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let healthy_before = subscriber.endpoint_stats()[1].clone();
    assert_eq!(subscriber.endpoint_stats()[0].rebuilds, 0);
    denied.allow_and_reload();
    tokio::time::timeout(Duration::from_secs(15), async {
        while recovered_received.load(Ordering::Relaxed) == 0 {
            bad_pub
                .publish(TOPIC, Bytes::from_static(b"recovered"))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    until(Duration::from_secs(2), || subscriber.stats().diverged == 0).await;
    let stats = subscriber.endpoint_stats();
    assert!(
        stats[0].rebuilds >= 1,
        "denied endpoint must actually rebuild"
    );
    assert_eq!(stats[1].rebuilds, healthy_before.rebuilds);
    assert_eq!(stats[1].reconnects, healthy_before.reconnects);
    let received = healthy_received.load(Ordering::Relaxed);
    good_pub
        .publish(TOPIC, Bytes::from_static(b"healthy"))
        .await
        .unwrap();
    until(Duration::from_secs(2), || {
        healthy_received.load(Ordering::Relaxed) > received
    })
    .await;
    subscriber.stop().await;
    eprintln!(
        "ACL reload recovered rejected endpoint; healthy mirror reconnects={} rebuilds={}",
        stats[1].reconnects, stats[1].rebuilds
    );
}
