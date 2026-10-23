//! Percent-encoded URL credentials must authenticate against the same server.
//! NATS_TEST_URL must refer to a disposable local test broker.

use natslink::{probe, Options, Publisher, PublisherOptions, ReportInterval};
use std::time::Duration;

#[tokio::test]
async fn encoded_credentials_authenticate_for_probe_and_publisher() {
    let Ok(url) = std::env::var("NATS_TEST_URL") else {
        return;
    };
    let (scheme, remainder) = url.split_once("://").expect("test URL needs a scheme");
    let (userinfo, address) = remainder
        .rsplit_once('@')
        .expect("test broker needs credentials");
    let encode = |value: &str| -> String {
        value
            .as_bytes()
            .iter()
            .map(|byte| format!("%{byte:02X}"))
            .collect()
    };
    let userinfo = match userinfo.split_once(':') {
        Some((user, password)) => format!("{}:{}", encode(user), encode(password)),
        None => encode(userinfo),
    };
    let encoded = format!("{scheme}://{userinfo}@{address}");
    probe(&encoded).await.unwrap();
    let publisher = Publisher::new(PublisherOptions {
        options: Options {
            url: encoded,
            topic: "contract.authentication".into(),
            report_interval: ReportInterval::Off,
            ..Default::default()
        },
        ..Default::default()
    })
    .unwrap();
    publisher.start().await.unwrap();
    publisher
        .wait_connected(Duration::from_secs(5))
        .await
        .unwrap();
    publisher.stop().await;
}
