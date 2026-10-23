//! Rust-side regression for an escaping form reported by a user of the Go module (natslink
//! v1.5.2): when the userinfo contains double quotes / backslashes / control characters /
//! non-ASCII / percent-encoding, the errors from `probe` and `start` (both `Display` and
//! `Debug`) must not carry any fragment of the credentials or any escaped form of it (the
//! probe error; after `start`, the `wait_connected` timeout error and the stats snapshot).
//! The Rust port strips the userinfo itself (`split_auth`) before handing the URL to
//! async-nats, so async-nats errors never contain credentials and the Go `%q` escaping
//! bypass cannot happen here; this test pins that down.
//! Comma-separated lists (ordered failover within one endpoint) are covered both ways:
//! identical credentials per segment → normal start; differing credentials → start returns
//! a configuration error (async-nats has one global credential set and cannot match
//! nats.go's per-server userinfo), and that error must not carry credentials either.
//! No NATS server is needed (ports 1 / 2 are refused outright).

use natslink::{probe, Options, Publisher, PublisherOptions, ReportInterval};

const CASES: &[(&str, &[&str])] = &[
    ("nats://Q1tok\"Q2tok@127.0.0.1:1", &["Q1tok", "Q2tok"]),
    (
        "nats://Q1usr:Q2pa\"Q3pa@127.0.0.1:1",
        &["Q1usr", "Q2pa", "Q3pa"],
    ),
    ("nats://Q1bs\\Q2bs@127.0.0.1:1", &["Q1bs", "Q2bs"]),
    ("nats://Q1nl\nQ2nl@127.0.0.1:1", &["Q1nl", "Q2nl"]),
    ("nats://Q1cc\u{1}Q2cc@127.0.0.1:1", &["Q1cc", "Q2cc"]),
    ("nats://Q1\u{e9}Q2na\"x@127.0.0.1:1", &["Q1\u{e9}", "Q2na"]),
    ("nats://Q1pc%22Q2pc\"@127.0.0.1:1", &["Q1pc", "Q2pc"]),
    ("nats://Q1be%ZZQ2be@127.0.0.1:1", &["Q1be", "Q2be"]),
    (
        "nats://Q1uc\"Q2uc@127.0.0.1:1,nats://Q1uc\"Q2uc@127.0.0.1:2",
        &["Q1uc", "Q2uc"],
    ),
    // The only case start must reject: credentials differ between segments.
    (
        "nats://127.0.0.1:1,nats://Q1cl\"Q2cl@127.0.0.1:2",
        &["Q1cl", "Q2cl"],
    ),
];

fn forms(s: &str) -> Vec<String> {
    let dbg = format!("{s:?}");
    vec![
        s.to_string(),
        dbg[1..dbg.len() - 1].to_string(),
        s.escape_default().to_string(),
        s.escape_unicode().to_string(),
    ]
}

fn assert_clean(case: &str, pieces: &[&str], text: &str) {
    for p in pieces {
        for f in forms(p) {
            assert!(
                !text.contains(&f),
                "{case:?}: piece {p:?} (form {f:?}) leaked in {text:?}"
            );
        }
    }
}

#[tokio::test]
async fn quoted_userinfo_never_leaks() {
    let (mut started, mut rejected) = (0, 0);
    for (raw, pieces) in CASES {
        let e = probe(raw).await.expect_err("probe to port 1 must fail");
        assert_clean(raw, pieces, &format!("{e} | {e:?}"));

        let options = Options {
            url: raw.to_string(),
            topic: "offline".into(),
            name: String::new(),
            report_interval: ReportInterval::Off,
            tweak: None,
        };
        assert_clean(raw, pieces, &format!("{options:?}"));
        let p = Publisher::new(PublisherOptions {
            options,
            max_payload: 0,
            queue_capacity: 0,
        })
        .unwrap();
        // Both paths need explicit assertions:
        // - Ordinary: with retry_on_initial_connect a failed connect is not a start error;
        //   start succeeds and the connection retries in the background. Credentials could
        //   surface in the wait_connected timeout error (status and last_err) and in the
        //   stats snapshot.
        // - Configuration error: a comma list with differing credentials is rejected at
        //   start, and that error text must not carry credentials either.
        match p.start().await {
            Ok(()) => {
                started += 1;
                let e = p
                    .wait_connected(std::time::Duration::from_millis(300))
                    .await
                    .expect_err("port 1 never connects");
                assert_clean(raw, pieces, &format!("{e} | {e:?}"));
                assert_clean(raw, pieces, &format!("{:?}", p.stats()));
            }
            Err(e) => {
                rejected += 1;
                let text = format!("{e} | {e:?}");
                assert!(
                    text.contains("different credentials"),
                    "{raw:?}: only the mixed-credential comma list may fail start: {e}"
                );
                assert_clean(raw, pieces, &text);
            }
        }
        p.stop().await;
    }
    assert_eq!(
        (started, rejected),
        (CASES.len() - 1, 1),
        "both start paths must be exercised"
    );
}
