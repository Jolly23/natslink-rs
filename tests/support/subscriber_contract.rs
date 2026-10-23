// Included inside subscriber.rs's test module to exercise real private dispatch.
fn contract_payload(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0);
    (0..hex.len())
        .step_by(2)
        .map(|offset| u8::from_str_radix(&hex[offset..offset + 2], 16).unwrap())
        .collect()
}

#[tokio::test]
async fn shared_subscriber_contract() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../testdata/contract-v1.json")).unwrap();
    assert_eq!(fixture["version"], 1);
    let cases = fixture["subscriber_cases"].as_array().unwrap();
    assert!(!cases.is_empty());
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let set = Arc::new(DedupSet::new(
            case["entries"].as_u64().unwrap() as usize,
            Duration::ZERO,
        ));
        let delivered = Arc::new(AtomicU64::new(0));
        let gate = Arc::new(AtomicBool::new(true));
        let key = Arc::new(AtomicU64::new(0));
        let key_ok = Arc::new(AtomicBool::new(false));
        let mut subscribers: std::collections::HashMap<String, Subscriber> =
            std::collections::HashMap::new();
        for (index, step) in case["steps"].as_array().unwrap().iter().enumerate() {
            let subject = step["subject"].as_str().unwrap();
            let sub = subscribers.entry(subject.to_owned()).or_insert_with(|| {
                let mut opts = options(subject, delivered.clone());
                opts.dedup = Some(set.clone());
                opts.gate = Some({
                    let gate = gate.clone();
                    Arc::new(move || gate.load(Ordering::Relaxed))
                });
                if case["custom_key"].as_bool().unwrap() {
                    opts.dedup_key = Some({
                        let key = key.clone();
                        let key_ok = key_ok.clone();
                        Arc::new(move |_| {
                            key_ok
                                .load(Ordering::Relaxed)
                                .then(|| key.load(Ordering::Relaxed))
                        })
                    });
                }
                Subscriber::new(opts).unwrap()
            });
            gate.store(step["gate"].as_bool().unwrap(), Ordering::Relaxed);
            key.store(step["key"].as_u64().unwrap_or(0), Ordering::Relaxed);
            key_ok.store(step["key_ok"].as_bool().unwrap_or(false), Ordering::Relaxed);
            let before = sub.stats();
            let previous_delivered = delivered.load(Ordering::Relaxed);
            let data = contract_payload(step["payload_hex"].as_str().unwrap());
            sub.inner
                .dispatch(Message {
                    subject: subject.to_owned().into(),
                    reply: None,
                    length: subject.len() + data.len(),
                    payload: data.into(),
                    headers: None,
                    status: None,
                    description: None,
                })
                .await;
            let after = sub.stats();
            let actual = (
                delivered.load(Ordering::Relaxed) - previous_delivered,
                after.gated - before.gated,
                after.deduped - before.deduped,
            );
            let expected = match step["expected"].as_str().unwrap() {
                "delivered" => (1, 0, 0),
                "gated" => (0, 1, 0),
                "deduped" => (0, 0, 1),
                value => panic!("unknown contract outcome {value}"),
            };
            assert_eq!(actual, expected, "{name}, step {index}");
        }
        for (_, sub) in subscribers {
            sub.stop().await;
        }
    }
}
