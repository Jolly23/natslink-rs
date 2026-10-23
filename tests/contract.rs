use natslink::{redact_error, redact_url, DedupSet};
use serde_json::Value;
use std::time::Duration;

fn fixtures() -> Value {
    let fixture: Value =
        serde_json::from_str(include_str!("../testdata/contract-v1.json")).unwrap();
    assert_eq!(fixture["version"], 1);
    fixture
}

fn payload(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0, "fixture payload must contain whole bytes");
    (0..hex.len())
        .step_by(2)
        .map(|offset| u8::from_str_radix(&hex[offset..offset + 2], 16).unwrap())
        .collect()
}

#[test]
fn shared_dedup_contract() {
    let fixtures = fixtures();
    let cases = fixtures["dedup_cases"].as_array().unwrap();
    assert!(!cases.is_empty());
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let set = DedupSet::new(
            case["entries"].as_u64().unwrap() as usize,
            Duration::from_millis(case["window_ms"].as_u64().unwrap()),
        );
        for (index, step) in case["steps"].as_array().unwrap().iter().enumerate() {
            if let Some(delay) = step["advance_ms"].as_u64() {
                std::thread::sleep(Duration::from_millis(delay));
            }
            assert_ne!(step.get("key").is_some(), step.get("payload_hex").is_some());
            let seen = if let Some(key) = step["key"].as_u64() {
                set.seen_key(key)
            } else {
                set.seen(&payload(step["payload_hex"].as_str().unwrap()))
            };
            assert_eq!(
                seen,
                step["expected_seen"].as_bool().unwrap(),
                "{name}, step {index}"
            );
        }
    }
}

#[test]
fn shared_url_redaction_contract() {
    let fixtures = fixtures();
    let cases = fixtures["url_cases"].as_array().unwrap();
    assert!(!cases.is_empty());
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let url = case["url"].as_str().unwrap();
        assert_eq!(
            redact_url(url),
            case["redacted"].as_str().unwrap(),
            "{name}"
        );
        let forbidden: Vec<&str> = case["forbidden"]
            .as_array()
            .unwrap()
            .iter()
            .map(|secret| secret.as_str().unwrap())
            .collect();
        let error = format!("failed for {url}: {}", forbidden.join(" "));
        let redacted = redact_error(url, &error);
        for secret in forbidden {
            assert!(
                !redacted.contains(secret),
                "{name}: credential was not redacted"
            );
        }
    }
}
