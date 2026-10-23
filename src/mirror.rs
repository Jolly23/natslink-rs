//! Mirroring: mirror the same topic's operations across M mutually independent NATS endpoints.
//!
//! Intended for active-active topologies over several disconnected NATS trees (e.g. one host
//! running two local leaf nodes that each uplink to a different region): a publish is sent once per
//! endpoint, and a subscription is opened once per endpoint. Byte-identical copies can optionally
//! be deduplicated inside the crate; deduplication by content semantics (transaction hashes,
//! sequence numbers, ...) remains the consumer's job. Multi-endpoint publishing uses an independent
//! bounded send queue per endpoint. The mirrored Subscriber / PublisherPool return the same types;
//! Publisher::new_mirrored returns a PublisherPool with one member per endpoint, reusing the same
//! send / statistics implementation. Each endpoint is supervised and self-healed independently and
//! is unaware of the others.
//!
//! Relation to comma-separated URLs: the comma list in `Options.url` is ordered failover within an
//! endpoint (only one connected at a time); the mirror urls are active-active across endpoints (all
//! connected, all used). The two are orthogonal and composable.
//! Topology rule: two local leaf nodes must have distinct server_name values, otherwise the
//! upstream deduplicates them as the same origin and the mirror collapses to a single copy.

use crate::redact::redact_url;
use crate::Error;

/// Builds the connection name for endpoint i: the base name with `-e<i>` appended, so every
/// endpoint is attributable in logs / server monitoring.
pub(crate) fn mirror_endpoint_name(base: &str, role: &str, topic: &str, i: usize) -> String {
    let base = if base.is_empty() {
        format!("natslink-{role}-{topic}")
    } else {
        base.to_string()
    };
    format!("{base}-e{i}")
}

/// Construction-time fail-fast: at least one endpoint, no empty strings, no duplicates.
pub(crate) fn validate_mirror_urls(urls: &[String]) -> Result<(), Error> {
    if urls.is_empty() {
        return Err(Error::Config("mirror requires at least one url".into()));
    }
    let mut seen = std::collections::HashSet::new();
    for u in urls {
        let key = u.trim();
        if key.is_empty() {
            return Err(Error::Config("mirror url is empty".into()));
        }
        if !seen.insert(key.to_string()) {
            return Err(Error::Config(format!(
                "duplicate mirror url {}",
                redact_url(u)
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_validation() {
        assert_eq!(
            mirror_endpoint_name("", "sub", "events.block", 1),
            "natslink-sub-events.block-e1"
        );
        assert_eq!(mirror_endpoint_name("x", "pub", "t", 0), "x-e0");
        assert!(validate_mirror_urls(&[]).is_err());
        assert!(validate_mirror_urls(&["a".into(), " ".into()]).is_err());
        assert!(validate_mirror_urls(&["nats://t@a".into(), "nats://t@a".into()]).is_err());
        assert!(validate_mirror_urls(&["nats://a".into(), "nats://b".into()]).is_ok());
    }
}
