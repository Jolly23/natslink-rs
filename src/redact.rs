//! Log / error redaction: the userinfo of a NATS URL (token or user:pass) never reaches the logs.
//! Same rules as the Go version's `redaction.go` / `redactURL`.

/// Replaces the credentials in a NATS URL (token / user:pass in the userinfo segment) with `***`.
/// Supports comma-separated multi-server URLs; a malformed entry that looks like it carries
/// credentials is masked entirely: better to show less than to leak.
pub fn redact_url(raw: &str) -> String {
    let parts: Vec<&str> = raw.split(',').collect();
    let multi = parts.len() > 1;
    let mut out = Vec::with_capacity(parts.len());
    for p in parts {
        let p = p.trim();
        match p.rfind('@') {
            None => {
                if multi && raw.contains('@') {
                    out.push("(redacted)".to_string()); // a malformed comma list may split credentials across entries; mask generously
                } else {
                    out.push(p.to_string());
                }
            }
            Some(at) => match p.find("://") {
                Some(scheme) if scheme + 3 <= at => {
                    out.push(format!("{}***{}", &p[..scheme + 3], &p[at..]));
                }
                _ => out.push("(redacted)".to_string()),
            },
        }
    }
    out.join(",")
}

/// Extracts every secret that must be masked from the raw URL (verbatim, URL-decoded, and the two
/// halves of a split `user:pass`).
fn secrets_of(raw_url: &str) -> Vec<String> {
    let mut secrets = Vec::new();
    for part in raw_url.split(',') {
        let part = part.trim();
        let Some(at) = part.rfind('@') else { continue };
        let start = match part.find("://") {
            Some(i) if i + 3 <= at => i + 3,
            _ => 0,
        };
        let auth = &part[start..at];
        let mut cands = vec![auth.to_string()];
        // Split before decoding, just like authentication: a percent-escaped
        // colon is part of a username or token, not the password separator.
        if let Some((user, password)) = auth.split_once(':') {
            cands.push(user.to_string());
            cands.push(password.to_string());
        }
        for candidate in cands.clone() {
            if let Some(decoded) = percent_decode(&candidate) {
                cands.push(decoded);
            }
        }
        // Errors may quote a credential independently of its URL. Cover the
        // runtime's escaped forms, including a second layer of Debug wrapping.
        for candidate in cands.clone() {
            for escaped in escaped_forms(&candidate) {
                cands.extend(escaped_forms(&escaped));
                cands.push(escaped);
            }
        }
        for c in cands {
            if !c.is_empty() && !secrets.contains(&c) {
                secrets.push(c);
            }
        }
    }
    // Mask the whole credential before any shorter username/password fragment.
    secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
    secrets
}

fn escaped_forms(value: &str) -> [String; 3] {
    let debug = format!("{value:?}");
    [
        debug[1..debug.len() - 1].to_owned(),
        value.escape_default().to_string(),
        value.escape_unicode().to_string(),
    ]
}

pub(crate) fn percent_decode(s: &str) -> Option<String> {
    if !s.contains('%') {
        return None;
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return None;
            }
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok()?;
            let v = u8::from_str_radix(hex, 16).ok()?;
            out.push(v);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Masks every URL and credential in an underlying error message. First the whole URL, then any
/// URL of the form `nats://` / `tls://` / `ws(s)://`, and finally the credentials on their own
/// (parse errors from an invalid port / percent sign echo the raw input back, so masking the outer
/// URL alone is not enough).
pub fn redact_error(raw_url: &str, message: &str) -> String {
    let mut m = message.replace(raw_url, &redact_url(raw_url));
    m = redact_urls_in_text(&m);
    for secret in secrets_of(raw_url) {
        m = m.replace(&secret, "***");
    }
    m
}

/// Masks the userinfo of any `scheme://...@host` URL in the text (hand-written scan, no regex
/// dependency).
fn redact_urls_in_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    loop {
        let Some(pos) = find_scheme(rest) else {
            out.push_str(rest);
            return out;
        };
        out.push_str(&rest[..pos]);
        let tail = &rest[pos..];
        let end = tail
            .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | '<' | '>'))
            .unwrap_or(tail.len());
        out.push_str(&redact_url(&tail[..end]));
        rest = &tail[end..];
    }
}

fn find_scheme(s: &str) -> Option<usize> {
    ["nats://", "tls://", "wss://", "ws://"]
        .iter()
        .filter_map(|p| s.find(p))
        .min()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_url_hides_token_and_userpass() {
        assert_eq!(
            redact_url("nats://tok3n@10.0.0.1:4222"),
            "nats://***@10.0.0.1:4222"
        );
        assert_eq!(
            redact_url("nats://u:p@h:4222,nats://q@h2:4222"),
            "nats://***@h:4222,nats://***@h2:4222"
        );
        assert_eq!(redact_url("nats://h:4222"), "nats://h:4222");
        assert_eq!(redact_url("tok3n@h:4222"), "(redacted)");
    }

    #[test]
    fn redact_error_hides_every_form() {
        let raw = "nats://tok%2Den:pw123@h:4222";
        let msg = format!(
            "dial {raw} failed: invalid port for tok-en / pw123 / tok%2Den / tok%2Den:pw123"
        );
        let r = redact_error(raw, &msg);
        for leak in ["tok-en", "pw123", "tok%2Den"] {
            assert!(!r.contains(leak), "{leak} leaked: {r}");
        }
        assert!(r.contains("nats://***@h:4222"), "{r}");
        // other URLs in the text are masked too
        assert_eq!(
            redact_error("nats://h", "see nats://abc@x:1 now"),
            "see nats://***@x:1 now"
        );
        assert_eq!(redact_error("nats://h", "plain"), "plain");
    }

    #[test]
    fn short_and_encoded_credentials_are_not_exempt_from_redaction() {
        for (url, text, secrets) in [
            ("nats://Q@localhost:4222", "token Q rejected", vec!["Q"]),
            ("nats://XY@localhost:4222", "token XY rejected", vec!["XY"]),
            (
                "nats://Q:XY@localhost:4222",
                "user Q password XY",
                vec!["Q", "XY"],
            ),
            (
                "nats://%51:%58%59@localhost:4222",
                "user Q password XY",
                vec!["Q", "XY"],
            ),
        ] {
            let redacted = redact_error(url, text);
            for secret in secrets {
                assert!(!redacted.contains(secret), "credential survived redaction");
            }
        }
    }

    #[test]
    fn standalone_quoted_credentials_are_redacted() {
        for (raw, decoded) in [
            ("token%22value", "token\"value"),
            ("token%5Cvalue", "token\\value"),
            ("token%0Avalue", "token\nvalue"),
            ("token%C3%A9value", "token\u{e9}value"),
        ] {
            let url = format!("nats://{raw}@localhost:4222");
            for secret in [raw, decoded] {
                for escaped in escaped_forms(secret) {
                    for form in std::iter::once(escaped.clone()).chain(escaped_forms(&escaped)) {
                        assert_eq!(
                            redact_error(&url, &format!("credential {form} rejected")),
                            "credential *** rejected",
                        );
                    }
                }
            }
        }
    }
}
