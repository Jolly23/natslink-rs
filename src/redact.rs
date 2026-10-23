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
        if let Some(decoded) = percent_decode(auth) {
            if decoded != auth {
                cands.push(decoded);
            }
        }
        for c in cands.clone() {
            if let Some((u, p)) = c.split_once(':') {
                cands.push(u.to_string());
                cands.push(p.to_string());
            }
        }
        // "Secrets" shorter than 3 characters are not masked: turning every "s" in the logs into
        // *** is worse than leaking a two-byte token.
        for c in cands {
            if c.len() >= 3 && !secrets.contains(&c) {
                secrets.push(c);
            }
        }
    }
    secrets
}

fn percent_decode(s: &str) -> Option<String> {
    if !s.contains('%') {
        return None;
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
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
}
