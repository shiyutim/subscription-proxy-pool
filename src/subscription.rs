//! Fetch and parse subscriptions without exposing their URLs in diagnostics.

use std::{collections::HashSet, fmt, net::Ipv6Addr};

use base64::{Engine, engine::general_purpose};
use reqwest::header::{ETAG, HeaderValue, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use url::Url;

use crate::{Error, ProxyNode, Result};

/// Default limit for both downloaded and decoded subscription documents (4 MiB).
pub const DEFAULT_MAX_SUBSCRIPTION_BYTES: usize = 4 * 1024 * 1024;

/// Resource limits for parsing an untrusted subscription document.
#[derive(Clone, Debug)]
pub struct ParseOptions {
    /// Maximum size of the input and decoded document, in bytes. Must be positive.
    pub max_bytes: usize,
}

impl Default for ParseOptions {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_MAX_SUBSCRIPTION_BYTES,
        }
    }
}

/// HTTP validators belonging to a successfully parsed subscription response.
///
/// Keep these with the corresponding nodes and only reuse them for the same
/// [`SubscriptionSource`]. Empty values and invalid HTTP header values are rejected
/// before sending a request. Servers determine whether validators still match.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SubscriptionValidators {
    /// The response's `ETag`, including quotes and an optional `W/` prefix.
    pub etag: Option<String>,
    /// The response's `Last-Modified` HTTP date.
    pub last_modified: Option<String>,
}

impl SubscriptionValidators {
    pub(crate) fn is_valid(&self) -> bool {
        [self.etag.as_deref(), self.last_modified.as_deref()]
            .into_iter()
            .flatten()
            .all(|value| validator_header(value).is_some())
    }
}

/// A conditional subscription request's result.
#[derive(Clone, Debug)]
pub enum SubscriptionUpdate {
    /// New content was received and successfully parsed.
    Modified {
        /// Nodes and skipped-entry count from the new content.
        report: ParseReport,
        /// Validators from this response, replacing any previous validators.
        validators: SubscriptionValidators,
    },
    /// HTTP 304: retain the nodes belonging to the supplied validators.
    NotModified,
}

/// Usable nodes, in subscription order, and the number of discarded entries.
#[derive(Clone, Debug)]
pub struct ParseReport {
    /// Valid nodes with duplicate endpoints removed. The first name wins.
    pub nodes: Vec<ProxyNode>,
    /// Unsupported, malformed, or duplicate entries. Blank lines and comments do not count.
    pub skipped: usize,
}

/// A subscription URL whose debug representation reveals only a stable hash.
#[derive(Clone)]
pub struct SubscriptionSource {
    url: Url,
}

impl SubscriptionSource {
    /// Accept an HTTP(S) URL. Query parameters and credentials remain private.
    pub fn new(value: &str) -> Result<Self> {
        let mut url = Url::parse(value).map_err(|_| Error::Config("invalid subscription URL"))?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none_or(str::is_empty) {
            return Err(Error::Config("subscription URL must use HTTP or HTTPS"));
        }
        // Fragments are never transmitted and must not create separate cache entries.
        url.set_fragment(None);
        Ok(Self { url })
    }

    /// SHA-256 of the normalized URL, suitable for isolating subscription caches.
    pub fn key(&self) -> String {
        format!("{:x}", Sha256::digest(self.url.as_str().as_bytes()))
    }

    /// Download and parse a subscription with a streaming response-size limit.
    ///
    /// `max_bytes` must be positive. The supplied client's timeout,
    /// redirect, and proxy policies apply. HTTP failure responses are rejected.
    pub async fn fetch(&self, client: &reqwest::Client, max_bytes: usize) -> Result<ParseReport> {
        match self.fetch_update(client, max_bytes, None).await? {
            SubscriptionUpdate::Modified { report, .. } => Ok(report),
            // fetch_update rejects unsolicited 304 responses. Keep the same
            // checked error here if its implementation changes in the future.
            SubscriptionUpdate::NotModified => Err(Error::Subscription(
                "received HTTP 304 without subscription validators",
            )),
        }
    }

    /// Download a subscription, optionally revalidating previously obtained nodes.
    ///
    /// Sends `If-None-Match` and/or `If-Modified-Since` from `validators`. A 304
    /// response succeeds only when at least one validator was actually sent; it
    /// has no body to parse. Callers must retain the matching nodes themselves.
    /// A modified response replaces the old validators, including clearing any
    /// that the server no longer sends. No automatic retries are performed.
    ///
    /// `max_bytes` is a positive limit on the downloaded and decoded document.
    /// The supplied client's timeout, redirect, and proxy policies apply.
    pub async fn fetch_update(
        &self,
        client: &reqwest::Client,
        max_bytes: usize,
        validators: Option<&SubscriptionValidators>,
    ) -> Result<SubscriptionUpdate> {
        if max_bytes == 0 {
            return Err(Error::Config("subscription size limit must be positive"));
        }
        let mut request = client.get(self.url.clone());
        let mut conditional = false;
        if let Some(validators) = validators {
            for (name, value) in [
                (IF_NONE_MATCH, validators.etag.as_deref()),
                (IF_MODIFIED_SINCE, validators.last_modified.as_deref()),
            ] {
                if let Some(value) = value {
                    let value = validator_header(value)
                        .ok_or(Error::Config("invalid subscription validator"))?;
                    request = request.header(name, value);
                    conditional = true;
                }
            }
        }
        let response = request
            .send()
            .await
            .map_err(|error| Error::Transport(error.without_url()))?;
        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            return if conditional {
                Ok(SubscriptionUpdate::NotModified)
            } else {
                Err(Error::Subscription(
                    "received HTTP 304 without subscription validators",
                ))
            };
        }
        let mut response = response
            .error_for_status()
            .map_err(|error| Error::Transport(error.without_url()))?;
        if response
            .content_length()
            .is_some_and(|size| size > max_bytes as u64)
        {
            return Err(Error::Subscription("subscription exceeds size limit"));
        }
        let validators = SubscriptionValidators {
            etag: response.headers().get(ETAG).and_then(response_validator),
            last_modified: response
                .headers()
                .get(LAST_MODIFIED)
                .and_then(response_validator),
        };
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| Error::Transport(error.without_url()))?
        {
            if chunk.len() > max_bytes.saturating_sub(body.len()) {
                return Err(Error::Subscription("subscription exceeds size limit"));
            }
            body.extend_from_slice(&chunk);
        }
        let content = std::str::from_utf8(&body)
            .map_err(|_| Error::Subscription("subscription is not UTF-8"))?;
        let report = parse_subscription_with_options(content, &ParseOptions { max_bytes })?;
        Ok(SubscriptionUpdate::Modified { report, validators })
    }
}

fn validator_header(value: &str) -> Option<HeaderValue> {
    if value.trim().is_empty() {
        return None;
    }
    HeaderValue::from_str(value).ok()
}

fn response_validator(value: &HeaderValue) -> Option<String> {
    let value = value.to_str().ok()?;
    validator_header(value).map(|_| value.to_owned())
}

impl fmt::Debug for SubscriptionSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SubscriptionSource")
            .field("key", &self.key())
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct ClashSubscription {
    proxies: Vec<Value>,
}

/// Parse Clash YAML or newline-separated HTTP(S)/SOCKS5(H) proxy URLs.
///
/// Either format may be wrapped once in standard or URL-safe Base64, with or
/// without padding or whitespace. Unsupported protocols, malformed entries,
/// and duplicates are skipped; an empty usable result is an error. No parsing
/// error includes subscription content or credentials.
pub fn parse_subscription(content: &str) -> Result<ParseReport> {
    parse_subscription_with_options(content, &ParseOptions::default())
}

/// Parse a subscription using an explicit input and decoded-document size limit.
///
/// This has the same format and error behavior as [`parse_subscription`], with
/// a caller-selected limit instead of the default 4 MiB. YAML also retains the
/// parser's structural limits on nesting and alias expansion.
pub fn parse_subscription_with_options(
    content: &str,
    options: &ParseOptions,
) -> Result<ParseReport> {
    if options.max_bytes == 0 {
        return Err(Error::Config("subscription size limit must be positive"));
    }
    if content.len() > options.max_bytes {
        return Err(Error::Subscription("subscription exceeds size limit"));
    }
    let content = content.trim().trim_start_matches('\u{feff}').trim();
    if content.is_empty() {
        return Err(Error::Subscription("subscription is empty"));
    }
    let mut parse_error = match parse_plain(content) {
        Ok(report) => return Ok(report),
        Err(error) => error,
    };
    let encoded: String = content
        .chars()
        .filter(|value| !value.is_whitespace())
        .collect();
    for engine in [
        &general_purpose::STANDARD,
        &general_purpose::STANDARD_NO_PAD,
        &general_purpose::URL_SAFE,
        &general_purpose::URL_SAFE_NO_PAD,
    ] {
        let Ok(decoded) = engine.decode(&encoded) else {
            continue;
        };
        // Base64 cannot expand beyond its input size, but keep the decoded
        // document's invariant explicit if decoding changes in the future.
        if decoded.len() > options.max_bytes {
            return Err(Error::Subscription("subscription exceeds size limit"));
        }
        let Ok(decoded) = std::str::from_utf8(&decoded) else {
            continue;
        };
        match parse_plain(decoded.trim().trim_start_matches('\u{feff}').trim()) {
            Ok(report) => return Ok(report),
            Err(error) => parse_error = error,
        }
    }
    Err(parse_error)
}

fn parse_plain(content: &str) -> Result<ParseReport> {
    // An initial URI identifies a line list, even if a later malformed entry
    // resembles a YAML key. Decide this before looking for Clash diagnostics,
    // so one bad line cannot discard the list's usable nodes.
    let starts_with_uri = content
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .is_some_and(|line| Url::parse(line).is_ok_and(|url| url.has_host()));
    if starts_with_uri {
        return collect_uri_nodes(content);
    }
    if let Ok(clash) = serde_saphyr::from_str::<ClashSubscription>(content) {
        return collect_nodes(clash.proxies.iter().map(clash_node));
    }
    // A recognizable Clash document with an invalid structure is a parsing
    // failure, rather than a list whose nodes happen to use unsupported protocols.
    // Retain only a static diagnostic: YAML parser errors can contain credentials.
    if content.lines().any(|line| {
        line.trim_start()
            .strip_prefix("proxies")
            .and_then(|rest| rest.trim_start().strip_prefix(':'))
            .is_some_and(|rest| {
                rest.is_empty()
                    || rest.starts_with(char::is_whitespace)
                    || rest.starts_with(['[', '{'])
            })
    }) {
        return Err(Error::Subscription("invalid Clash subscription document"));
    }
    collect_uri_nodes(content)
}

fn collect_uri_nodes(content: &str) -> Result<ParseReport> {
    collect_nodes(
        content
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(|line| ProxyNode::from_url(line).ok()),
    )
}

fn collect_nodes(nodes: impl Iterator<Item = Option<ProxyNode>>) -> Result<ParseReport> {
    let mut report = ParseReport {
        nodes: Vec::new(),
        skipped: 0,
    };
    let mut seen = HashSet::new();
    for node in nodes {
        match node {
            Some(node) if seen.insert(node.id()) => report.nodes.push(node),
            _ => report.skipped += 1,
        }
    }
    if report.nodes.is_empty() {
        return Err(Error::Subscription(
            "subscription contains no supported valid proxy nodes",
        ));
    }
    Ok(report)
}

fn clash_node(value: &Value) -> Option<ProxyNode> {
    let value = value.as_object()?;
    let protocol = value.get("type")?.as_str()?;
    let tls = match value.get("tls") {
        Some(value) => value.as_bool()?,
        None => false,
    };
    let scheme = match (protocol, tls) {
        ("http", false) => "http",
        ("http", true) | ("https", _) => "https",
        ("socks5", false) => "socks5",
        ("socks5h", false) => "socks5h",
        // TLS-wrapped SOCKS is not plain SOCKS and cannot be represented safely.
        _ => return None,
    };
    let server = value.get("server")?.as_str()?;
    if server.is_empty() {
        return None;
    }
    let port = match value.get("port")? {
        Value::Number(number) => u16::try_from(number.as_u64()?).ok()?,
        Value::String(port) => port.parse::<u16>().ok()?,
        _ => return None,
    };
    if port == 0 {
        return None;
    }
    let mut url = Url::parse(&format!("{scheme}://localhost")).ok()?;
    let host = if server.parse::<Ipv6Addr>().is_ok() {
        format!("[{server}]")
    } else {
        server.to_owned()
    };
    // Unlike Host::parse, set_host accepts a trailing ':port' and truncates it.
    // Validate the whole server field before installing it into the URL.
    let host = url::Host::parse(&host).ok()?;
    url.set_host(Some(&host.to_string())).ok()?;
    url.set_port(Some(port)).ok()?;
    if let Some(username) = value.get("username") {
        // URL setters preserve existing percent escapes. Clash credentials are
        // literal strings, so escape '%' first to prevent accidental decoding.
        url.set_username(&username.as_str()?.replace('%', "%25"))
            .ok()?;
    }
    if let Some(password) = value.get("password") {
        url.set_password(Some(&password.as_str()?.replace('%', "%25")))
            .ok()?;
    }
    let mut node = ProxyNode::from_url(url.as_str()).ok()?;
    if let Some(name) = value.get("name") {
        node = node.with_name(name.as_str()?);
    }
    Some(node)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::ProxyKind;

    #[test]
    fn clash_preserves_protocol_auth_and_ipv6_and_skips_bad_entries() {
        let report = parse_subscription(r#"
proxies:
  - {name: secure, type: http, server: proxy.test, port: 8443, tls: true, username: 'a@b', password: 'p:/?#@'}
  - {name: ipv6, type: socks5h, server: '::1', port: '1080'}
  - {type: ss, server: proxy.test, port: 8388, password: hidden}
  - {type: socks5, server: proxy.test, port: 1080, tls: true}
  - {type: http, server: proxy.test, port: -1}
  - {type: http, server: proxy.test}
  - invalid
"#).unwrap();
        assert_eq!(report.nodes.len(), 2);
        assert_eq!(report.skipped, 5);
        assert_eq!(report.nodes[0].kind(), ProxyKind::Https);
        assert_eq!(report.nodes[0].name(), "secure");
        let url = Url::parse(report.nodes[0].url()).unwrap();
        assert_eq!(url.username(), "a%40b");
        assert_eq!(url.password(), Some("p%3A%2F%3F%23%40"));
        assert_eq!(report.nodes[1].url(), "socks5h://[::1]:1080");
    }

    #[test]
    fn uri_lists_deduplicate_in_order_and_ignore_comments() {
        let report = parse_subscription("\u{feff}# comment\nhttp://host:80#first\nss://unsupported\n\nhttps://host:443\nhttp://host#second\n").unwrap();
        assert_eq!(report.nodes.len(), 2);
        assert_eq!(report.nodes[0].name(), "first");
        assert_eq!(report.nodes[1].kind(), ProxyKind::Https);
        assert_eq!(report.skipped, 2);
    }

    #[test]
    fn literal_percent_credentials_are_preserved_and_invalid_hosts_are_skipped() {
        let report = parse_subscription(
            r#"
proxies:
  - {type: http, server: host, port: 8080, username: 'user%40literal', password: 'pass%2Fword'}
  - {type: http, server: 'host:9999', port: 8080}
  - {type: http, server: 'host/path', port: 8080}
"#,
        )
        .unwrap();
        assert_eq!(report.nodes.len(), 1);
        assert_eq!(report.skipped, 2);
        let url = Url::parse(report.nodes[0].url()).unwrap();
        assert_eq!(url.username(), "user%2540literal");
        assert_eq!(url.password(), Some("pass%252Fword"));
    }

    #[test]
    fn accepts_base64_engines_padding_and_whitespace() {
        for content in [
            "http://user:secret@host:8080#节点??\nsocks5://host:1080",
            "proxies:\n  - {type: http, server: host, port: 8080}",
        ] {
            for engine in [
                &general_purpose::STANDARD,
                &general_purpose::STANDARD_NO_PAD,
                &general_purpose::URL_SAFE,
                &general_purpose::URL_SAFE_NO_PAD,
            ] {
                let encoded = engine.encode(content);
                let spaced = encoded
                    .chars()
                    .enumerate()
                    .fold(String::new(), |mut out, (i, ch)| {
                        if i % 13 == 0 {
                            out.push_str(" \n\t");
                        }
                        out.push(ch);
                        out
                    });
                assert_eq!(
                    parse_subscription(&spaced).unwrap().nodes.len(),
                    parse_subscription(content).unwrap().nodes.len()
                );
            }
        }
    }

    #[test]
    fn parsing_errors_and_debug_do_not_expose_secrets() {
        for content in [
            "",
            "proxies: []",
            "ss://private:secret@host",
            "proxies: [secret",
        ] {
            let error = parse_subscription(content).unwrap_err();
            assert!(!format!("{error:?} {error}").contains("secret"));
        }
        assert!(parse_subscription(&" ".repeat(DEFAULT_MAX_SUBSCRIPTION_BYTES + 1)).is_err());
        let source =
            SubscriptionSource::new("https://private:secret@host/private-path?token=hidden")
                .unwrap();
        let debug = format!("{source:?}");
        for secret in ["private", "secret", "hidden", "host"] {
            assert!(!debug.contains(secret));
        }
        assert_eq!(source.key().len(), 64);
        assert_ne!(
            source.key(),
            SubscriptionSource::new("https://host/other").unwrap().key()
        );
        assert!(SubscriptionSource::new("file:///secret").is_err());
        assert!(SubscriptionSource::new("invalid-secret").is_err());
    }

    async fn serve(response: &'static str) -> (SubscriptionSource, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let _ = socket.read(&mut request).await.unwrap();
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        (
            SubscriptionSource::new(&format!("http://{address}/private?token=secret")).unwrap(),
            server,
        )
    }

    fn client() -> reqwest::Client {
        reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn fetch_checks_status_and_content_length_without_leaking_url() {
        let (source, server) =
            serve("HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await;
        let error = source.fetch(&client(), 1024).await.unwrap_err();
        assert!(!format!("{error} {error:?}").contains("secret"));
        assert!(!format!("{error} {error:?}").contains("private"));
        server.await.unwrap();

        let (source, server) =
            serve("HTTP/1.1 200 OK\r\nContent-Length: 1000\r\nConnection: close\r\n\r\n").await;
        assert!(matches!(
            source.fetch(&client(), 20).await,
            Err(Error::Subscription("subscription exceeds size limit"))
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_checks_chunked_response_size_and_parses_success() {
        let (source, server) = serve("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n11\r\nhttp://host:8080\n\r\n0\r\n\r\n").await;
        assert!(matches!(
            source.fetch(&client(), 8).await,
            Err(Error::Subscription("subscription exceeds size limit"))
        ));
        server.await.unwrap();

        let (source, server) = serve(
            "HTTP/1.1 200 OK\r\nContent-Length: 17\r\nConnection: close\r\n\r\nhttp://host:8080\n",
        )
        .await;
        assert_eq!(source.fetch(&client(), 17).await.unwrap().nodes.len(), 1);
        server.await.unwrap();
    }
}
