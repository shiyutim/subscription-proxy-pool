use std::time::{Duration, SystemTime};

use base64::{Engine, engine::general_purpose::STANDARD};
use subscription_proxy_pool::{
    CachePolicy, DEFAULT_MAX_SUBSCRIPTION_BYTES, Error, ParseOptions, ProxyNode, ProxyPool,
    SourceOutcome, SubscriptionSource, SubscriptionUpdate, SubscriptionValidators,
    parse_subscription, parse_subscription_with_options,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}

async fn serve(responses: Vec<String>) -> (SubscriptionSource, JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for response in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let mut buffer = [0; 1024];
                let read = socket.read(&mut buffer).await.unwrap();
                assert_ne!(read, 0, "request ended before its headers");
                request.extend_from_slice(&buffer[..read]);
                assert!(request.len() < 16 * 1024);
            }
            requests.push(String::from_utf8(request).unwrap().to_ascii_lowercase());
            // A size-limited client can close before the whole response is written.
            let _ = socket.write_all(response.as_bytes()).await;
        }
        requests
    });
    (
        SubscriptionSource::new(&format!("http://{address}/private?token=secret")).unwrap(),
        server,
    )
}

fn modified(body: &str, headers: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n{body}",
        body.len()
    )
}

fn not_modified() -> String {
    "HTTP/1.1 304 Not Modified\r\nConnection: close\r\n\r\n".to_owned()
}

#[tokio::test]
async fn validators_round_trip_without_parsing_a_304_body() {
    let (source, server) = serve(vec![
        modified(
            "http://host:8080\n",
            "ETag: W/\"version-one\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\n",
        ),
        not_modified(),
    ])
    .await;
    let client = client();
    let SubscriptionUpdate::Modified { report, validators } =
        source.fetch_update(&client, 1024, None).await.unwrap()
    else {
        panic!("first response must contain nodes");
    };
    assert_eq!(report.nodes.len(), 1);
    assert_eq!(validators.etag.as_deref(), Some("W/\"version-one\""));
    assert_eq!(
        validators.last_modified.as_deref(),
        Some("Wed, 21 Oct 2015 07:28:00 GMT")
    );
    assert!(matches!(
        source
            .fetch_update(&client, 1024, Some(&validators))
            .await
            .unwrap(),
        SubscriptionUpdate::NotModified
    ));
    let requests = server.await.unwrap();
    assert!(!requests[0].contains("if-none-match:"));
    assert!(!requests[0].contains("if-modified-since:"));
    assert!(requests[1].contains("if-none-match: w/\"version-one\"\r\n"));
    assert!(requests[1].contains("if-modified-since: wed, 21 oct 2015 07:28:00 gmt\r\n"));
}

#[tokio::test]
async fn either_validator_can_revalidate_independently() {
    for validators in [
        SubscriptionValidators {
            etag: Some("\"version\"".to_owned()),
            last_modified: None,
        },
        SubscriptionValidators {
            etag: None,
            last_modified: Some("Wed, 21 Oct 2015 07:28:00 GMT".to_owned()),
        },
    ] {
        let (source, server) = serve(vec![not_modified()]).await;
        assert!(matches!(
            source
                .fetch_update(&client(), 1024, Some(&validators))
                .await
                .unwrap(),
            SubscriptionUpdate::NotModified
        ));
        let requests = server.await.unwrap();
        assert_eq!(
            requests[0].contains("if-none-match:"),
            validators.etag.is_some()
        );
        assert_eq!(
            requests[0].contains("if-modified-since:"),
            validators.last_modified.is_some()
        );
    }
}

#[tokio::test]
async fn unsolicited_304_is_an_error_even_with_an_empty_validator_object() {
    for validators in [None, Some(SubscriptionValidators::default())] {
        let (source, server) = serve(vec![not_modified()]).await;
        let error = source
            .fetch_update(&client(), 1024, validators.as_ref())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::Subscription("received HTTP 304 without subscription validators")
        ));
        assert!(!format!("{error:?} {error}").contains("secret"));
        server.await.unwrap();
    }
    let (source, server) = serve(vec![not_modified()]).await;
    assert!(source.fetch(&client(), 1024).await.is_err());
    server.await.unwrap();
}

#[tokio::test]
async fn malformed_validator_values_are_rejected_before_network_io() {
    let source = SubscriptionSource::new("http://127.0.0.1:9/?token=secret").unwrap();
    let client = client();
    for value in ["", " \t", "secret\r\nX-Injected: yes", "secret\0"] {
        for validators in [
            SubscriptionValidators {
                etag: Some(value.to_owned()),
                last_modified: None,
            },
            SubscriptionValidators {
                etag: None,
                last_modified: Some(value.to_owned()),
            },
        ] {
            let error = source
                .fetch_update(&client, 1024, Some(&validators))
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                Error::Config("invalid subscription validator")
            ));
            assert!(!format!("{error:?} {error}").contains("secret"));
        }
    }
}

#[tokio::test]
async fn invalid_cached_validators_are_misses_and_do_not_poison_later_refreshes() {
    for stale in [false, true] {
        for (field, invalid) in [
            ("etag", ""),
            ("etag", "secret\r\nX-Injected: yes"),
            ("last_modified", " \t"),
            ("last_modified", "secret\0"),
        ] {
            let (source, server) = serve(vec![
                modified("http://host:9090\n", "ETag: \"recovered\"\r\n"),
                not_modified(),
            ])
            .await;
            let directory = tempfile::tempdir().unwrap();
            let policy = CachePolicy::new(directory.path());
            let saved_at = if stale {
                SystemTime::now() - policy.ttl - Duration::from_secs(1)
            } else {
                SystemTime::now()
            };
            let mut validators = serde_json::json!({
                "etag": "\"old\"",
                "last_modified": "Wed, 21 Oct 2015 07:28:00 GMT"
            });
            validators[field] = invalid.into();
            let cached = serde_json::json!({
                "schema": 1,
                "source_key": source.key(),
                "saved_at": saved_at,
                "nodes": [ProxyNode::from_url("http://host:8080").unwrap()],
                "validators": validators
            });
            std::fs::write(
                directory.path().join(format!("{}.json", source.key())),
                serde_json::to_vec(&cached).unwrap(),
            )
            .unwrap();

            let pool = ProxyPool::builder()
                .subscription(source)
                .cache(policy)
                .build()
                .await
                .unwrap();
            let startup = pool.last_refresh_report().unwrap();
            assert_eq!(startup.sources[0].outcome, SourceOutcome::Updated);
            assert_eq!(startup.failed_sources, 0);
            assert_eq!(pool.acquire().unwrap().node().url(), "http://host:9090/");

            let refresh = pool.refresh().await.unwrap();
            assert_eq!(refresh.sources[0].outcome, SourceOutcome::NotModified);
            assert_eq!(refresh.failed_sources, 0);
            let requests = server.await.unwrap();
            assert!(!requests[0].contains("if-none-match:"));
            assert!(!requests[0].contains("if-modified-since:"));
            assert!(requests[1].contains("if-none-match: \"recovered\"\r\n"));
            assert!(!requests[1].contains("if-modified-since:"));
        }
    }
}

#[tokio::test]
async fn modified_response_replaces_validators_instead_of_retaining_old_values() {
    let (source, server) = serve(vec![modified(
        "http://host:8080\n",
        "ETag: \"new\"\r\nLast-Modified: \r\n",
    )])
    .await;
    let previous = SubscriptionValidators {
        etag: Some("\"old\"".to_owned()),
        last_modified: Some("Wed, 21 Oct 2015 07:28:00 GMT".to_owned()),
    };
    let SubscriptionUpdate::Modified { validators, .. } = source
        .fetch_update(&client(), 1024, Some(&previous))
        .await
        .unwrap()
    else {
        panic!("expected updated nodes");
    };
    assert_eq!(validators.etag.as_deref(), Some("\"new\""));
    assert!(validators.last_modified.is_none());
    server.await.unwrap();
}

#[tokio::test]
async fn explicit_limit_can_exceed_the_default_in_fetch_and_parse() {
    let body = format!(
        "http://host:8080\n#{}",
        "padding".repeat(DEFAULT_MAX_SUBSCRIPTION_BYTES / 7 + 1)
    );
    assert!(body.len() > DEFAULT_MAX_SUBSCRIPTION_BYTES);
    assert!(matches!(
        parse_subscription(&body),
        Err(Error::Subscription("subscription exceeds size limit"))
    ));
    let options = ParseOptions {
        max_bytes: body.len(),
    };
    assert_eq!(
        parse_subscription_with_options(&body, &options)
            .unwrap()
            .nodes
            .len(),
        1
    );
    let (source, server) = serve(vec![modified(&body, ""), modified(&body, "")]).await;
    let client = client();
    assert_eq!(
        source.fetch(&client, body.len()).await.unwrap().nodes.len(),
        1
    );
    assert!(matches!(
        source
            .fetch_update(&client, body.len(), None)
            .await
            .unwrap(),
        SubscriptionUpdate::Modified { .. }
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn conditional_requests_still_enforce_declared_and_streamed_size_limits() {
    let validators = SubscriptionValidators {
        etag: Some("\"old\"".to_owned()),
        last_modified: None,
    };
    let (source, server) = serve(vec![
        modified("http://host:8080\n", ""),
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n11\r\nhttp://host:8080\n\r\n0\r\n\r\n".to_owned(),
    ])
    .await;
    let client = client();
    for _ in 0..2 {
        assert!(matches!(
            source.fetch_update(&client, 8, Some(&validators)).await,
            Err(Error::Subscription("subscription exceeds size limit"))
        ));
    }
    server.await.unwrap();
}

#[tokio::test]
async fn zero_limits_are_configuration_errors_for_every_entry_point() {
    let source = SubscriptionSource::new("http://127.0.0.1:9/").unwrap();
    assert!(matches!(
        source.fetch_update(&client(), 0, None).await,
        Err(Error::Config("subscription size limit must be positive"))
    ));
    assert!(matches!(
        source.fetch(&client(), 0).await,
        Err(Error::Config("subscription size limit must be positive"))
    ));
    assert!(matches!(
        parse_subscription_with_options("http://host:8080", &ParseOptions { max_bytes: 0 }),
        Err(Error::Config("subscription size limit must be positive"))
    ));
}

#[test]
fn malformed_clash_is_distinguished_from_a_valid_but_unsupported_subscription() {
    for document in [
        "proxies: [credential-secret",
        "proxies: {type: http, password: credential-secret}",
    ] {
        for content in [document.to_owned(), STANDARD.encode(document)] {
            let error = parse_subscription(&content).unwrap_err();
            assert!(matches!(
                error,
                Error::Subscription("invalid Clash subscription document")
            ));
            assert!(!format!("{error:?} {error}").contains("credential-secret"));
        }
    }
    assert!(matches!(
        parse_subscription("proxies:\n  - {type: ss, server: host, port: 8080}"),
        Err(Error::Subscription(
            "subscription contains no supported valid proxy nodes"
        ))
    ));
}

#[test]
fn uri_lists_skip_malformed_clash_like_lines_without_discarding_valid_nodes() {
    for document in [
        "http://host:8080\nproxies: [credential-secret\nsocks5h://host:1080",
        "# comment\n\nhttp://host:8080\nproxies: garbage\nsocks5h://host:1080",
        "ss://unsupported\nhttp://host:8080\nproxies: garbage\nsocks5h://host:1080",
    ] {
        for content in [document.to_owned(), STANDARD.encode(document)] {
            let report = parse_subscription(&content).unwrap();
            assert_eq!(report.nodes.len(), 2);
            assert_eq!(report.nodes[0].url(), "http://host:8080/");
            assert_eq!(report.nodes[1].url(), "socks5h://host:1080");
            assert_eq!(
                report.skipped,
                1 + usize::from(document.starts_with("ss://"))
            );
        }
    }
}

#[test]
fn malformed_clash_does_not_extract_embedded_uri_lines_as_a_partial_subscription() {
    for document in [
        "proxies: [credential-secret\nhttp://host:8080",
        "# comment\nproxies: [credential-secret\nhttp://host:8080",
        "mixed-port: 7890\nproxies: [credential-secret\nhttp://host:8080",
    ] {
        for content in [document.to_owned(), STANDARD.encode(document)] {
            let error = parse_subscription(&content).unwrap_err();
            assert!(matches!(
                error,
                Error::Subscription("invalid Clash subscription document")
            ));
            assert!(!format!("{error:?} {error}").contains("credential-secret"));
        }
    }
}
