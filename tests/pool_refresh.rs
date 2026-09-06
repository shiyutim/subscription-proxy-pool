use std::{
    num::NonZeroU64,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use subscription_proxy_pool::{
    CachePolicy, HealthPolicy, PoolBuilder, ProxyPool, RotationPolicy, SubscriptionSource,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinHandle,
};

const FIRST: &str = "http://127.0.0.1:18001#first\nhttp://127.0.0.1:18002#second";

/// A loopback-only subscription service. Every response closes its connection
/// so hit counts correspond exactly to remote subscription fetches.
struct Server {
    source: SubscriptionSource,
    response: Arc<Mutex<(u16, String)>>,
    hits: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl Server {
    async fn new(body: &str) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let response = Arc::new(Mutex::new((200, body.to_owned())));
        let hits = Arc::new(AtomicUsize::new(0));
        let task = {
            let response = Arc::clone(&response);
            let hits = Arc::clone(&hits);
            tokio::spawn(async move {
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    let mut buffer = [0; 1024];
                    loop {
                        let count = socket.read(&mut buffer).await.unwrap();
                        if count == 0 {
                            break;
                        }
                        request.extend_from_slice(&buffer[..count]);
                        if request.windows(4).any(|part| part == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let (status, body) = response.lock().unwrap().clone();
                    hits.fetch_add(1, Ordering::SeqCst);
                    let wire = format!(
                        "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    // A caller may cancel a fetch while the test tears down.
                    let _ = socket.write_all(wire.as_bytes()).await;
                }
            })
        };
        Self {
            source: SubscriptionSource::new(&format!(
                "http://{address}/subscription?token=private"
            ))
            .unwrap(),
            response,
            hits,
            task,
        }
    }

    fn respond(&self, status: u16, body: &str) {
        *self.response.lock().unwrap() = (status, body.to_owned());
    }

    fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn builder(source: &SubscriptionSource) -> PoolBuilder {
    ProxyPool::builder()
        .subscription(source.clone())
        .health(HealthPolicy {
            check_on_build: false,
            // No test uses an external health target, even if accidentally probed.
            check_url: Some("http://127.0.0.1:1/health".into()),
            ..HealthPolicy::default()
        })
        .subscription_timeout(Duration::from_secs(3))
}

fn names(pool: &ProxyPool) -> Vec<String> {
    pool.nodes()
        .iter()
        .map(|node| node.name().to_owned())
        .collect()
}

fn acquired_name(pool: &ProxyPool) -> String {
    pool.acquire().unwrap().node().name().to_owned()
}

#[tokio::test]
async fn first_fetch_is_cached_and_explicit_refresh_bypasses_a_fresh_cache() {
    let server = Server::new(FIRST).await;
    let directory = tempfile::tempdir().unwrap();
    let cache = CachePolicy::new(directory.path());
    let first = builder(&server.source)
        .cache(cache.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(server.hits(), 1);
    assert_eq!(names(&first), ["first", "second"]);

    server.respond(200, "http://127.0.0.1:18003#new");
    let restored = builder(&server.source)
        .cache(cache.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(server.hits(), 1, "fresh startup cache must avoid a fetch");
    assert_eq!(names(&restored), ["first", "second"]);

    let report = restored.refresh().await.unwrap();
    assert_eq!(server.hits(), 2);
    assert_eq!(report.updated_sources, 1);
    assert_eq!(report.cached_sources, 0);
    assert_eq!(report.cache_write_failures, 0);
    assert_eq!(names(&restored), ["new"]);
    assert_eq!(names(&first), ["first", "second"]);

    // A subsequent process observes the new successfully persisted list.
    let restarted = builder(&server.source).cache(cache).build().await.unwrap();
    assert_eq!(server.hits(), 2);
    assert_eq!(names(&restarted), ["new"]);
}

#[tokio::test]
async fn invalid_empty_unsupported_and_http_failure_updates_retain_working_nodes() {
    let server = Server::new(FIRST).await;
    let pool = builder(&server.source).build().await.unwrap();
    let original_ids: Vec<_> = pool.nodes().iter().map(|node| node.id()).collect();
    for (status, body) in [
        (200, "proxies: [broken"),
        (200, "proxies: []"),
        (200, "ss://unsupported-secret@host:8080"),
        (503, "service unavailable"),
    ] {
        server.respond(status, body);
        let report = pool.refresh().await.unwrap();
        assert_eq!(report.nodes, 2);
        assert_eq!(report.failed_sources, 1);
        assert_eq!(report.updated_sources, 0);
        assert_eq!(
            pool.nodes()
                .iter()
                .map(|node| node.id())
                .collect::<Vec<_>>(),
            original_ids
        );
        assert_eq!(pool.stats().eligible, 2);
    }
    assert_eq!(server.hits(), 5);
}

#[tokio::test]
async fn identical_endpoints_keep_their_remaining_quota_when_names_and_order_change() {
    let server = Server::new(FIRST).await;
    let pool = builder(&server.source)
        .rotation(RotationPolicy {
            requests_per_proxy: NonZeroU64::new(3),
            ..RotationPolicy::default()
        })
        .build()
        .await
        .unwrap();
    let cloned = pool.clone();
    assert_eq!(acquired_name(&pool), "first");
    assert_eq!(acquired_name(&cloned), "first");

    server.respond(
        200,
        "http://127.0.0.1:18002#renamed-second\nhttp://127.0.0.1:18001#renamed-first",
    );
    assert_eq!(cloned.refresh().await.unwrap().updated_sources, 1);
    assert_eq!(acquired_name(&cloned), "renamed-first");
    assert_eq!(acquired_name(&pool), "renamed-second");
}

#[tokio::test]
async fn sources_and_nodes_are_deduplicated_and_partial_failure_retains_its_source() {
    let first = Server::new(FIRST).await;
    let second =
        Server::new("http://127.0.0.1:18002#duplicate-second\nhttp://127.0.0.1:18003#third").await;
    let pool = builder(&first.source)
        .subscription(first.source.clone())
        .subscription(second.source.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(first.hits(), 1, "a repeated source is fetched once");
    assert_eq!(second.hits(), 1);
    assert_eq!(names(&pool), ["first", "second", "third"]);

    first.respond(200, "http://127.0.0.1:18004#fourth");
    second.respond(500, "broken");
    let report = pool.refresh().await.unwrap();
    assert_eq!(report.updated_sources, 1);
    assert_eq!(report.failed_sources, 1);
    assert_eq!(report.nodes, 3);
    assert_eq!(names(&pool), ["fourth", "duplicate-second", "third"]);
    assert_eq!(first.hits(), 2);
    assert_eq!(second.hits(), 2);
}

#[tokio::test]
async fn maintenance_refresh_shares_live_state_without_consuming_or_resetting_quota() {
    let server = Server::new(FIRST).await;
    let pool = builder(&server.source)
        .rotation(RotationPolicy {
            requests_per_proxy: NonZeroU64::new(3),
            ..RotationPolicy::default()
        })
        .refresh_interval(Duration::from_millis(30))
        .health_interval(Duration::from_secs(3600))
        .build()
        .await
        .unwrap();
    let business = pool.clone();
    assert_eq!(acquired_name(&business), "first");
    server.respond(
        200,
        "http://127.0.0.1:18001#updated-first\nhttp://127.0.0.1:18002#second",
    );
    let maintenance = pool.spawn_maintenance();
    tokio::time::timeout(Duration::from_secs(3), async {
        while names(&business)[0] != "updated-first" {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("maintenance should update the live pool");
    assert!(server.hits() >= 2);
    assert_eq!(acquired_name(&pool), "updated-first");
    assert_eq!(acquired_name(&business), "updated-first");
    assert_eq!(acquired_name(&business), "second");

    maintenance.shutdown().await;
    let hits = server.hits();
    tokio::time::sleep(Duration::from_millis(70)).await;
    assert_eq!(server.hits(), hits, "shutdown cancels further refreshes");
}
