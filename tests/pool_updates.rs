use std::{
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use futures_util::future::join_all;
use subscription_proxy_pool::{
    CachePolicy, HealthPolicy, PoolBuilder, ProxyPool, RotationPolicy, SourceOutcome,
    SubscriptionSource,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::{JoinHandle, JoinSet},
};

const FIRST: &str = "http://127.0.0.1:18001#first\nhttp://127.0.0.1:18002#second";

async fn read_request(socket: &mut TcpStream) -> Option<String> {
    let mut request = Vec::new();
    let mut buffer = [0; 1024];
    loop {
        let count = socket.read(&mut buffer).await.ok()?;
        if count == 0 {
            return None;
        }
        request.extend_from_slice(&buffer[..count]);
        if request.windows(4).any(|part| part == b"\r\n\r\n") {
            return Some(String::from_utf8(request).unwrap().to_ascii_lowercase());
        }
        assert!(request.len() < 16 * 1024);
    }
}

struct Server {
    source: SubscriptionSource,
    response: Arc<Mutex<String>>,
    gate: Arc<Mutex<Option<Arc<Semaphore>>>>,
    hits: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<()>,
}

impl Server {
    async fn new(body: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let response = Arc::new(Mutex::new(Self::wire(200, body, "")));
        let gate: Arc<Mutex<Option<Arc<Semaphore>>>> = Arc::new(Mutex::new(None));
        let hits = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let task = {
            let response = Arc::clone(&response);
            let gate = Arc::clone(&gate);
            let hits = Arc::clone(&hits);
            let requests = Arc::clone(&requests);
            tokio::spawn(async move {
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let Some(request) = read_request(&mut socket).await else {
                        continue;
                    };
                    requests.lock().unwrap().push(request);
                    hits.fetch_add(1, Ordering::SeqCst);
                    let waiting = gate.lock().unwrap().clone();
                    if let Some(waiting) = waiting {
                        waiting.acquire().await.unwrap().forget();
                    }
                    let wire = response.lock().unwrap().clone();
                    let _ = socket.write_all(wire.as_bytes()).await;
                }
            })
        };
        Self {
            source: SubscriptionSource::new(&format!(
                "http://{address}/private-path?token=credential-secret"
            ))
            .unwrap(),
            response,
            gate,
            hits,
            requests,
            task,
        }
    }

    fn wire(status: u16, body: &str, headers: &str) -> String {
        format!(
            "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n{body}",
            body.len()
        )
    }

    fn respond(&self, status: u16, body: &str, headers: &str) {
        *self.response.lock().unwrap() = Self::wire(status, body, headers);
    }

    fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }

    async fn wait_for_hits(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.hits() < count {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("local subscription request did not arrive");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct ProxyServer {
    endpoint: String,
    connections: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl ProxyServer {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let connections = Arc::new(AtomicUsize::new(0));
        let task = {
            let connections = Arc::clone(&connections);
            tokio::spawn(async move {
                let mut handlers = JoinSet::new();
                loop {
                    tokio::select! {
                        accepted = listener.accept() => {
                            let (mut socket, _) = accepted.unwrap();
                            connections.fetch_add(1, Ordering::SeqCst);
                            handlers.spawn(async move {
                                while read_request(&mut socket).await.is_some() {
                                    if socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok").await.is_err() {
                                        break;
                                    }
                                }
                            });
                        }
                        _ = handlers.join_next(), if !handlers.is_empty() => {}
                    }
                }
            })
        };
        Self {
            endpoint: format!("http://{address}"),
            connections,
            task,
        }
    }
}

impl Drop for ProxyServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn builder(source: &SubscriptionSource) -> PoolBuilder {
    ProxyPool::builder()
        .subscription(source.clone())
        .health(HealthPolicy::default())
        .request_timeout(Duration::from_secs(3))
        .subscription_timeout(Duration::from_secs(3))
}

fn acquired_name(pool: &ProxyPool) -> String {
    pool.acquire().unwrap().node().name().to_owned()
}

#[tokio::test]
async fn not_modified_preserves_clients_quota_and_renews_the_persistent_cache() {
    let proxy = ProxyServer::new().await;
    let body = format!("{}#first\nhttp://127.0.0.1:1#second", proxy.endpoint);
    let server = Server::new(&body).await;
    server.respond(200, &body, "ETag: \"one\"\r\n");
    let directory = tempfile::tempdir().unwrap();
    let cache = CachePolicy {
        ttl: Duration::from_secs(30),
        max_stale: Duration::from_secs(600),
        ..CachePolicy::new(directory.path())
    };
    let pool = builder(&server.source)
        .cache(cache.clone())
        .rotation(RotationPolicy {
            requests_per_proxy: NonZeroU64::new(3),
            ..RotationPolicy::default()
        })
        .build()
        .await
        .unwrap();
    let ids: Vec<_> = pool.nodes().iter().map(|node| node.id()).collect();
    let mut first = pool.acquire().unwrap();
    assert_eq!(
        first
            .client()
            .get("http://127.0.0.1:9/resource")
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
        "ok"
    );
    first.report_success();
    assert_eq!(proxy.connections.load(Ordering::SeqCst), 1);

    // Age only the cache fixture; the test need not sleep through a real TTL.
    let cache_path = directory
        .path()
        .join(format!("{}.json", server.source.key()));
    let mut saved: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&cache_path).unwrap()).unwrap();
    let old_time = SystemTime::now() - Duration::from_secs(60);
    saved["saved_at"] = serde_json::to_value(old_time).unwrap();
    std::fs::write(&cache_path, serde_json::to_vec(&saved).unwrap()).unwrap();
    server.respond(304, "", "");
    let report = pool.refresh().await.unwrap();
    assert_eq!(report.not_modified_sources, 1);
    assert_eq!(report.updated_sources, 0);
    assert_eq!(report.sources[0].outcome, SourceOutcome::NotModified);
    assert_eq!(
        pool.nodes()
            .iter()
            .map(|node| node.id())
            .collect::<Vec<_>>(),
        ids
    );
    let mut second = pool.acquire().unwrap();
    assert_eq!(second.node().name(), "first");
    assert_eq!(
        second
            .client()
            .get("http://127.0.0.1:9/resource")
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
        "ok"
    );
    second.report_success();
    assert_eq!(
        proxy.connections.load(Ordering::SeqCst),
        1,
        "304 must retain the client's reusable connection"
    );
    assert_eq!(acquired_name(&pool), "first");
    assert_eq!(
        acquired_name(&pool),
        "second",
        "304 must not reset the request quota"
    );
    assert!(server.requests.lock().unwrap()[1].contains("if-none-match: \"one\"\r\n"));

    let saved: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&cache_path).unwrap()).unwrap();
    let renewed: SystemTime = serde_json::from_value(saved["saved_at"].clone()).unwrap();
    assert!(renewed > old_time + Duration::from_secs(30));
    let restarted = builder(&server.source).cache(cache).build().await.unwrap();
    assert_eq!(
        server.hits(),
        2,
        "revalidation must make startup cache fresh again"
    );
    assert_eq!(
        restarted.last_refresh_report().unwrap().sources[0].outcome,
        SourceOutcome::FreshCache
    );
}

#[tokio::test]
async fn reports_capture_startup_manual_refresh_and_sanitized_retained_failures() {
    let server = Server::new(FIRST).await;
    let pool = builder(&server.source).build().await.unwrap();
    let startup = pool.last_refresh_report().unwrap();
    assert_eq!(startup.updated_sources, 1);
    assert_eq!(startup.sources[0].source_key, server.source.key());
    assert_eq!(startup.sources[0].outcome, SourceOutcome::Updated);
    assert!(startup.sources[0].error.is_none());

    server.respond(200, "http://127.0.0.1:18003#new", "");
    pool.refresh().await.unwrap();
    assert_eq!(pool.last_refresh_report().unwrap().nodes, 1);
    for (status, body) in [
        (503, "credential-secret"),
        (200, "proxies: [credential-secret"),
    ] {
        server.respond(status, body, "");
        let report = pool.refresh().await.unwrap();
        assert_eq!(report.failed_sources, 1);
        assert_eq!(report.sources[0].outcome, SourceOutcome::Retained);
        assert!(report.sources[0].error.is_some());
        let stored = pool.last_refresh_report().unwrap();
        assert_eq!(stored.sources[0].outcome, SourceOutcome::Retained);
        assert_eq!(stored.sources[0].error, report.sources[0].error);
        let diagnostics = format!("{stored:?}");
        for secret in ["credential-secret", "private-path", "token="] {
            assert!(!diagnostics.contains(secret));
        }
        assert_eq!(acquired_name(&pool), "new");
    }
}

#[tokio::test]
async fn overlapping_refreshes_share_one_remote_request_wave() {
    let server = Server::new(FIRST).await;
    let pool = builder(&server.source).build().await.unwrap();
    let gate = Arc::new(Semaphore::new(0));
    *server.gate.lock().unwrap() = Some(Arc::clone(&gate));
    server.respond(200, "http://127.0.0.1:18003#new", "");
    let refreshes = join_all((0..24).map(|_| pool.refresh()));
    let release = async {
        server.wait_for_hits(2).await;
        gate.add_permits(24);
    };
    let (reports, ()) = tokio::join!(refreshes, release);
    for report in reports {
        let report = report.unwrap();
        assert_eq!(report.updated_sources, 1);
        assert_eq!(report.nodes, 1);
    }
    assert_eq!(
        server.hits(),
        2,
        "overlapping calls must reuse the completed refresh"
    );
    assert_eq!(acquired_name(&pool), "new");
}

#[tokio::test]
async fn maintenance_handles_share_a_loop_and_only_last_drop_stops_it() {
    let server = Server::new(FIRST).await;
    let period = Duration::from_millis(100);
    let pool = builder(&server.source)
        .refresh_interval(period)
        .build()
        .await
        .unwrap();
    let started = Instant::now();
    let first = pool.spawn_maintenance();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let second = pool.spawn_maintenance();
    let cloned = second.clone();
    // Staggered starts expose duplicate loops even if simultaneous refreshes coalesce.
    tokio::time::sleep(Duration::from_millis(280)).await;
    let hits = server.hits();
    let elapsed_periods = started.elapsed().as_millis() / period.as_millis();
    assert!(hits >= 2);
    assert!(
        hits <= 1 + elapsed_periods as usize,
        "repeated spawn started another refresh loop"
    );
    drop(first);
    server.wait_for_hits(hits + 1).await;
    let hits = server.hits();
    drop(second);
    server.wait_for_hits(hits + 1).await;
    assert_eq!(
        pool.last_refresh_report().unwrap().sources[0].outcome,
        SourceOutcome::Updated
    );
    drop(cloned);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let stopped_at = server.hits();
    tokio::time::sleep(period * 2).await;
    assert_eq!(
        server.hits(),
        stopped_at,
        "last handle drop must stop background requests"
    );
}

#[tokio::test]
async fn shutdown_stops_every_shared_handle_and_allows_a_fresh_restart() {
    let server = Server::new(FIRST).await;
    let period = Duration::from_millis(50);
    let pool = builder(&server.source)
        .refresh_interval(period)
        .build()
        .await
        .unwrap();
    let first = pool.spawn_maintenance();
    let second = pool.spawn_maintenance();
    let cloned = first.clone();
    server.wait_for_hits(2).await;
    first.shutdown().await;
    let stopped_at = server.hits();
    tokio::time::sleep(period * 3).await;
    assert_eq!(server.hits(), stopped_at);
    let restarted = pool.spawn_maintenance();
    server.wait_for_hits(stopped_at + 1).await;
    drop(second);
    drop(cloned);
    let hits = server.hits();
    server.wait_for_hits(hits + 1).await;
    restarted.shutdown().await;
}

#[tokio::test]
async fn leases_from_removed_generations_cannot_change_readded_endpoint_health_or_capacity() {
    for old_success in [false, true] {
        let server = Server::new(FIRST).await;
        let pool = builder(&server.source)
            .rotation(RotationPolicy {
                requests_per_proxy: NonZeroU64::new(10),
                ..RotationPolicy::default()
            })
            .health(HealthPolicy {
                failure_threshold: NonZeroU32::new(1).unwrap(),
                cooldown_jitter: 0.0,
                ..HealthPolicy::default()
            })
            .max_in_flight_per_proxy(NonZeroUsize::new(1).unwrap())
            .build()
            .await
            .unwrap();
        let mut old = pool.acquire().unwrap();
        assert_eq!(old.node().name(), "first");
        server.respond(200, "http://127.0.0.1:18002#second", "");
        pool.refresh().await.unwrap();
        server.respond(200, FIRST, "");
        pool.refresh().await.unwrap();
        let mut current = pool.acquire().unwrap();
        assert_eq!(current.node().name(), "first");
        assert_eq!(pool.stats().in_flight, 1);
        if old_success {
            current.report_failure();
            assert_eq!(pool.stats().eligible, 1);
            old.report_success();
            assert_eq!(
                pool.stats().eligible,
                1,
                "old success must not recover a new unhealthy entry"
            );
        } else {
            old.report_failure();
            assert_eq!(
                pool.stats().in_flight,
                1,
                "old completion must not release a new generation's slot"
            );
            current.report_success();
            assert_eq!(
                pool.stats().eligible,
                2,
                "old failure must not quarantine a new healthy entry"
            );
        }
    }
}
