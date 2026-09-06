use std::{
    collections::HashMap,
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    sync::{Arc, Mutex},
    time::Duration,
};

use subscription_proxy_pool::{
    Error, HealthPolicy, ProxyNode, ProxyPool, RotationPolicy,
    reqwest::{Body, Method, Request, StatusCode, Version},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::{JoinHandle, JoinSet},
};

/// A local forward proxy that records the absolute request URI and responds
/// itself. It never resolves or connects to the request's target.
struct MockProxy {
    node: ProxyNode,
    requests: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<()>,
}

#[derive(Clone, Copy)]
enum ProxyBehavior {
    Respond(u16),
    Timeout,
    Disconnect,
}

impl MockProxy {
    async fn responding(name: &str, status: u16) -> Self {
        Self::start(name, ProxyBehavior::Respond(status)).await
    }

    async fn timing_out(name: &str) -> Self {
        Self::start(name, ProxyBehavior::Timeout).await
    }

    async fn disconnecting(name: &str) -> Self {
        Self::start(name, ProxyBehavior::Disconnect).await
    }

    async fn start(name: &str, behavior: ProxyBehavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node = ProxyNode::from_url(&format!("http://{}", listener.local_addr().unwrap()))
            .unwrap()
            .with_name(name);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let body = name.to_owned();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut headers = Vec::new();
                loop {
                    let mut chunk = [0_u8; 1024];
                    let count = socket.read(&mut chunk).await.unwrap();
                    if count == 0 {
                        break;
                    }
                    headers.extend_from_slice(&chunk[..count]);
                    if headers.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                    assert!(headers.len() < 16_384, "unexpectedly large request headers");
                }
                if headers.is_empty() {
                    continue;
                }
                let request = String::from_utf8(headers).unwrap();
                recorded
                    .lock()
                    .unwrap()
                    .push(request.lines().next().unwrap().to_owned());
                let status = match behavior {
                    ProxyBehavior::Respond(status) => status,
                    ProxyBehavior::Timeout => {
                        // Keep this connection open until the test drops the server.
                        std::future::pending::<()>().await;
                        unreachable!();
                    }
                    ProxyBehavior::Disconnect => continue,
                };
                let response = format!(
                    "HTTP/1.1 {status} Mock\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
                socket.shutdown().await.unwrap();
            }
        });
        Self {
            node,
            requests,
            task,
        }
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for MockProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn rotation(requests: u64) -> RotationPolicy {
    RotationPolicy {
        requests_per_proxy: NonZeroU64::new(requests),
        ..RotationPolicy::default()
    }
}

fn health() -> HealthPolicy {
    HealthPolicy {
        check_on_build: false,
        check_url: Some("http://127.0.0.1:1/health".into()),
        cooldown: Duration::from_secs(60),
        cooldown_jitter: 0.0,
        ..HealthPolicy::default()
    }
}

async fn pool(nodes: impl IntoIterator<Item = ProxyNode>, requests: u64) -> ProxyPool {
    ProxyPool::builder()
        .nodes(nodes)
        .rotation(rotation(requests))
        .health(health())
        .request_timeout(Duration::from_secs(2))
        .build()
        .await
        .unwrap()
}

fn request(method: Method) -> Request {
    Request::new(
        method,
        "http://127.0.0.1:1/resource?case=rotation".parse().unwrap(),
    )
}

#[tokio::test]
async fn execute_sends_exactly_two_requests_through_each_proxy_before_switching() {
    let first = MockProxy::responding("first", 200).await;
    let second = MockProxy::responding("second", 200).await;
    let pool = pool([first.node.clone(), second.node.clone()], 2).await;

    for expected in ["first", "first", "second", "second", "first", "first"] {
        let response = pool.execute(request(Method::GET)).await.unwrap();
        assert_eq!(response.text().await.unwrap(), expected);
    }

    assert_eq!(first.requests().len(), 4);
    assert_eq!(second.requests().len(), 2);
    for request in first.requests().into_iter().chain(second.requests()) {
        assert_eq!(
            request,
            "GET http://127.0.0.1:1/resource?case=rotation HTTP/1.1"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_pool_clones_share_one_request_budget() {
    // Acquisitions do not send network traffic or run probes.
    let nodes = (0..4).map(|index| {
        ProxyNode::from_url(&format!("http://127.0.0.1:{}", 10_001 + index))
            .unwrap()
            .with_name(format!("node-{index}"))
    });
    let pool = pool(nodes, 20).await;
    let mut tasks = JoinSet::new();
    for _ in 0..8 {
        let clone = pool.clone();
        tasks.spawn(async move {
            let mut names = Vec::new();
            for _ in 0..100 {
                names.push(clone.acquire().unwrap().node().name().to_owned());
                tokio::task::yield_now().await;
            }
            names
        });
    }
    let mut counts = HashMap::new();
    while let Some(task) = tasks.join_next().await {
        for name in task.unwrap() {
            *counts.entry(name).or_insert(0) += 1;
        }
    }
    for index in 0..4 {
        assert_eq!(counts[&format!("node-{index}")], 200);
    }
    assert_eq!(pool.acquire().unwrap().node().name(), "node-0");
}

#[tokio::test]
async fn manual_rotation_through_a_clone_preserves_shared_round_robin_position() {
    let nodes = (0..3).map(|index| {
        ProxyNode::from_url(&format!("http://127.0.0.1:{}", 11_001 + index))
            .unwrap()
            .with_name(format!("node-{index}"))
    });
    let pool = pool(nodes, 0).await;
    let clone = pool.clone();
    for expected in ["node-0", "node-1", "node-2", "node-0"] {
        for _ in 0..25 {
            assert_eq!(pool.acquire().unwrap().node().name(), expected);
        }
        clone.rotate();
    }
}

#[tokio::test]
async fn get_failover_changes_proxy_after_transport_timeout_before_cooldown_threshold() {
    let first = MockProxy::timing_out("first").await;
    let second = MockProxy::responding("second", 200).await;
    let pool = ProxyPool::builder()
        .nodes([first.node.clone(), second.node.clone()])
        .health(health())
        .request_timeout(Duration::from_millis(300))
        .build()
        .await
        .unwrap();

    let response = pool
        .execute_with_failover(request(Method::GET), NonZeroUsize::new(3).unwrap())
        .await
        .unwrap();
    assert_eq!(response.text().await.unwrap(), "second");
    assert_eq!(first.requests().len(), 1);
    assert_eq!(second.requests().len(), 1);
    // A single failure does not yet quarantine the first node (threshold = 2).
    assert_eq!(pool.stats().eligible, 2);
}

#[tokio::test]
async fn disconnects_reselect_sticky_pools_and_sessions_and_eventually_quarantine() {
    for use_session in [false, true] {
        let broken = MockProxy::disconnecting("broken").await;
        let healthy = MockProxy::responding("healthy", 200).await;
        let pool = ProxyPool::builder()
            .nodes([broken.node.clone(), healthy.node.clone()])
            .rotation(RotationPolicy::sticky())
            .max_in_flight_per_proxy(NonZeroUsize::new(1).unwrap())
            .request_timeout(Duration::from_secs(2))
            .build()
            .await
            .unwrap();
        let session = use_session.then(|| pool.session(RotationPolicy::sticky()).unwrap());

        for (attempt, eligible) in [2, 1].into_iter().enumerate() {
            let result = match &session {
                Some(session) => session.execute(request(Method::GET)).await,
                None => pool.execute(request(Method::GET)).await,
            };
            assert!(matches!(result, Err(Error::Transport(error))
                if error.is_request() && !error.is_connect() && !error.is_timeout()));
            assert_eq!(pool.stats().eligible, eligible);
            assert_eq!(pool.stats().in_flight, 0);
            assert_eq!(broken.requests().len(), attempt + 1);
            assert_eq!(healthy.requests().len(), attempt, "execute must not replay");

            let response = match &session {
                Some(session) => session.execute(request(Method::GET)).await,
                None => pool.execute(request(Method::GET)).await,
            }
            .unwrap();
            assert_eq!(response.text().await.unwrap(), "healthy");
            // Return to the broken node to verify consecutive failures across
            // separate attempts; the first failure must already reselect.
            match &session {
                Some(session) => session.rotate(),
                None => pool.rotate(),
            }
        }
    }
}

#[tokio::test]
async fn local_request_errors_do_not_quarantine_or_reselect_the_proxy() {
    let first = MockProxy::responding("first", 200).await;
    let second = MockProxy::responding("second", 200).await;
    let pool = ProxyPool::builder()
        .nodes([first.node.clone(), second.node.clone()])
        .rotation(RotationPolicy::sticky())
        .health(HealthPolicy {
            failure_threshold: NonZeroU32::new(1).unwrap(),
            ..HealthPolicy::default()
        })
        .build()
        .await
        .unwrap();
    let unsupported_scheme = Request::new(Method::GET, "ftp://127.0.0.1/file".parse().unwrap());
    let mut unsupported_version = request(Method::GET);
    *unsupported_version.version_mut() = Version::HTTP_09;

    for request in [unsupported_scheme, unsupported_version] {
        assert!(matches!(
            pool.execute(request).await,
            Err(Error::Transport(_))
        ));
        assert_eq!(pool.stats().eligible, 2);
        assert_eq!(pool.stats().in_flight, 0);
        assert_eq!(pool.acquire().unwrap().node().name(), "first");
    }
    assert!(first.requests().is_empty());
    assert!(second.requests().is_empty());
}

#[tokio::test]
async fn a_failing_request_body_does_not_quarantine_or_reselect_the_proxy() {
    let first = MockProxy::timing_out("first").await;
    let second = MockProxy::responding("second", 200).await;
    let pool = ProxyPool::builder()
        .nodes([first.node.clone(), second.node.clone()])
        .rotation(RotationPolicy::sticky())
        .health(HealthPolicy {
            failure_threshold: NonZeroU32::new(1).unwrap(),
            ..HealthPolicy::default()
        })
        .request_timeout(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    for kind in [
        std::io::ErrorKind::ConnectionReset,
        std::io::ErrorKind::TimedOut,
    ] {
        let mut request = request(Method::POST);
        *request.body_mut() = Some(Body::wrap_stream(futures_util::stream::once(async move {
            Err::<Vec<u8>, _>(std::io::Error::new(kind, "caller-provided body failed"))
        })));

        let result = pool.execute(request).await;
        assert!(matches!(result, Err(Error::Transport(error)) if error.is_request()));
        assert_eq!(pool.stats().eligible, 2);
        assert_eq!(pool.stats().in_flight, 0);
        assert_eq!(pool.acquire().unwrap().node().name(), "first");
    }
    assert!(second.requests().is_empty());
}

#[tokio::test]
async fn failover_exhausts_distinct_nodes_without_retrying_a_failed_node() {
    let first = MockProxy::timing_out("first").await;
    let second = MockProxy::timing_out("second").await;
    let pool = ProxyPool::builder()
        .nodes([first.node.clone(), second.node.clone()])
        .health(health())
        .request_timeout(Duration::from_millis(300))
        .build()
        .await
        .unwrap();

    let result = pool
        .execute_with_failover(request(Method::GET), NonZeroUsize::new(5).unwrap())
        .await;
    assert!(matches!(result, Err(Error::Transport(error)) if error.is_timeout()));
    assert_eq!(first.requests().len(), 1);
    assert_eq!(second.requests().len(), 1);
    assert_eq!(pool.stats().eligible, 2);
}

#[tokio::test]
async fn post_failover_is_rejected_before_any_request_or_quota_is_consumed() {
    let first = MockProxy::responding("first", 200).await;
    let second = MockProxy::responding("second", 200).await;
    let pool = pool([first.node.clone(), second.node.clone()], 1).await;
    let result = pool
        .execute_with_failover(request(Method::POST), NonZeroUsize::new(2).unwrap())
        .await;

    assert!(matches!(result, Err(Error::Config(_))));
    assert!(first.requests().is_empty());
    assert!(second.requests().is_empty());
    assert_eq!(pool.acquire().unwrap().node().name(), "first");
}

#[tokio::test]
async fn upstream_http_errors_do_not_trigger_failover_or_quarantine() {
    for status in [404, 503] {
        let first = MockProxy::responding("first", status).await;
        let second = MockProxy::responding("second", 200).await;
        let pool = ProxyPool::builder()
            .nodes([first.node.clone(), second.node.clone()])
            .rotation(RotationPolicy::every(20.try_into().unwrap()))
            .health(HealthPolicy {
                failure_threshold: NonZeroU32::new(1).unwrap(),
                ..health()
            })
            .build()
            .await
            .unwrap();

        for _ in 0..2 {
            let response = pool
                .execute_with_failover(request(Method::GET), NonZeroUsize::new(2).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), status);
            assert_eq!(response.text().await.unwrap(), "first");
        }
        assert_eq!(pool.stats().eligible, 2);
        assert_eq!(first.requests().len(), 2);
        assert!(second.requests().is_empty());
    }
}

#[tokio::test]
async fn proxy_authentication_failure_quarantines_and_rotates_without_replaying_response() {
    let first = MockProxy::responding("first", 407).await;
    let second = MockProxy::responding("second", 200).await;
    let pool = ProxyPool::builder()
        .nodes([first.node.clone(), second.node.clone()])
        .health(HealthPolicy {
            failure_threshold: NonZeroU32::new(1).unwrap(),
            ..health()
        })
        .build()
        .await
        .unwrap();

    let response = pool
        .execute_with_failover(request(Method::GET), NonZeroUsize::new(2).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PROXY_AUTHENTICATION_REQUIRED);
    assert_eq!(pool.stats().eligible, 1);
    assert!(second.requests().is_empty());
    assert_eq!(
        pool.execute(request(Method::GET))
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
        "second"
    );
}

#[tokio::test]
async fn no_eligible_proxy_returns_an_error_without_connecting_directly() {
    let proxy = MockProxy::responding("proxy", 200).await;
    let origin = MockProxy::responding("direct connection must not happen", 200).await;
    let pool = ProxyPool::builder()
        .nodes([proxy.node.clone()])
        .health(HealthPolicy {
            failure_threshold: NonZeroU32::new(1).unwrap(),
            ..health()
        })
        .build()
        .await
        .unwrap();
    pool.acquire().unwrap().report_failure();
    assert_eq!(pool.stats().eligible, 0);

    let request = Request::new(Method::GET, origin.node.url().parse().unwrap());
    assert!(matches!(
        pool.execute(request).await,
        Err(Error::NoProxyAvailable)
    ));
    assert!(proxy.requests().is_empty());
    assert!(origin.requests().is_empty());
}

#[tokio::test]
async fn an_empty_pool_is_rejected_instead_of_enabling_direct_connections() {
    let result = ProxyPool::builder().health(health()).build().await;
    assert!(matches!(result, Err(Error::NoProxyAvailable)));
}
