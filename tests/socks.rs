//! Exercise the real reqwest SOCKS connector against a loopback-only server.
//! The mock never resolves or connects to the requested destination.

use std::{net::SocketAddr, time::Duration};

use subscription_proxy_pool::{
    HealthPolicy, ProxyNode, ProxyPool, parse_subscription,
    reqwest::{Method, Request, Url},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

const DESTINATION: &str = "unresolved-target.invalid";
const DESTINATION_PORT: u16 = 8123;
const USERNAME: &str = "user+%40@/";
const PASSWORD: &str = "pass%2F:@?#/% +中文";

struct Observation {
    credentials: Option<(String, String)>,
    destination: String,
    port: u16,
    request: String,
}

async fn read_field(socket: &mut TcpStream) -> String {
    let length = socket.read_u8().await.unwrap();
    let mut bytes = vec![0; usize::from(length)];
    socket.read_exact(&mut bytes).await.unwrap();
    String::from_utf8(bytes).unwrap()
}

async fn serve(listener: TcpListener, authenticated: bool) -> Observation {
    let (mut socket, _) = listener.accept().await.unwrap();
    assert_eq!(socket.read_u8().await.unwrap(), 5, "SOCKS version");
    let count = socket.read_u8().await.unwrap();
    let mut methods = vec![0; usize::from(count)];
    socket.read_exact(&mut methods).await.unwrap();
    let method = if authenticated { 2 } else { 0 };
    assert!(
        methods.contains(&method),
        "required auth method was offered"
    );
    socket.write_all(&[5, method]).await.unwrap();

    let credentials = if authenticated {
        assert_eq!(socket.read_u8().await.unwrap(), 1, "RFC 1929 version");
        let username = read_field(&mut socket).await;
        let password = read_field(&mut socket).await;
        socket.write_all(&[1, 0]).await.unwrap();
        Some((username, password))
    } else {
        None
    };

    let mut connect = [0; 4];
    socket.read_exact(&mut connect).await.unwrap();
    assert_eq!(connect[0], 5);
    assert_eq!(connect[1], 1, "the client requests CONNECT");
    assert_eq!(connect[2], 0);
    assert_eq!(connect[3], 3, "SOCKS5H sends the domain for remote DNS");
    let destination = read_field(&mut socket).await;
    let port = socket.read_u16().await.unwrap();
    socket
        .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
        .await
        .unwrap();

    let mut request = Vec::new();
    let mut buffer = [0; 1024];
    while !request.windows(4).any(|part| part == b"\r\n\r\n") {
        let count = socket.read(&mut buffer).await.unwrap();
        assert!(count > 0, "HTTP request must follow the SOCKS tunnel");
        request.extend_from_slice(&buffer[..count]);
        assert!(request.len() <= 16_384, "bounded test request headers");
    }
    socket
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\nvia socks5")
        .await
        .unwrap();
    Observation {
        credentials,
        destination,
        port,
        request: String::from_utf8(request).unwrap(),
    }
}

async fn mock(authenticated: bool) -> (SocketAddr, JoinHandle<Observation>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(4), serve(listener, authenticated))
            .await
            .expect("SOCKS handshake should finish")
    });
    (address, task)
}

async fn exercise(node: ProxyNode, server: JoinHandle<Observation>) -> Observation {
    let pool = ProxyPool::builder()
        .nodes([node])
        .health(HealthPolicy {
            check_on_build: false,
            check_url: Some("http://127.0.0.1:1/health".into()),
            ..HealthPolicy::default()
        })
        .request_timeout(Duration::from_secs(3))
        .connect_timeout(Duration::from_secs(3))
        .build()
        .await
        .unwrap();
    let request = Request::new(
        Method::GET,
        Url::parse(&format!(
            "http://{DESTINATION}:{DESTINATION_PORT}/resource?case=socks"
        ))
        .unwrap(),
    );
    let response = pool.execute(request).await;
    let observation = server.await.unwrap();
    let response = response.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), "via socks5");
    assert_eq!(observation.destination, DESTINATION);
    assert_eq!(observation.port, DESTINATION_PORT);
    assert!(
        observation
            .request
            .starts_with("GET /resource?case=socks HTTP/1.1\r\n")
    );
    assert!(
        !observation
            .request
            .to_ascii_lowercase()
            .contains("proxy-authorization"),
        "SOCKS credentials must not be forwarded to the destination HTTP server"
    );
    assert_eq!(pool.stats().eligible, 1);
    observation
}

#[tokio::test]
async fn clash_socks5h_uses_rfc1929_and_decodes_credentials_exactly_once() {
    let (address, server) = mock(true).await;
    let subscription = format!(
        "proxies:\n  - name: authenticated\n    type: socks5h\n    server: 127.0.0.1\n    port: {}\n    username: '{USERNAME}'\n    password: '{PASSWORD}'\n",
        address.port()
    );
    let node = parse_subscription(&subscription)
        .unwrap()
        .nodes
        .pop()
        .unwrap();
    let observation = exercise(node, server).await;
    assert_eq!(
        observation.credentials,
        Some((USERNAME.to_owned(), PASSWORD.to_owned()))
    );
}

#[tokio::test]
async fn uri_socks5h_supports_no_authentication_and_preserves_remote_dns() {
    let (address, server) = mock(false).await;
    let node = ProxyNode::from_url(&format!("socks5h://{address}")).unwrap();
    let observation = exercise(node, server).await;
    assert_eq!(observation.credentials, None);
}
