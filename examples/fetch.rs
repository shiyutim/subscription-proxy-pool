use subscription_proxy_pool::{
    CachePolicy, ProxyPool, SubscriptionSource,
    reqwest::{Method, Request},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = SubscriptionSource::new(&std::env::var("PROXY_SUBSCRIPTION_URL")?)?;
    let target = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "https://example.com/".to_owned());
    // Defaults rotate on every request and learn health from request outcomes.
    // Reuse this pool, or create pool.session(RotationPolicy::sticky()) for a
    // workflow that should retain its selected proxy across several requests.
    let pool = ProxyPool::builder()
        .subscription(source)
        .cache(CachePolicy::new("./proxy-cache"))
        .build()
        .await?;
    eprintln!("refresh: {:?}", pool.last_refresh_report());
    let maintenance = pool.spawn_maintenance();
    let request = Request::new(Method::GET, target.parse()?);
    let response = pool.execute(request).await?;
    println!("status: {}", response.status());
    println!("{}", response.text().await?);
    maintenance.shutdown().await;
    Ok(())
}
