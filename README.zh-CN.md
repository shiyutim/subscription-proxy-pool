# subscription-proxy-pool

从订阅获取代理列表的 Rust 库，包含本地缓存、连接复用、节点健康管理和轮换。需要 Tokio 和 Rust 1.89 或更新版本。

默认每次请求按顺序轮换，通过实际请求反馈节点健康。缓存和主动探测按需开启；需要连续使用同一代理的业务可创建独立会话，共享底层连接与节点健康。

## 使用

在 Cargo 依赖中添加 `subscription-proxy-pool = "0.1"`；依赖配置和完整示例见 [英文 README](README.md#quick-start)。在本地源码目录中可运行以下示例：

```sh
export PROXY_SUBSCRIPTION_URL='你的订阅地址'
cargo run --example fetch -- https://example.com/
```

`ProxyPool` 应长期复用。`pool.clone()` 共享轮换计数、节点健康、连接池和容量限制。`build()` 默认不请求外部健康检查网站，也不启动后台任务。

## 轮换与独立会话

| 配置 | 行为 |
| --- | --- |
| `RotationPolicy::default()` | 每次预留请求都按顺序轮换 |
| `RotationPolicy::every(NonZeroU64)` | 同一轮选择使用 N 次，第 N+1 次切换 |
| `RotationPolicy::for_duration(Duration)?` | 使用到指定时长，在下一次获取时切换；时长必须大于零 |
| `RotationPolicy::sticky()` | 保持当前节点，直到不可用、被移除或手动切换 |
| 同时设置 `requests_per_proxy` 和 `max_age` | 任一限制到达即切换 |
| `SelectionStrategy::RoundRobin` | 按可用节点顺序轮换 |
| `SelectionStrategy::Random` | 从其他可用节点中均匀随机选择 |
| `SelectionStrategy::ShuffledRoundRobin` | 每轮打乱顺序，每个可用节点访问一次；次数和时长限制仍适用 |
| `pool.rotate()` / `session.rotate()` | 下次获取时切换对应池或会话的节点 |

有其他可用节点时，切换不重复选中当前节点；只有一个可用节点时仍可继续使用。打乱轮询会跳过不可用节点，新节点和恢复节点在下一轮加入。已经发出的请求可以继续在原节点完成。

`pool.session(policy)?` 创建独立轮换状态，适合登录流程、分页任务等需要连续使用一个代理的场景。`session.clone()` 共享同一会话；再次调用 `pool.session()` 才是新会话。不同会话首次使用的节点按顺序均摊，所有会话仍共享节点健康、HTTP 连接池和容量。节点故障或容量已满时会选择其他可用节点，因此 sticky 不保证公网 IP 永远不变。

```rust
use std::num::NonZeroU64;
use subscription_proxy_pool::{ProxyPool, RotationPolicy};

fn configure(pool: &ProxyPool) -> subscription_proxy_pool::Result<()> {
    let login = pool.session(RotationPolicy::sticky())?;
    let _same_login = login.clone();
    let _batch = pool.session(RotationPolicy::every(NonZeroU64::new(20).unwrap()))?;
    Ok(())
}
```

一次成功的 `acquire()` 预留一次请求尝试，即使随后失败或取消也消耗一个次数；获取失败不消耗配额。订阅下载、健康探测、重定向不额外计数。用 `execute()` 自动管理；手动使用 `ProxyLease::client()` 时，每个 lease 发送一次请求。

可用节点不变时复用共享列表和索引，保持节点、顺序轮询和随机选择不会每次遍历整个池。可用性变化时重建索引；打乱轮询还会在每轮开始时打乱节点顺序。

## 容量、失败和节点恢复

可通过 `max_in_flight_per_proxy(NonZeroUsize)` 限制每个节点的并发业务请求预留数，默认不限制。池和所有会话共同计算容量，满载节点会被跳过；所有健康节点都满载时，获取立即返回 `Error::PoolSaturated`，库内不排队。`stats()` 可查看预留总数和满载节点数。

`execute()` 在收到响应头时释放容量，并记录请求结果。若需要限制完整响应流的并发或将读取响应体失败纳入健康判断，可手动获取 lease，发送请求并读完响应体，再调用 `report_success()` 或 `report_failure()`，最后释放 lease。取消请求或丢弃 lease 会释放容量；未报告结果的 lease 不会被当作成功或失败。

默认仅从实际请求反馈健康。连接等传输错误或 HTTP 407 会触发重新选择；连续两次失败进入冷却。初始冷却 30 秒，恢复失败后指数增长，默认最多 5 分钟，带 20% 随机抖动且不超过上限。通过 `cooldown`、`max_cooldown`、`cooldown_jitter` 和 `failure_threshold` 配置。冷却后同一节点只放行一个恢复尝试，成功后重置退避；取消会释放恢复名额。目标网站普通 4xx/5xx 不直接判为代理损坏。

主动探测使用调用方提供的地址：

```rust
use subscription_proxy_pool::{HealthPolicy, ProxyPool};

fn configure() -> subscription_proxy_pool::Result<()> {
    let _builder = ProxyPool::builder()
        .health(HealthPolicy::active("https://your-service.example/health")?);
    Ok(())
}
```

`HealthPolicy::active(url)?` 开启初始检查并配置后续探测，默认最多 16 并发、每个 5 秒，实际通过代理请求，2xx/3xx 为成功。启用初始检查后，没有健康节点则构建失败；新加入的节点先执行探测，失败则进入冷却，到期后可放行一次恢复尝试，包括业务请求。保留 `check_url` 并设置 `check_on_build = false` 可先使用未探测节点。未配置地址时，`check_health()` 不发送网络探测。

`execute()` 只发送一次，不自动重放业务请求。显式调用 `execute_with_failover(request, max_attempts)` 才会对 GET/HEAD/OPTIONS 的连接失败或超时进行有限重试，要求请求体可克隆，每次使用不同节点。池没有可用代理时返回错误，不静默直连。

## 订阅和缓存

支持 Clash YAML/JSON 顶层 `proxies` 列表、HTTP(S)/SOCKS5(H) URI 列表，以及标准或 URL-safe Base64 包装。支持代理认证、IPv6；保留输入顺序去重，无效、重复或不支持的节点计入 `ParseReport::skipped`，结果没有可用节点则返回错误。

本库直接执行 HTTP、HTTPS、SOCKS5、SOCKS5H。Clash HTTP 节点的 `tls: true` 对应 HTTPS 代理；SOCKS5 与 SOCKS5H 分别使用本地和代理端目标 DNS 解析。SS、SSR、VMess、VLESS、Trojan、Hysteria、TLS 包装的 SOCKS 等节点会跳过，`proxy-providers` 不递归拉取，`skip-cert-verify` 不会关闭证书校验。

`parse_subscription()` 可独立使用，默认输入和解码内容上限为 4 MiB；通过 `parse_subscription_with_options(content, &ParseOptions { max_bytes })` 修改限制，池中对应 `max_subscription_bytes()`。上限必须为正数，YAML 嵌套和别名展开仍有结构限制。订阅下载也有整体超时。

磁盘缓存按需开启。`CachePolicy::new(directory)` 默认 3 天有效，过期后下载失败可再回退 7 天；设置 `max_stale = Duration::ZERO` 禁止过期回退。下载失败不续期，损坏、超大、未来时间、schema 不兼容或符号链接文件不采用。

ETag 和 Last-Modified 与节点一起保存在内存和磁盘，下次刷新发送条件请求。HTTP 304 复用已有节点并更新缓存有效期，省去重复下载和解析。没有验证器字段的旧 schema 1 缓存仍可读取。

文件按订阅 URL 哈希隔离，原子替换；Unix 新建目录为 `0700`、缓存文件为 `0600`，不更改已有目录权限。文件包含恢复连接所需的代理凭证，应放在私人目录。订阅 URL 不写入缓存，节点与订阅 Debug 输出脱敏；`ProxyNode::url()` 和 Serde 序列化会向调用者提供凭证。

## 刷新、诊断和后台任务

首次构建可以直接使用未过期缓存。`refresh()` 不受磁盘 TTL 限制，每次实际更新都会联系远端；重叠调用合并为同一次更新。多个来源按可配置并发获取，按配置顺序合并去重。失败或空更新保留对应来源原有节点，旧磁盘数据不会覆盖较新的内存节点。相同节点复用客户端、健康和轮换状态；被移除节点上已有请求可以完成。

磁盘回退期限只决定新池能否恢复旧数据。运行中的池在订阅故障期间保留节点，再通过节点健康判断能否承载请求。

`refresh()` 返回 `RefreshReport`，`last_refresh_report()` 可查看最近一次启动、手动或后台刷新。统计包括更新、304、缓存恢复、来源失败、跳过节点和缓存读写错误。`SourceReport` 提供来源哈希、结果、节点数和脱敏错误；来源请求失败但成功使用旧数据也能区分。

`spawn_maintenance()` 默认每小时刷新订阅，启用主动探测时每分钟检查节点，两个周期分别可配置。重复调用共享同一组任务。至少保留一个 handle 才会持续运行；最后一个 handle 丢弃会停止任务，任意 handle 的 `shutdown().await` 都会停止本轮共享任务并等待退出。

订阅请求默认直连，可通过 `subscription_client()` 设置用于拉取订阅的代理或自定义客户端。库不会修改环境变量；业务请求始终经过明确选定的代理。

## 验证与发布

测试使用本地模拟服务器。执行 `cargo test --all-targets` 和 `cargo test --doc`；完整检查及发布步骤见 [PUBLISHING.md](PUBLISHING.md)。包采用 MIT 许可证。
