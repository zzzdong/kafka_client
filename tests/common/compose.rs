//! Docker Compose 集群生命周期管理
//!
//! 提供按需启动/等待/清理 compose 集群的能力。
//! 如果对应集群类型的环境变量已设置（由 `run-all-tests.sh` 管理），
//! 则跳过 compose 操作，直接使用已运行的集群。
//!
//! # 核心原则
//!
//! 1. 每次调用 `ensure()` 都从环境变量或默认值重新解析地址
//! 2. 全局状态只用于防止重复启动 compose（`AtomicBool`），不缓存地址
//! 3. `common/mod.rs` 的 `bootstrap_addrs()` / `build_test_client()` 完全基于环境变量
//! 4. 不同集群类型（THREE_BROKER / SASL / TLS）完全独立，不互相干扰
//!
//! # 用法
//!
//! ```ignore
//! use common::compose;
//!
//! // 声明需要的集群类型（外部管理时无操作，否则自动启动）
//! compose::ensure(&compose::clusters::THREE_BROKER).await;
//!
//! // 然后通过 common::build_test_client() 获取连接
//! let client = common::build_test_client().await;
//! ```

use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

// ============================================================================
// 集群配置
// ============================================================================

pub struct Cluster {
    /// 人类可读名称（用于日志）
    pub name: &'static str,
    /// docker-compose 文件名
    pub compose_file: &'static str,
    /// 环境变量名（如 "KAFKA_BOOTSTRAP"、"KAFKA_BOOTSTRAP_SASL"）
    pub env_var: &'static str,
    /// 默认 bootstrap 地址（当环境变量未设置时使用）
    pub default_bootstrap: &'static str,
    /// 默认集群大小
    pub default_size: usize,
    /// 容器名称列表
    pub containers: &'static [&'static str],
    /// 容器内部端口（用于 readiness 探测）
    pub internal_port: u16,
    /// 主机端口（用于 TCP 连接探测）
    pub host_port: u16,
    /// 是否是安全集群（TLS/SASL），无法用明文 Metadata 检查就绪状态
    pub is_secure: bool,
}

pub mod clusters {
    use super::Cluster;

    pub const THREE_BROKER: Cluster = Cluster {
        name: "3-broker",
        compose_file: "docker-compose.yml",
        env_var: "KAFKA_BOOTSTRAP",
        default_bootstrap: "127.0.0.1:29093,127.0.0.1:29095,127.0.0.1:29097",
        default_size: 3,
        containers: &["kafka-1", "kafka-2", "kafka-3"],
        internal_port: 9092,
        host_port: 29093,
        is_secure: false,
    };

    pub const SINGLE_NODE: Cluster = Cluster {
        name: "single-node",
        compose_file: "docker-compose.single.yml",
        env_var: "KAFKA_BOOTSTRAP",
        default_bootstrap: "127.0.0.1:29092",
        default_size: 1,
        containers: &["kafka-single"],
        internal_port: 9092,
        host_port: 29092,
        is_secure: false,
    };

    pub const SASL: Cluster = Cluster {
        name: "sasl",
        compose_file: "docker-compose.sasl.yml",
        env_var: "KAFKA_BOOTSTRAP_SASL",
        default_bootstrap: "127.0.0.1:9094",
        default_size: 1,
        containers: &["kafka-sasl-broker"],
        internal_port: 9094,
        host_port: 9094,
        is_secure: true,
    };

    pub const TLS: Cluster = Cluster {
        name: "tls",
        compose_file: "docker-compose.tls.yml",
        env_var: "KAFKA_BOOTSTRAP_TLS",
        default_bootstrap: "127.0.0.1:9093",
        default_size: 1,
        containers: &["kafka-tls-broker"],
        internal_port: 9093,
        host_port: 9093,
        is_secure: true,
    };
}

// ============================================================================
// 全局标志 — 只用于防止重复启动 compose
// ============================================================================

static COMPOSE_STARTED: AtomicBool = AtomicBool::new(false);

// ============================================================================
// 公共 API
// ============================================================================

/// 确保指定集群正在运行。
///
/// - 如果对应环境变量已设置（由 `run-all-tests.sh` 管理），直接返回
/// - 否则自动启动 docker-compose 并等待就绪
/// - 进程内只启动一次（`AtomicBool` 保护）
pub async fn ensure(cluster: &Cluster) {
    // 环境变量已设置 → 外部管理，完全跳过
    let externally_managed = is_externally_managed(cluster);
    if externally_managed {
        return;
    }

    // 仅启动一次（跨所有集群类型）
    if COMPOSE_STARTED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        eprintln!(">>> Compose already started by another test, reusing...");
        return;
    }

    eprintln!(
        ">>> Starting compose cluster '{}' (file: {})...",
        cluster.name, cluster.compose_file
    );
    start_compose(cluster).await;
    wait_for_cluster(cluster).await;

    // 设置环境变量，使 common/mod.rs 的函数可用
    unsafe {
        std::env::set_var(cluster.env_var, cluster.default_bootstrap);
        std::env::set_var("KAFKA_CLUSTER_SIZE", cluster.default_size.to_string());
    }
}

/// 检查集群是否是外部管理的（环境变量已设置）
pub fn is_externally_managed(cluster: &Cluster) -> bool {
    std::env::var(cluster.env_var).is_ok()
}

// ============================================================================
// 内部函数
// ============================================================================

/// 执行 docker-compose up -d
async fn start_compose(cluster: &Cluster) {
    let cli = detect_container_cli();
    let test_dir = compose_dir();

    let status = tokio::process::Command::new(&cli)
        .args([
            "compose",
            "-f",
            &format!("{}/{}", test_dir, cluster.compose_file),
            "up",
            "-d",
        ])
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
        .await
        .expect(&format!("Failed to execute '{} compose'", cli));

    assert!(
        status.success(),
        "'{} compose up' failed for '{}'",
        cli,
        cluster.compose_file
    );
}

/// 获取 tests/ 目录的绝对路径
fn compose_dir() -> String {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.join("tests").to_string_lossy().to_string()
}

/// 检测容器 CLI
fn detect_container_cli() -> String {
    if let Ok(cli) = std::env::var("KAFKA_CLI") {
        return cli;
    }
    for cli in &["podman", "docker"] {
        if std::process::Command::new(cli)
            .arg("ps")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
        {
            return cli.to_string();
        }
    }
    "docker".to_string()
}

/// 等待集群就绪。
///
/// 1. 等待所有 bootstrap 地址的 TCP 端口可用
/// 2. 对非安全集群（PLAINTEXT）：用 Kafka Metadata 请求验证 API 就绪
/// 3. 对安全集群（TLS/SASL）：TCP 就绪后额外等待 KRaft 选举完成
async fn wait_for_cluster(cluster: &Cluster) {
    let addrs = resolve_bootstrap_addrs(cluster);
    let timeout = Duration::from_secs(90);
    let deadline = std::time::Instant::now() + timeout;

    eprintln!(
        "  Waiting for {} container(s) (timeout={:?})...",
        cluster.containers.len(),
        timeout
    );

    // Phase 1: TCP port check (适用于所有集群类型)
    for addr in &addrs {
        loop {
            if std::time::Instant::now() > deadline {
                panic!("Timeout waiting for cluster '{}' at {}", cluster.name, addr);
            }
            if TcpStream::connect_timeout(addr, Duration::from_secs(2)).is_ok() {
                eprintln!("  TCP port {} ready", addr.port());
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    // Phase 2: Kafka API 就绪检查
    if cluster.is_secure {
        // 安全集群（TLS/SASL）：无法用明文连接，等待后乐观继续
        tokio::time::sleep(Duration::from_secs(10)).await;
        eprintln!(
            "  Secure cluster '{}': TCP ready + 10s grace, continuing",
            cluster.name
        );
    } else {
        // 明文集群：实际发送 Metadata 请求验证
        for attempt in 1..=30 {
            let client = kafka_client::Client::builder(addrs.clone())
                .with_client_id("compose-wait")
                .with_metadata_ttl(Duration::from_secs(5))
                .build()
                .await;
            match client {
                Ok(c) => {
                    let meta_result = c.refresh_metadata().await;
                    let _ = c.close().await;
                    if meta_result.is_ok() {
                        eprintln!("  Kafka API ready after ~{}s", attempt);
                        return;
                    }
                }
                Err(_) => {}
            }
            if std::time::Instant::now() + Duration::from_secs(3) > deadline {
                panic!("Timeout waiting for Kafka API at {:?}", addrs);
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        eprintln!("  [WARN] Kafka API not confirmed ready, continuing optimistically");
    }
}

/// 从环境变量或默认值解析 bootstrap 地址
fn resolve_bootstrap_addrs(cluster: &Cluster) -> Vec<SocketAddr> {
    let bootstrap_str = std::env::var(cluster.env_var)
        .or_else(|_| std::env::var("KAFKA_BOOTSTRAP"))
        .unwrap_or_else(|_| cluster.default_bootstrap.to_string());
    bootstrap_str
        .split(',')
        .map(|s| {
            s.trim()
                .parse()
                .expect(&format!("Invalid bootstrap address: '{}'", s))
        })
        .collect()
}
