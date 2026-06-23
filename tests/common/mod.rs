#![allow(dead_code, unused_imports)]
//! Kafka 集成测试公共基础设施
//!
//! 支持多种 Kafka 部署形态：
//! - 单点（single-node）：本地开发/快速回归
//! - 分布式集群（multi-broker cluster）：通过外部 Docker Compose 或 CI 服务容器提供
//!
//! 运行时通过环境变量控制：
//! - `KAFKA_RUNTIME` = "auto" | "podman" | "docker" | "direct" | "external"（默认: auto）
//!     - auto：优先 podman，其次 direct
//!     - external：不启动 Kafka，使用 `KAFKA_BOOTSTRAP` 指向的外部集群
//! - `KAFKA_BOOTSTRAP`：逗号分隔的 bootstrap 地址，集群模式可填多个（默认: 127.0.0.1:29092）
//! - `KAFKA_IMAGE`：容器镜像名（默认: apache/kafka:4.3.0）
//! - `KAFKA_HOME`：direct 模式下 Kafka 发行版路径（默认: tests/kafka）
//! - `KAFKA_CLUSTER_SIZE`：期望的集群 broker 数量，用于健康检查（默认: 1）
//! - `KAFKA_CONTROLLER_PORT`：direct 单点模式控制器端口（默认: 29093）

use std::fs;
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use kafka_client::{
    AutoOffsetReset, ClusterConfig, Consumer, ConsumerConfig, ConsumerRecord,
    KafkaClient, PartitionAssignmentStrategy, PartitionRouting, Producer,
    ProducerConfig, ProducerRecord, RecordMetadata, SecurityProtocol,
};
use kafka_client::protocol::create_topics_request::CreatableTopic;
use kafka_client::protocol::{CreateTopicsRequest, CreateTopicsResponse};

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_BOOTSTRAP: &str = "127.0.0.1:29092";
const DEFAULT_CONTROLLER_PORT: u16 = 29093;
const DEFAULT_IMAGE: &str = "apache/kafka:4.3.0";

// ============================================================================
// Configuration
// ============================================================================

/// 集成测试配置，读取自环境变量。
#[derive(Debug, Clone)]
pub struct TestConfig {
    /// 运行时后端
    pub runtime: RuntimeBackend,
    /// 容器镜像名（podman/docker 模式）
    pub image: String,
    /// Bootstrap server 地址列表（逗号分隔解析）
    pub bootstrap_servers: Vec<String>,
    /// SocketAddr 格式的 bootstrap 列表
    pub bootstrap_addrs: Vec<SocketAddr>,
    /// Kafka 发行版目录（direct 模式）
    pub kafka_home: PathBuf,
    /// 期望的集群 broker 数量
    pub cluster_size: usize,
    /// direct 单点模式控制器端口
    pub controller_port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeBackend {
    Auto,
    Podman,
    Docker,
    Direct,
    External,
}

impl TestConfig {
    pub fn from_env() -> Self {
        let runtime = match std::env::var("KAFKA_RUNTIME").as_deref() {
            Ok("podman") => RuntimeBackend::Podman,
            Ok("docker") => RuntimeBackend::Docker,
            Ok("direct") => RuntimeBackend::Direct,
            Ok("external") => RuntimeBackend::External,
            _ => RuntimeBackend::Auto,
        };

        let image = std::env::var("KAFKA_IMAGE").unwrap_or_else(|_| DEFAULT_IMAGE.to_string());

        let bootstrap_servers: Vec<String> = std::env::var("KAFKA_BOOTSTRAP")
            .unwrap_or_else(|_| DEFAULT_BOOTSTRAP.to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if bootstrap_servers.is_empty() {
            panic!("KAFKA_BOOTSTRAP resolved to empty list");
        }

        let bootstrap_addrs: Vec<SocketAddr> = bootstrap_servers
            .iter()
            .map(|s| {
                s.parse().unwrap_or_else(|e| {
                    panic!("Invalid KAFKA_BOOTSTRAP address {:?}: {}", s, e);
                })
            })
            .collect();

        let kafka_home = std::env::var("KAFKA_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("tests")
                    .join("kafka")
            });

        let cluster_size = std::env::var("KAFKA_CLUSTER_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);

        let controller_port = std::env::var("KAFKA_CONTROLLER_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_CONTROLLER_PORT);

        Self {
            runtime,
            image,
            bootstrap_servers,
            bootstrap_addrs,
            kafka_home,
            cluster_size,
            controller_port,
        }
    }

    pub fn client_config(&self) -> ClusterConfig {
        ClusterConfig {
            bootstrap_servers: self.bootstrap_addrs.clone(),
            security_protocol: SecurityProtocol::Plaintext,
            client_id: "integration-test".to_string(),
            metadata_ttl: Duration::from_secs(10),
        }
    }

    /// Build KafkaClient using new builder API
    pub async fn build_client(&self) -> KafkaClient {
        KafkaClient::builder(self.bootstrap_addrs.clone())
            .with_client_id("integration-test")
            .with_metadata_ttl(Duration::from_secs(10))
            .build()
            .await
            .expect("failed to build KafkaClient")
    }

    /// 返回首个 bootstrap 地址（兼容单点模式）。
    pub fn first_bootstrap(&self) -> &str {
        &self.bootstrap_servers[0]
    }

    pub fn is_cluster(&self) -> bool {
        self.cluster_size > 1 || self.bootstrap_addrs.len() > 1
    }
}

// ============================================================================
// KafkaInstance — 管理 Kafka broker 生命周期
// ============================================================================

/// Kafka broker/集群实例。
///
/// 启动时根据配置自动选择后端：
/// - Podman 容器：启动单点 KRaft broker
/// - Docker 容器：同上（使用 docker CLI）
/// - Direct JVM：调用 `tests/kafka/bin/kafka-server-start.sh`
/// - External：不管理生命周期，连接外部 Kafka
///
/// Drop 时自动停止和清理本地启动的资源。
pub struct KafkaInstance {
    config: TestConfig,
    backend: BackendKind,
}

#[allow(dead_code)]
enum BackendKind {
    None,
    Podman { container_id: String },
    Docker { container_id: String },
    Direct { child: Child, work_dir: PathBuf },
}

impl KafkaInstance {
    /// 启动 Kafka 实例（兼容旧接口，默认单点）。
    pub async fn start() -> Self {
        Self::start_with(TestConfig::from_env()).await
    }

    /// 使用指定配置启动。
    pub async fn start_with(mut config: TestConfig) -> Self {
        // 当用户没有显式指定 KAFKA_BOOTSTRAP 且不是 external 模式时，
        // 自动分配随机空闲端口，避免并行测试互相抢占 29092。
        let bootstrap_from_env = std::env::var("KAFKA_BOOTSTRAP").is_ok();
        if config.runtime != RuntimeBackend::External && !bootstrap_from_env {
            let broker_port = find_free_port();
            config.controller_port = broker_port + 1;
            let bootstrap = format!("127.0.0.1:{}", broker_port);
            config.bootstrap_servers = vec![bootstrap.clone()];
            config.bootstrap_addrs = vec![
                bootstrap
                    .parse()
                    .expect("generated random bootstrap address must be valid"),
            ];
        }

        println!(
            "=== KafkaInstance: starting (runtime={:?}, bootstrap={:?}, cluster_size={}) ===",
            config.runtime, config.bootstrap_servers, config.cluster_size
        );

        let backend = match config.runtime {
            RuntimeBackend::External => BackendKind::None,
            RuntimeBackend::Podman => Self::start_podman(&config)
                .await
                .unwrap_or_else(|e| panic!("Podman backend requested but unavailable: {}", e)),
            RuntimeBackend::Docker => Self::start_docker(&config)
                .await
                .unwrap_or_else(|e| panic!("Docker backend requested but unavailable: {}", e)),
            RuntimeBackend::Direct => Self::start_direct(&config).await,
            RuntimeBackend::Auto => {
                // 优先 podman，其次 direct，最后 external（如果已配置）
                if let Ok(b) = Self::start_podman(&config).await {
                    b
                } else if let Ok(b) = Self::start_docker(&config).await {
                    b
                } else {
                    println!(
                        "Container backend unavailable, falling back to direct JVM or external"
                    );
                    if config.bootstrap_addrs.iter().any(|a| is_port_open(a)) {
                        println!("External Kafka already reachable, using external mode");
                        BackendKind::None
                    } else {
                        Self::start_direct(&config).await
                    }
                }
            }
        };

        println!(
            "=== KafkaInstance: ready at {:?} ===",
            config.bootstrap_servers
        );
        Self { config, backend }
    }

    // ── 容器后端 ────────────────────────────────────────────────────

    async fn start_podman(config: &TestConfig) -> Result<BackendKind, String> {
        Self::ensure_container_cli("podman")?;
        Self::ensure_image_present("podman", &config.image).await?;
        let container_id = Self::run_container("podman", config).await?;
        Self::wait_for_cluster(&config.bootstrap_addrs, config.cluster_size, 30).await;
        Ok(BackendKind::Podman { container_id })
    }

    async fn start_docker(config: &TestConfig) -> Result<BackendKind, String> {
        Self::ensure_container_cli("docker")?;
        Self::ensure_image_present("docker", &config.image).await?;
        let container_id = Self::run_container("docker", config).await?;
        Self::wait_for_cluster(&config.bootstrap_addrs, config.cluster_size, 30).await;
        Ok(BackendKind::Docker { container_id })
    }

    fn ensure_container_cli(cli: &str) -> Result<(), String> {
        let version_check = Command::new(cli)
            .args(["version", "--format", "{{.Server.Version}}"])
            .output()
            .map_err(|e| format!("{} not found: {}", cli, e))?;
        if !version_check.status.success() {
            return Err(format!("{} version check failed", cli));
        }
        let version = String::from_utf8_lossy(&version_check.stdout)
            .trim()
            .to_string();
        println!("  [{}] version={}", cli, version);
        Ok(())
    }

    async fn ensure_image_present(cli: &str, image: &str) -> Result<(), String> {
        let inspect = Command::new(cli)
            .args(["image", "inspect", image])
            .output()
            .map_err(|e| format!("{} image inspect failed: {}", cli, e))?;
        if !inspect.status.success() {
            println!("  [{}] pulling image {}...", cli, image);
            let pull = Command::new(cli)
                .args(["pull", image])
                .status()
                .map_err(|e| format!("{} pull {} failed: {}", cli, image, e))?;
            if !pull.success() {
                return Err(format!("{} pull {} failed", cli, image));
            }
        }
        Ok(())
    }

    async fn run_container(cli: &str, config: &TestConfig) -> Result<String, String> {
        let cluster_id = uuid::Uuid::new_v4().to_string();
        let container_name = format!("kafka-integration-{}", &cluster_id[..8]);
        let port = config.bootstrap_addrs[0].port();

        let output = Command::new(cli)
            .args([
                "run",
                "-d",
                "--rm",
                "--name",
                &container_name,
                "-p",
                &format!("{}:9092", port),
                "-e",
                "KAFKA_NODE_ID=1",
                "-e",
                "KAFKA_PROCESS_ROLES=broker,controller",
                "-e",
                "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
                "-e",
                &format!(
                    "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{}",
                    config.first_bootstrap()
                ),
                "-e",
                "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
                "-e",
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                "-e",
                "KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093",
                "-e",
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
                "-e",
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
                "-e",
                "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
                "-e",
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
                "-e",
                "KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS=classic",
                "-e",
                "KAFKA_AUTO_CREATE_TOPICS_ENABLE=false",
                "-e",
                &format!("CLUSTER_ID={}", cluster_id),
                &config.image,
            ])
            .output()
            .map_err(|e| format!("{} run failed: {}", cli, e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("{} run failed: {}", cli, stderr));
        }

        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        println!(
            "  [{}] container={}, name={}",
            cli, container_id, container_name
        );
        Ok(container_id)
    }

    // ── Direct JVM 后端 ────────────────────────────────────────────

    async fn start_direct(config: &TestConfig) -> BackendKind {
        let work_dir =
            std::env::temp_dir().join(format!("kafka_client_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&work_dir);
        fs::create_dir_all(&work_dir).expect("create work dir");
        let log_dir = work_dir.join("data");
        fs::create_dir_all(&log_dir).expect("create data dir");

        let server_config = work_dir.join("server.properties");
        let cfg_content = format!(
            r#"process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:{controller}
listeners=PLAINTEXT://:{broker},CONTROLLER://:{controller}
advertised.listeners=PLAINTEXT://{bootstrap}
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs={log_dir}
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
group.coordinator.rebalance.protocols=classic
auto.create.topics.enable=false
num.network.threads=2
num.io.threads=4
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.retention.hours=1
log.segment.bytes=1073741824
"#,
            broker = config.bootstrap_addrs[0].port(),
            controller = config.controller_port,
            bootstrap = config.first_bootstrap(),
            log_dir = log_dir.display(),
        );
        fs::write(&server_config, &cfg_content).expect("write server.properties");

        let uuid_out = run_kafka_script(config, "kafka-storage.sh", &["random-uuid"])
            .expect("kafka-storage.sh random-uuid");
        let cluster_id = String::from_utf8_lossy(&uuid_out.stdout).trim().to_string();
        println!("  [direct] cluster_id={}", cluster_id);

        let format_out = run_kafka_script(
            config,
            "kafka-storage.sh",
            &[
                "format",
                "-t",
                &cluster_id,
                "-c",
                &server_config.to_string_lossy(),
            ],
        )
        .expect("kafka-storage.sh format");
        if !format_out.status.success() {
            panic!(
                "kafka-storage.sh format failed: {}",
                String::from_utf8_lossy(&format_out.stderr)
            );
        }
        println!("  [direct] storage formatted");

        let log_path = work_dir.join("server.log");
        let log_file = fs::File::create(&log_path).expect("create server log");
        let child = Command::new(format!(
            "{}/bin/kafka-server-start.sh",
            config.kafka_home.display()
        ))
        .arg(&server_config)
        .stdout(log_file.try_clone().expect("clone log fd"))
        .stderr(log_file)
        .spawn()
        .expect("spawn kafka-server-start.sh");

        let backend = BackendKind::Direct { child, work_dir };

        Self::wait_for_cluster(&config.bootstrap_addrs, config.cluster_size, 60).await;
        backend
    }

    // ── 健康等待 ────────────────────────────────────────────────────

    async fn wait_for_cluster(addrs: &[SocketAddr], cluster_size: usize, max_secs: u64) {
        // 1. 等待 TCP 端口可连
        for i in 0..max_secs {
            let ready = addrs.iter().filter(|a| is_port_open(a)).count();
            if ready == addrs.len() {
                println!("  All {} bootstrap ports ready after ~{}s", ready, i + 1);
                break;
            }
            sleep(Duration::from_secs(1)).await;
        }

        // 2. 等待 Kafka API 真正可服务（metadata 可刷新）
        let api_ready = async {
            for i in 0..max_secs {
                match KafkaClient::builder(addrs.to_vec())
                    .with_client_id("kafka-instance-readiness")
                    .with_metadata_ttl(Duration::from_secs(10))
                    .build()
                    .await
                {
                    Ok(client) => {
                        if client.cluster().refresh_metadata().await.is_ok() {
                            let _ = client.close().await;
                            println!("  Kafka broker API ready after ~{}s", i + 1);
                            return true;
                        }
                        let _ = client.close().await;
                    }
                    Err(e) => {
                        println!("  Readiness check not ready yet: {}", e);
                    }
                }
                sleep(Duration::from_secs(1)).await;
            }
            false
        }
        .await;

        if !api_ready {
            println!(
                "  Warning: Kafka API readiness check timed out after {}s, continuing anyway",
                max_secs
            );
        }

        // 3. 给 KRaft 选举/集群元数据同步留出时间
        let settle = if cluster_size > 1 { 3 } else { 1 };
        sleep(Duration::from_secs(settle)).await;
    }

    /// 获取首个 bootstrap server 地址字符串（兼容旧接口）。
    pub fn bootstrap(&self) -> &str {
        self.config.first_bootstrap()
    }

    /// 获取所有 bootstrap 地址。
    pub fn bootstrap_servers(&self) -> &[String] {
        &self.config.bootstrap_servers
    }

    /// 获取所有 bootstrap SocketAddr。
    pub fn bootstrap_addrs(&self) -> &[SocketAddr] {
        &self.config.bootstrap_addrs
    }

    pub fn client_config(&self) -> ClusterConfig {
        self.config.client_config()
    }

    /// Build KafkaClient using new builder API
    pub async fn build_client(&self) -> KafkaClient {
        self.config.build_client().await
    }

    pub fn config(&self) -> &TestConfig {
        &self.config
    }

    fn cleanup(&mut self) {
        match &mut self.backend {
            BackendKind::None => {}
            BackendKind::Podman { container_id } => {
                println!("  Stopping podman container {}", container_id);
                let _ = Command::new("podman")
                    .args(["stop", "-t", "5", container_id])
                    .status();
            }
            BackendKind::Docker { container_id } => {
                println!("  Stopping docker container {}", container_id);
                let _ = Command::new("docker")
                    .args(["stop", "-t", "5", container_id])
                    .status();
            }
            BackendKind::Direct { child, work_dir } => {
                println!("  Stopping direct JVM Kafka...");
                let pid = child.id();
                let _ = Command::new("kill")
                    .args(["-TERM", &pid.to_string()])
                    .status();
                for _ in 0..25 {
                    if let Ok(Some(_)) = child.try_wait() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(200));
                }
                let _ = child.kill();
                let _ = child.wait();
                let _ = fs::remove_dir_all(work_dir);
            }
        }
    }
}

impl Drop for KafkaInstance {
    fn drop(&mut self) {
        self.cleanup();
    }
}

fn is_port_open(addr: &SocketAddr) -> bool {
    TcpStream::connect_timeout(addr, Duration::from_secs(1)).is_ok()
}

/// 找一个当前空闲的 TCP 端口（绑定后立即释放）。
fn find_free_port() -> u16 {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("failed to bind to find a free port");
    listener
        .local_addr()
        .expect("failed to get local address")
        .port()
}

// ============================================================================
// Topic helpers
// ============================================================================

/// 创建主题并等待 leader 就绪。集群模式下 replication_factor 会取 min(3, cluster_size)。
pub async fn create_topic(client: &KafkaClient, topic: &str, partitions: i32) {
    let cluster_size = std::env::var("KAFKA_CLUSTER_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let replication_factor = if cluster_size >= 3 { 3 } else { 1 };

    println!(
        "  Creating topic '{}' ({} partitions, rf={})...",
        topic, partitions, replication_factor
    );
    let request = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.to_string(),
            num_partitions: partitions,
            replication_factor,
            assignments: vec![],
            configs: vec![],
        }],
        timeout_ms: 10000,
        validate_only: false,
    };

    let response: CreateTopicsResponse = client.cluster().send_to_any_broker(&request).await.unwrap();
    for t in &response.topics {
        if t.error_code != 0 && t.error_code != 36 {
            panic!(
                "Create topic '{}' failed: error_code {} (message: {:?})",
                topic, t.error_code, t.error_message
            );
        }
    }
    println!("  Topic '{}' created", topic);

    wait_for_topic_ready(client, topic, partitions).await;
}

pub async fn wait_for_topic_ready(client: &KafkaClient, topic: &str, partitions: i32) {
    for i in 0..30 {
        client.cluster().refresh_metadata().await.unwrap();
        if let Some(tm) = client.metadata().get_topic(topic).await {
            let online = tm.partitions.iter().filter(|p| p.leader_id >= 0).count();
            if online == partitions as usize {
                println!(
                    "  Topic '{}' ready after ~{}s ({} leaders)",
                    topic,
                    i + 1,
                    online
                );
                return;
            }
            println!("  Topic '{}': {}/{} leaders", topic, online, partitions);
        }
        sleep(Duration::from_secs(1)).await;
    }
    println!("  Topic '{}' continuing optimistically after 30s", topic);
}

// ============================================================================
// Producer/Consumer helpers
// ============================================================================

pub fn default_producer_config() -> ProducerConfig {
    ProducerConfig {
        acks: 1,
        timeout_ms: 10000,
        retries: 5,
        batch_size: 16384,
        linger_ms: 50,
        routing: PartitionRouting::HashKey,
        ..Default::default()
    }
}

pub fn consumer_config(group_id: &str, reset: AutoOffsetReset) -> ConsumerConfig {
    ConsumerConfig {
        group_id: group_id.to_string(),
        auto_commit: true,
        auto_commit_interval_ms: 1000,
        auto_offset_reset: reset,
        min_bytes: 0,
        max_bytes: 1048576,
        partition_max_bytes: 1048576,
        max_wait_ms: 5000,
        session_timeout_ms: 45000,
        rebalance_timeout_ms: 60000,
        heartbeat_interval_ms: 3000,
        partition_assignment_strategy: PartitionAssignmentStrategy::Range,
    }
}

pub async fn produce_messages(
    client: &KafkaClient,
    topic: &str,
    count: i32,
) -> Vec<RecordMetadata> {
    let producer = client.producer(default_producer_config()).await.unwrap();
    let mut metas = Vec::new();
    for i in 0..count {
        let msg = ProducerRecord::new(topic, bytes::Bytes::from(format!("msg-{}", i)));
        let mut attempts = 0u32;
        loop {
            match producer.send(msg.clone()).await {
                Ok(meta) => {
                    metas.push(meta);
                    break;
                }
                Err(e) => {
                    attempts += 1;
                    if attempts >= 10 {
                        panic!("Failed to produce after {} retries: {}", attempts, e);
                    }
                    let delay_ms = 1000u64 * attempts as u64;
                    println!(
                        "    Produce attempt {} failed ({}), retry in {}ms...",
                        attempts, e, delay_ms
                    );
                    client.cluster().refresh_metadata().await.unwrap();
                    sleep(Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }
    producer.flush().await.unwrap();
    println!("  Produced {} messages to '{}'", count, topic);
    metas
}

pub async fn produce_messages_with_keys(
    client: &KafkaClient,
    topic: &str,
    count: i32,
    key_count: i32,
) -> Vec<RecordMetadata> {
    let producer = client.producer(default_producer_config()).await.unwrap();
    let mut metas = Vec::new();
    for i in 0..count {
        let key = bytes::Bytes::from(format!("key-{}", i % key_count));
        let msg =
            ProducerRecord::new(topic, bytes::Bytes::from(format!("val-{}", i))).with_key(key);
        metas.push(producer.send(msg).await.unwrap());
    }
    producer.flush().await.unwrap();
    println!("  Produced {} keyed messages to '{}'", count, topic);
    metas
}

pub async fn consume_all(
    client: &KafkaClient,
    group_id: &str,
    topic: &str,
    expected_count: i32,
) -> Vec<ConsumerRecord> {
    consume_all_timeout(
        client,
        group_id,
        topic,
        expected_count,
        Duration::from_secs(30),
    )
    .await
}

pub async fn consume_all_timeout(
    client: &KafkaClient,
    group_id: &str,
    topic: &str,
    expected_count: i32,
    timeout: Duration,
) -> Vec<ConsumerRecord> {
    let mut consumer = client.consumer(consumer_config(group_id, AutoOffsetReset::Earliest));
    consumer.subscribe(vec![topic.to_string()]).await.unwrap();

    for i in 0..20 {
        let assignment = consumer.assignment().await;
        let has_partitions: usize = assignment.values().map(|v| v.len()).sum();
        if has_partitions > 0 {
            println!(
                "  Consumer joined group after ~{}s (partitions={})",
                i + 1,
                has_partitions
            );
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }

    let mut all = Vec::new();
    let deadline = std::time::Instant::now() + timeout;
    while all.len() < expected_count as usize && std::time::Instant::now() < deadline {
        let records = consumer.poll(3000).await.unwrap();
        all.extend(records);
    }
    println!(
        "  Consumed {} messages from '{}' (expected {})",
        all.len(),
        topic,
        expected_count
    );
    assert!(
        all.len() as i32 >= expected_count,
        "Consumer '{}' got only {} messages, expected at least {}",
        group_id,
        all.len(),
        expected_count
    );
    drop(consumer);
    all
}

// ============================================================================
// Cluster helpers
// ============================================================================

/// 验证 metadata 中报告的 broker 数量不少于期望值。
pub async fn assert_cluster_size(client: &KafkaClient, expected: usize) {
    client.cluster().refresh_metadata().await.unwrap();
    let brokers = client.metadata().get_all_brokers().await;
    let actual = brokers.len();
    println!(
        "  Cluster metadata reports {} brokers (expected >= {})",
        actual, expected
    );
    assert!(
        actual >= expected,
        "Expected at least {} brokers, got {}",
        expected,
        actual
    );
}

/// 统计 metadata 中某主题每个分区的 leader 分布（按 broker id）。
pub async fn partition_leader_distribution(
    client: &KafkaClient,
    topic: &str,
) -> std::collections::HashMap<i32, Vec<i32>> {
    client.cluster().refresh_metadata().await.unwrap();
    let mut dist: std::collections::HashMap<i32, Vec<i32>> = std::collections::HashMap::new();
    if let Some(tm) = client.metadata().get_topic(topic).await {
        for p in &tm.partitions {
            dist.entry(p.leader_id).or_default().push(p.partition_index);
        }
    }
    dist
}

/// 等待 metadata 中某主题的某个分区重新选举出新的 leader（leader_id 改变）。
pub async fn wait_for_new_leader(
    client: &KafkaClient,
    topic: &str,
    partition: i32,
    old_leader: i32,
    max_secs: u64,
) -> Option<i32> {
    for _ in 0..max_secs {
        client.cluster().refresh_metadata().await.unwrap();
        if let Some(tm) = client.metadata().get_topic(topic).await {
            if let Some(p) = tm
                .partitions
                .iter()
                .find(|p| p.partition_index == partition)
            {
                if p.leader_id != old_leader && p.leader_id >= 0 {
                    println!(
                        "  Partition {}/{} leader changed: {} -> {}",
                        topic, partition, old_leader, p.leader_id
                    );
                    return Some(p.leader_id);
                }
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
    None
}

// ============================================================================
// Internal helpers
// ============================================================================

fn run_kafka_script(
    config: &TestConfig,
    script: &str,
    args: &[&str],
) -> Result<std::process::Output, std::io::Error> {
    Command::new(format!("{}/bin/{}", config.kafka_home.display(), script))
        .args(args)
        .output()
}
