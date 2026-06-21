//! Kafka 集成测试公共基础设施
//!
//! 支持两种 Kafka 启动方式（自动检测）：
//! 1. Podman 容器：使用已有的 apache/kafka 镜像
//! 2. 直接 JVM 进程：使用 tests/kafka/ 下的 Kafka 发行版
//!
//! 通过环境变量控制：
//! - `KAFKA_RUNTIME` = "auto" | "podman" | "direct"（默认: "auto"）
//! - `KAFKA_IMAGE` 指定容器镜像名（默认: swr.cn-north-4...）
//! - `KAFKA_BOOTSTRAP` 指定 bootstrap 地址（默认: 127.0.0.1:29092）
//! - `KAFKA_HOME` 指定 Kafka 发行版路径（默认: tests/kafka）

use std::fs;
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

use kafka_client::client::high_level::{
    AutoOffsetReset, Consumer, ConsumerConfig, ConsumerRecord, PartitionAssignmentStrategy,
    PartitionRouting, Producer, ProducerConfig, ProducerRecord, RecordMetadata,
};
use kafka_client::client::low_level::{ClientConfig, KafkaClient};
use kafka_client::protocol::create_topics_request::CreatableTopic;
use kafka_client::protocol::{CreateTopicsRequest, CreateTopicsResponse};
use kafka_client::transport::SecurityProtocol;

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_BOOTSTRAP: &str = "127.0.0.1:29092";
const DEFAULT_CONTROLLER_PORT: u16 = 29093;

// ============================================================================
// Configuration
// ============================================================================

/// 集成测试配置，读取自环境变量
pub struct TestConfig {
    /// 运行时后端：auto / podman / direct
    pub runtime: RuntimeBackend,
    /// 容器镜像名（podman 模式）
    pub image: String,
    /// Bootstrap server 地址
    pub bootstrap: String,
    /// SocketAddr 格式的 bootstrap
    pub bootstrap_addr: std::net::SocketAddr,
    /// Kafka 发行版目录（direct 模式）
    pub kafka_home: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeBackend {
    Auto,
    Podman,
    Direct,
}

impl TestConfig {
    pub fn from_env() -> Self {
        let runtime = match std::env::var("KAFKA_RUNTIME").as_deref() {
            Ok("podman") => RuntimeBackend::Podman,
            Ok("direct") => RuntimeBackend::Direct,
            _ => RuntimeBackend::Auto,
        };

        let image = std::env::var("KAFKA_IMAGE").unwrap_or_else(|_| {
            "swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/apache/kafka:4.1.1".to_string()
        });

        let bootstrap =
            std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| DEFAULT_BOOTSTRAP.to_string());
        let bootstrap_addr = bootstrap.parse().unwrap_or_else(|e| {
            panic!("Invalid KAFKA_BOOTSTRAP={:?}: {}", bootstrap, e);
        });

        let kafka_home = std::env::var("KAFKA_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("tests")
                    .join("kafka")
            });

        Self {
            runtime,
            image,
            bootstrap,
            bootstrap_addr,
            kafka_home,
        }
    }

    pub fn client_config(&self) -> ClientConfig {
        ClientConfig {
            bootstrap_servers: vec![self.bootstrap_addr],
            security_protocol: SecurityProtocol::Plaintext,
            client_id: "integration-test".to_string(),
            metadata_ttl: Duration::from_secs(10),
        }
    }
}

// ============================================================================
// KafkaInstance — 管理 Kafka broker 生命周期
// ============================================================================

/// Kafka broker 实例。
///
/// 启动时根据配置自动选择后端：
/// - Podman 容器：`podman run ... apache/kafka:4.1.1`
/// - Direct JVM：调用 `tests/kafka/bin/kafka-server-start.sh`
///
/// Drop 时自动停止和清理。
pub struct KafkaInstance {
    config: TestConfig,
    backend: BackendKind,
}

#[allow(dead_code)]
enum BackendKind {
    Podman {
        container_id: String,
    },
    Direct {
        child: Child,
        work_dir: PathBuf,
        log_path: PathBuf,
    },
}

#[allow(dead_code)]
impl KafkaInstance {
    /// 启动 Kafka broker。根据配置和环境自动选择最优后端。
    pub async fn start() -> Self {
        let config = TestConfig::from_env();

        println!(
            "=== KafkaInstance: starting (runtime={:?}, bootstrap={}) ===",
            config.runtime, config.bootstrap
        );

        // 尝试优先选择后端
        let backend = match config.runtime {
            RuntimeBackend::Podman => Self::start_podman(&config)
                .await
                .unwrap_or_else(|e| panic!("Podman backend requested but unavailable: {}", e)),
            RuntimeBackend::Direct => Self::start_direct(&config).await,
            RuntimeBackend::Auto => {
                // 先尝试 podman，失败则降级到 direct
                match Self::start_podman(&config).await {
                    Ok(b) => b,
                    Err(e) => {
                        println!(
                            "Podman backend unavailable ({}), falling back to direct JVM",
                            e
                        );
                        Self::start_direct(&config).await
                    }
                }
            }
        };

        println!("=== KafkaInstance: ready at {} ===", config.bootstrap);
        Self { config, backend }
    }

    // ── Podman 后端 ────────────────────────────────────────────────

    async fn start_podman(config: &TestConfig) -> Result<BackendKind, String> {
        // 检查 podman 是否可用
        let version_check = Command::new("podman")
            .args(["version", "--format", "{{.Version}}"])
            .output()
            .map_err(|e| format!("podman not found: {}", e))?;
        if !version_check.status.success() {
            return Err("podman version check failed".to_string());
        }
        let version = String::from_utf8_lossy(&version_check.stdout)
            .trim()
            .to_string();
        println!("  [podman] version={}", version);

        // 检查镜像是否存在
        let inspect = Command::new("podman")
            .args(["image", "exists", &config.image])
            .status()
            .map_err(|e| format!("podman image check failed: {}", e))?;
        if !inspect.success() {
            return Err(format!(
                "Image '{}' not found locally. Pull with: podman pull {}",
                config.image, config.image
            ));
        }

        // 生成集群 ID
        let cluster_id = uuid::Uuid::new_v4().to_string();
        let container_name = format!("kafka-integration-{}", &cluster_id[..8]);

        // 启动容器
        let output = Command::new("podman")
            .args([
                "run",
                "-d",
                "--rm",
                "--name",
                &container_name,
                "-p",
                &format!("{}:9092", config.bootstrap_addr.port()),
                "-e",
                &format!("KAFKA_NODE_ID=1"),
                "-e",
                "KAFKA_PROCESS_ROLES=broker,controller",
                "-e",
                "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
                "-e",
                &format!(
                    "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{}",
                    config.bootstrap
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
            .map_err(|e| format!("podman run failed: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("podman run failed: {}", stderr));
        }

        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        println!(
            "  [podman] container={}, name={}",
            container_id, container_name
        );

        // 等待就绪
        Self::wait_for_port(config.bootstrap_addr.port(), 30);
        // 额外等待 KRaft 选举完成
        sleep(Duration::from_secs(5)).await;

        Ok(BackendKind::Podman { container_id })
    }

    // ── Direct JVM 后端 ────────────────────────────────────────────

    async fn start_direct(config: &TestConfig) -> BackendKind {
        let work_dir =
            std::env::temp_dir().join(format!("kafka_client_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&work_dir);
        fs::create_dir_all(&work_dir).expect("create work dir");
        let log_dir = work_dir.join("data");
        fs::create_dir_all(&log_dir).expect("create data dir");

        // 生成 server.properties
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
            broker = config.bootstrap_addr.port(),
            controller = DEFAULT_CONTROLLER_PORT,
            bootstrap = config.bootstrap,
            log_dir = log_dir.display(),
        );
        fs::write(&server_config, &cfg_content).expect("write server.properties");

        // 格式化存储
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

        // 启动 Kafka 服务器
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

        let instance = BackendKind::Direct {
            child,
            work_dir,
            log_path,
        };

        Self::wait_for_port(config.bootstrap_addr.port(), 60);
        // KRaft 选举
        sleep(Duration::from_secs(3)).await;

        instance
    }

    /// 等待 TCP 端口可连接
    fn wait_for_port(port: u16, max_secs: u64) {
        let addr: std::net::SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        for i in 0..max_secs {
            if TcpStream::connect_timeout(&addr, Duration::from_secs(1)).is_ok() {
                println!("  Port {} ready after ~{}s", port, i + 1);
                return;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        println!("  Port {} not ready after {}s (continuing)", port, max_secs);
    }

    /// 获取 bootstrap server 地址
    pub fn bootstrap(&self) -> &str {
        &self.config.bootstrap
    }

    pub fn client_config(&self) -> ClientConfig {
        self.config.client_config()
    }

    /// 获取配置引用
    pub fn config(&self) -> &TestConfig {
        &self.config
    }

    /// 清理子进程/容器
    fn cleanup(&mut self) {
        match &mut self.backend {
            BackendKind::Podman { container_id } => {
                println!("  Stopping podman container {}", container_id);
                let _ = Command::new("podman")
                    .args(["stop", "-t", "5", container_id])
                    .status();
            }
            BackendKind::Direct {
                child, work_dir, ..
            } => {
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

// ============================================================================
// Topic helpers
// ============================================================================

/// 创建主题并等待 leader 就绪
pub async fn create_topic(client: &mut KafkaClient, topic: &str, partitions: i32) {
    println!(
        "  Creating topic '{}' ({} partitions)...",
        topic, partitions
    );
    let request = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.to_string(),
            num_partitions: partitions,
            replication_factor: 1,
            assignments: vec![],
            configs: vec![],
        }],
        timeout_ms: 10000,
        validate_only: false,
    };

    let addr = client.any_broker_address().unwrap();
    let response: CreateTopicsResponse = client.send_request(addr, 19, &request).await.unwrap();
    for t in &response.topics {
        if t.error_code != 0 && t.error_code != 36 {
            // 36 = TOPIC_ALREADY_EXISTS
            panic!(
                "Create topic '{}' failed: error_code {} (message: {:?})",
                topic, t.error_code, t.error_message
            );
        }
    }
    println!("  Topic '{}' created", topic);

    // 等待 leader 选举完成
    for i in 0..15 {
        let meta = client.refresh_metadata().await.unwrap();
        let topic_meta = meta
            .topics
            .iter()
            .find(|t| t.name.as_deref() == Some(topic));
        if let Some(tm) = topic_meta {
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
    println!("  Topic '{}' continuing optimistically after 15s", topic);
}

// ============================================================================
// Producer/Consumer helpers
// ============================================================================

/// 默认生产者配置
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

/// 消费者配置
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

/// 生产指定数量的消息（自动重试）
pub async fn produce_messages(
    client: &Arc<Mutex<KafkaClient>>,
    topic: &str,
    count: i32,
) -> Vec<RecordMetadata> {
    let producer = Producer::new(client.clone(), default_producer_config()).await;
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
                    {
                        let mut c = client.lock().await;
                        let _ = c.refresh_metadata().await;
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }
    producer.flush().await;
    println!("  Produced {} messages to '{}'", count, topic);
    metas
}

/// 生产带 key 的消息
pub async fn produce_messages_with_keys(
    client: &Arc<Mutex<KafkaClient>>,
    topic: &str,
    count: i32,
    key_count: i32,
) -> Vec<RecordMetadata> {
    let producer = Producer::new(client.clone(), default_producer_config()).await;
    let mut metas = Vec::new();
    for i in 0..count {
        let key = bytes::Bytes::from(format!("key-{}", i % key_count));
        let msg =
            ProducerRecord::new(topic, bytes::Bytes::from(format!("val-{}", i))).with_key(key);
        metas.push(producer.send(msg).await.unwrap());
    }
    producer.flush().await;
    println!("  Produced {} keyed messages to '{}'", count, topic);
    metas
}

/// 消费所有消息（使用 auto_offset_reset=Earliest 的独立消费者组）
pub async fn consume_all(
    client: &Arc<Mutex<KafkaClient>>,
    group_id: &str,
    topic: &str,
    expected_count: i32,
) -> Vec<ConsumerRecord> {
    let mut consumer = Consumer::new(
        client.clone(),
        consumer_config(group_id, AutoOffsetReset::Earliest),
    )
    .await;
    consumer.subscribe(vec![topic.to_string()]).await.unwrap();

    // 轮询等待消费者组加入完成（check assignment every 1s, max 20s）
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
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
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
