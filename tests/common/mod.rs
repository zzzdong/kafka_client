#![allow(dead_code)]
//! Kafka 集成测试公共基础设施
//!
//! # 集群管理
//!
//! 每个测试函数通过调用 `compose::ensure()` 声明需要的集群类型：
//!
//! ```ignore
//! use common::compose;
//!
//! #[tokio::test]
//! async fn test_foo() {
//!     compose::ensure(&compose::clusters::SINGLE_NODE).await;
//!     // ...
//! }
//! ```
//!
//! 如果环境变量 `KAFKA_BOOTSTRAP` 已设置（由 `run-all-tests.sh` 管理），
//! 则跳过 compose 操作，直接使用已运行的集群。

pub mod compose;

use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

use kafka_client::{
    AutoOffsetReset, Client, ConsumerConfig, ConsumerRecord, ProducerConfig, ProducerRecord,
    RecordMetadata, admin::NewTopic,
};

/// 获取 bootstrap 地址。从环境变量读取，回退到默认值。
fn bootstrap_addrs() -> Vec<SocketAddr> {
    let servers = std::env::var("KAFKA_BOOTSTRAP")
        .unwrap_or_else(|_| "127.0.0.1:29093,127.0.0.1:29095,127.0.0.1:29097".to_string());
    servers
        .split(',')
        .map(|s| s.trim().parse().expect("Invalid KAFKA_BOOTSTRAP address"))
        .collect()
}

pub fn cluster_size() -> usize {
    std::env::var("KAFKA_CLUSTER_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3)
}

pub async fn build_test_client() -> Client {
    Client::builder(bootstrap_addrs())
        .with_client_id("integration-test")
        .with_metadata_ttl(Duration::from_secs(10))
        .build()
        .await
        .expect("failed to build Client")
}

// ============================================================================
// Topic helpers
// ============================================================================

pub async fn create_topic(client: &Client, topic: &str, partitions: i32) {
    let replication_factor = if cluster_size() >= 3 { 3 } else { 1 };

    println!(
        "  Creating topic '{}' ({} partitions, rf={})...",
        topic, partitions, replication_factor
    );

    let result = client
        .admin()
        .create_topic(&NewTopic::new(topic, partitions, replication_factor as i16))
        .await
        .unwrap();

    if !result.is_success() && !result.already_exists() {
        panic!(
            "Create topic '{}' failed: error_code {} (message: {:?})",
            topic, result.error_code, result.error_message
        );
    }
    println!("  Topic '{}' created", topic);

    wait_for_topic_ready(client, topic, partitions).await;
}

pub async fn wait_for_topic_ready(client: &Client, topic: &str, partitions: i32) {
    for attempt in 0..30 {
        client.refresh_metadata().await.unwrap();
        if let Some(tm) = client.metadata().get_topic(topic).await {
            let online = tm.partitions.iter().filter(|p| p.leader_id >= 0).count();
            if online == partitions as usize {
                println!(
                    "  Topic '{}' ready after ~{}s ({} leaders)",
                    topic,
                    attempt + 1,
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
    ProducerConfig::new()
        .with_timeout(10000)
        .with_retries(5)
        .with_linger(50)
}

pub fn consumer_config(group_id: &str, reset: AutoOffsetReset) -> ConsumerConfig {
    ConsumerConfig::new(group_id)
        .with_auto_commit_interval(Duration::from_secs(1))
        .with_auto_offset_reset(reset)
        .with_min_bytes(0)
        .with_max_bytes(1048576)
        .with_max_wait(Duration::from_secs(5))
}

pub async fn produce_messages(client: &Client, topic: &str, count: i32) -> Vec<RecordMetadata> {
    let config = ProducerConfig::new()
        .with_timeout(15000)
        .with_retries(10)
        .with_linger(1); // minimum positive linger (interval requires > 0)

    let producer = client.producer(config).await;

    let mut metadatas = Vec::with_capacity(count as usize);
    for i in 0..count {
        let record = ProducerRecord::new(topic, bytes::Bytes::from(format!("msg-{}", i)));
        // Library handles retries internally via send_single
        let meta = producer
            .send(record)
            .await
            .expect(&format!("Failed to produce msg-{}", i));
        metadatas.push(meta);
    }
    producer.flush().await.unwrap();
    drop(producer);
    println!("  Produced {} messages to '{}'", count, topic);
    metadatas
}

pub async fn produce_messages_with_keys(
    client: &Client,
    topic: &str,
    count: i32,
    key_count: i32,
) -> Vec<RecordMetadata> {
    let config = ProducerConfig::new()
        .with_timeout(15000)
        .with_retries(10)
        .with_linger(1);
    let producer = client.producer(config).await;
    let mut metadatas = Vec::with_capacity(count as usize);
    for i in 0..count {
        let key = bytes::Bytes::from(format!("key-{}", i % key_count));
        let record =
            ProducerRecord::new(topic, bytes::Bytes::from(format!("val-{}", i))).with_key(key);
        // Library handles retries internally via send_single
        let meta = producer
            .send(record)
            .await
            .expect(&format!("Failed to produce keyed msg-{}", i));
        metadatas.push(meta);
    }
    producer.flush().await.unwrap();
    drop(producer);
    println!("  Produced {} keyed messages to '{}'", count, topic);
    metadatas
}

pub async fn consume_all(
    client: &Client,
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
    client: &Client,
    group_id: &str,
    topic: &str,
    expected_count: i32,
    timeout: Duration,
) -> Vec<ConsumerRecord> {
    // 使用唯一组 ID，避免已提交偏移量干扰后续运行
    let unique_group = format!(
        "{}-{}",
        group_id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mut consumer = client.consumer(consumer_config(&unique_group, AutoOffsetReset::Earliest));
    consumer.subscribe(vec![topic.to_string()]).await.unwrap();

    // No assignment wait loop — consumer auto-joins on first poll().
    let mut all = Vec::new();
    let deadline = std::time::Instant::now() + timeout;
    while all.len() < expected_count as usize && std::time::Instant::now() < deadline {
        let records = consumer
            .poll_timeout(Duration::from_millis(3000))
            .await
            .unwrap();
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
        unique_group,
        all.len(),
        expected_count
    );
    drop(consumer);
    all
}

// ============================================================================
// Cluster helpers
// ============================================================================

pub async fn assert_cluster_size(client: &Client, expected: usize) {
    client.refresh_metadata().await.unwrap();
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

pub async fn partition_leader_distribution(
    client: &Client,
    topic: &str,
) -> std::collections::HashMap<i32, Vec<i32>> {
    client.refresh_metadata().await.unwrap();
    let mut dist: std::collections::HashMap<i32, Vec<i32>> = std::collections::HashMap::new();
    if let Some(tm) = client.metadata().get_topic(topic).await {
        for p in &tm.partitions {
            dist.entry(p.leader_id).or_default().push(p.partition_index);
        }
    }
    dist
}

pub async fn wait_for_new_leader(
    client: &Client,
    topic: &str,
    partition: i32,
    old_leader: i32,
    max_secs: u64,
) -> Option<i32> {
    for _ in 0..max_secs {
        client.refresh_metadata().await.unwrap();
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
