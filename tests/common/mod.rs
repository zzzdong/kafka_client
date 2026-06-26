#![allow(dead_code)]
//! Kafka 集成测试公共基础设施
//!
//! 集群由外部管理（run-all-tests 脚本或 CI），测试只做连接。
//!
//! 环境变量：
//!   KAFKA_BOOTSTRAP    逗号分隔 bootstrap 地址（默认: 127.0.0.1:29093,29095,29097）
//!   KAFKA_CLUSTER_SIZE 集群 broker 数量（默认: 3）

use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

use kafka_client::protocol::create_topics_request::CreatableTopic;
use kafka_client::protocol::{CreateTopicsRequest, CreateTopicsResponse};
use kafka_client::{
    AutoOffsetReset, ConsumerConfig, ConsumerRecord, KafkaClient, ProducerConfig, ProducerRecord,
    RecordMetadata,
};

const DEFAULT_BOOTSTRAP: &str = "127.0.0.1:29093,127.0.0.1:29095,127.0.0.1:29097";

fn bootstrap_addrs() -> Vec<SocketAddr> {
    let servers =
        std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| DEFAULT_BOOTSTRAP.to_string());
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

pub async fn build_test_client() -> KafkaClient {
    KafkaClient::builder(bootstrap_addrs())
        .with_client_id("integration-test")
        .with_metadata_ttl(Duration::from_secs(10))
        .build()
        .await
        .expect("failed to build KafkaClient")
}

// ============================================================================
// Topic helpers
// ============================================================================

pub async fn create_topic(client: &KafkaClient, topic: &str, partitions: i32) {
    let sz = cluster_size();
    let replication_factor = if sz >= 3 { 3 } else { 1 };

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

    let response: CreateTopicsResponse =
        client.cluster().send_to_any_broker(&request).await.unwrap();
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

pub async fn produce_messages(
    client: &KafkaClient,
    topic: &str,
    count: i32,
) -> Vec<RecordMetadata> {
    let producer = client.producer(default_producer_config()).await.unwrap();

    // Batch all messages to avoid per-message network round-trips.
    // Individual send() for 100 messages can take 60s+ with linger delays.
    let records: Vec<ProducerRecord> = (0..count)
        .map(|i| ProducerRecord::new(topic, bytes::Bytes::from(format!("msg-{}", i))))
        .collect();
    let sent = producer
        .send_batch(records)
        .await
        .expect("Failed to buffer batch");
    assert_eq!(
        sent as usize, count as usize,
        "send_batch returned wrong count"
    );
    producer.flush().await.unwrap();
    println!("  Produced {} messages to '{}'", count, topic);
    Vec::new()
}

pub async fn produce_messages_with_keys(
    client: &KafkaClient,
    topic: &str,
    count: i32,
    key_count: i32,
) -> Vec<RecordMetadata> {
    let producer = client.producer(default_producer_config()).await.unwrap();
    let records: Vec<ProducerRecord> = (0..count)
        .map(|i| {
            let key = bytes::Bytes::from(format!("key-{}", i % key_count));
            ProducerRecord::new(topic, bytes::Bytes::from(format!("val-{}", i))).with_key(key)
        })
        .collect();
    let sent = producer.send_batch(records).await.unwrap();
    assert_eq!(sent as usize, count as usize);
    producer.flush().await.unwrap();
    println!("  Produced {} keyed messages to '{}'", count, topic);
    Vec::new()
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
