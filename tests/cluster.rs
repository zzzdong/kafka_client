//! 分布式 Kafka 集群集成测试
//!
//! 这些测试默认被 Cargo 忽略（需要 `integration_tests` feature），因为：
//! - 它们要求一个多 broker 集群（通常由 Docker Compose 提供）。
//! - 它们会创建/删除主题，耗时较长，不适合作为普通 `cargo test` 的一部分。
//!
//! 运行方式：
//!   KAFKA_BOOTSTRAP=127.0.0.1:29092,127.0.0.1:29093,127.0.0.1:29094 \
//!   KAFKA_CLUSTER_SIZE=3 \
//!   KAFKA_RUNTIME=external \
//!   cargo test --test cluster --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

mod common;

use common::{
    KafkaInstance, assert_cluster_size, consume_all, create_topic, partition_leader_distribution,
    produce_messages,
};
use kafka_client::client::core::KafkaClient;
use std::sync::Arc;

#[tokio::test]
async fn test_cluster_metadata_reports_multiple_brokers() {
    let server = KafkaInstance::start().await;
    let client = KafkaClient::connect(server.client_config()).await.unwrap();

    let expected = server.config().cluster_size;
    assert_cluster_size(&client, expected).await;
}

#[tokio::test]
async fn test_cluster_produce_consume_with_replication() {
    let server = KafkaInstance::start().await;
    let client = Arc::new(KafkaClient::connect(server.client_config()).await.unwrap());

    {
        let c = client.clone();
        create_topic(&c, "tc-cluster-basic", 3).await;
    }

    produce_messages(&client, "tc-cluster-basic", 9).await;
    let records = consume_all(&client, "cg-cluster-basic", "tc-cluster-basic", 9).await;

    let mut parts: Vec<_> = records.iter().map(|r| r.partition).collect();
    parts.sort();
    parts.dedup();
    println!("  Partitions used: {:?}", parts);
    assert!(!parts.is_empty() && parts.len() <= 3);
}

#[tokio::test]
async fn test_cluster_partition_leaders_are_distributed() {
    let server = KafkaInstance::start().await;
    let client = Arc::new(KafkaClient::connect(server.client_config()).await.unwrap());

    {
        let c = client.clone();
        create_topic(&c, "tc-cluster-leaders", 6).await;
    }

    // 集群模式下，6 个分区的 leader 应当分布到多个 broker 上
    let c = client.clone();
    let dist = partition_leader_distribution(&c, "tc-cluster-leaders").await;

    println!("  Leader distribution: {:?}", dist);
    let distinct_leaders = dist.len();

    if server.config().cluster_size >= 3 {
        assert!(
            distinct_leaders >= 2,
            "Expected leaders distributed across at least 2 brokers, got {}",
            distinct_leaders
        );
    }
    assert!(
        distinct_leaders >= 1,
        "Expected at least one broker to be leader"
    );
}

#[tokio::test]
async fn test_cluster_consumer_group_with_multiple_brokers() {
    let server = KafkaInstance::start().await;
    let client = Arc::new(KafkaClient::connect(server.client_config()).await.unwrap());

    {
        let c = client.clone();
        create_topic(&c, "tc-cluster-group", 3).await;
    }

    produce_messages(&client, "tc-cluster-group", 6).await;

    let records = consume_all(&client, "cg-cluster-group", "tc-cluster-group", 6).await;
    assert!(records.len() >= 6);
}
