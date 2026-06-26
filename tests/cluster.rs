//! 分布式 Kafka 集群集成测试
//!
//! 这些测试默认被 Cargo 忽略（需要 `integration_tests` feature），因为：
//! - 它们要求一个多 broker 集群（通常由 Docker Compose 提供）。
//! - 它们会创建/删除主题，耗时较长，不适合作为普通 `cargo test` 的一部分。
//!
//! 运行方式：
//!   cargo test --test cluster --features integration_tests -- --nocapture
//! （需要先通过 `cd tests && docker compose up -d` 启动 3-broker 集群）

#![cfg(feature = "integration_tests")]

mod common;

use common::{
    assert_cluster_size, build_test_client, cluster_size, consume_all, create_topic,
    partition_leader_distribution, produce_messages,
};

#[tokio::test]
async fn test_cluster_metadata_reports_multiple_brokers() {
    let client = build_test_client().await;

    let expected = cluster_size();
    assert_cluster_size(&client, expected).await;
}

#[tokio::test]
async fn test_cluster_produce_consume_with_replication() {
    let client = build_test_client().await;

    create_topic(&client, "tc-cluster-basic", 3).await;
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
    let client = build_test_client().await;

    create_topic(&client, "tc-cluster-leaders", 6).await;

    // 集群模式下，6 个分区的 leader 应当分布到多个 broker 上
    let dist = partition_leader_distribution(&client, "tc-cluster-leaders").await;

    println!("  Leader distribution: {:?}", dist);
    let distinct_leaders = dist.len();

    if cluster_size() >= 3 {
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
    let client = build_test_client().await;

    create_topic(&client, "tc-cluster-group", 3).await;
    produce_messages(&client, "tc-cluster-group", 6).await;

    let records = consume_all(&client, "cg-cluster-group", "tc-cluster-group", 6).await;
    assert!(records.len() >= 6);
}
