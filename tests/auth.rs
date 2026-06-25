//! SASL 认证集成测试
//!
//! 验证 SASL PLAIN / SCRAM 认证机制可以正常连接并进行基本生产消费。
//!
//! # 前置条件
//!
//! ```bash
//! cd tests
//! docker compose -f docker-compose.sasl.yml up -d
//!
//! SASL_MECHANISM=PLAIN SASL_USERNAME=admin SASL_PASSWORD=admin-secret \
//!   KAFKA_BOOTSTRAP=127.0.0.1:9094 KAFKA_RUNTIME=external \
//!   cargo test --test auth --features integration_tests -- --nocapture
//! ```

#![cfg(feature = "integration_tests")]

mod common;

use kafka_client::{
    AutoOffsetReset, ConsumerConfig, KafkaClient, PartitionAssignmentStrategy, ProducerConfig,
    ProducerRecord, SaslMechanismType,
};
use std::time::Duration;

/// 从环境变量读取 SASL 配置
fn sasl_config_from_env() -> Option<(SaslMechanismType, String, String)> {
    let mechanism = match std::env::var("SASL_MECHANISM").as_deref() {
        Ok("PLAIN") => SaslMechanismType::Plain,
        Ok("SCRAM-SHA-256") => SaslMechanismType::ScramSha256,
        Ok("SCRAM-SHA-512") => SaslMechanismType::ScramSha512,
        _ => return None,
    };
    let username = std::env::var("SASL_USERNAME").unwrap_or_else(|_| "admin".to_string());
    let password = std::env::var("SASL_PASSWORD").unwrap_or_else(|_| "admin-secret".to_string());
    Some((mechanism, username, password))
}

#[tokio::test]
async fn test_sasl_authentication() {
    let Some((mechanism, username, password)) = sasl_config_from_env() else {
        eprintln!("SKIP: SASL_MECHANISM not set, skipping SASL auth test");
        return;
    };

    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9094".to_string());
    let addrs: Vec<std::net::SocketAddr> = bootstrap
        .split(',')
        .map(|s| s.trim().parse().expect("Invalid bootstrap address"))
        .collect();

    println!(
        "=== SASL Auth Test: mechanism={}, user={}, bootstrap={:?} ===",
        mechanism.as_str(),
        username,
        addrs
    );

    let client = KafkaClient::builder(addrs.clone())
        .with_client_id("sasl-auth-test")
        .with_sasl(mechanism, &username, &password)
        .with_metadata_ttl(Duration::from_secs(10))
        .build()
        .await
        .expect("Failed to build KafkaClient with SASL auth");

    // 1. 验证 metadata 可以正常获取
    client
        .cluster()
        .refresh_metadata()
        .await
        .expect("Failed to refresh metadata after SASL auth");

    let brokers = client.metadata().get_all_brokers().await;
    println!("  Metadata OK: {} broker(s) in cluster", brokers.len());
    assert!(!brokers.is_empty(), "Expected at least 1 broker");

    // 2. 基本生产消费验证
    let topic = "sasl-auth-test-topic";

    // 创建主题
    common::create_topic(&client, topic, 1).await;

    // 生产消息
    let producer = client
        .producer(ProducerConfig {
            acks: 1,
            timeout_ms: 10000,
            retries: 3,
            batch_size: 16384,
            linger_ms: 50,
            ..Default::default()
        })
        .await
        .expect("Failed to create producer");

    for i in 0..5 {
        let record = ProducerRecord::new(topic, bytes::Bytes::from(format!("sasl-msg-{}", i)));
        producer
            .send(record)
            .await
            .expect("Failed to produce message via SASL auth");
    }
    producer.flush().await.expect("Failed to flush producer");
    println!("  Produced 5 messages via SASL auth");

    // 消费消息
    let mut consumer = client.consumer(ConsumerConfig {
        group_id: "sasl-auth-test-group".to_string(),
        auto_commit: true,
        auto_commit_interval: Duration::from_secs(1),
        auto_offset_reset: AutoOffsetReset::Earliest,
        min_bytes: 1,
        max_bytes: 1048576,
        partition_max_bytes: 1048576,
        max_wait: Duration::from_secs(5),
        session_timeout: Duration::from_secs(10),
        rebalance_timeout: Duration::from_secs(30),
        heartbeat_interval: Duration::from_secs(3),
        partition_assignment_strategy: PartitionAssignmentStrategy::Range,
    });

    consumer.subscribe(vec![topic.to_string()]).await.unwrap();

    // 等待分配
    for i in 0..10 {
        let assignment = consumer.assignment().await;
        let has_partitions: usize = assignment.values().map(|v| v.len()).sum();
        if has_partitions > 0 {
            println!("  Consumer joined group after ~{}s", i + 1);
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // 消费消息
    let mut all = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while all.len() < 5 && std::time::Instant::now() < deadline {
        match consumer.poll(3000).await {
            Ok(records) => all.extend(records),
            Err(e) => eprintln!("  WARNING: Poll error: {}", e),
        }
    }

    println!("  Consumed {}/5 messages via SASL auth", all.len());
    assert!(
        all.len() >= 5,
        "Expected at least 5 messages, got {}",
        all.len()
    );

    for r in &all {
        let value = String::from_utf8_lossy(&r.value);
        println!("    {}", value);
    }

    if let Err(e) = client.close().await {
        eprintln!("  Close warning: {}", e);
    }
    println!("=== SASL Auth Test PASSED ===");
}
