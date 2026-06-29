//! SASL 认证集成测试
//!
//! 验证 SASL PLAIN / SCRAM 认证机制可以正常连接并进行基本生产消费。
//! 需要 SASL 单节点集群（docker-compose.sasl.yml）。
//!
//! 单独运行:
//! ```bash
//! cd tests
//! docker compose -f docker-compose.sasl.yml up -d
//! SASL_MECHANISM=PLAIN SASL_USERNAME=admin SASL_PASSWORD=admin-secret \
//!   KAFKA_BOOTSTRAP_SASL=127.0.0.1:9094 \
//!   cargo test --test auth --features integration_tests -- --nocapture
//! ```
//!
//! 自动运行（会尝试自动启动集群）:
//!   cargo test --test auth --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

mod common;

use common::compose;
use kafka_client::{
    Client, ConsumerConfig, ProducerConfig, ProducerRecord, SaslMechanismType, admin::NewTopic,
};
use std::time::Duration;

/// 确保 SASL 集群已就绪（如果环境变量未设置则自动启动）
async fn setup() {
    compose::ensure(&compose::clusters::SASL).await;
}

/// SASL broker bootstrap address
fn sasl_bootstrap_addrs() -> Vec<std::net::SocketAddr> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP_SASL")
        .or_else(|_| std::env::var("KAFKA_BOOTSTRAP"))
        .unwrap_or_else(|_| "127.0.0.1:9094".to_string());
    bootstrap
        .split(',')
        .map(|s| s.trim().parse().expect("Invalid bootstrap address"))
        .collect()
}

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
    setup().await;
    let Some((mechanism, username, password)) = sasl_config_from_env() else {
        eprintln!("SKIP: SASL_MECHANISM not set, skipping SASL auth test");
        return;
    };

    let addrs = sasl_bootstrap_addrs();

    println!(
        "=== SASL Auth Test: mechanism={}, user={}, bootstrap={:?} ===",
        mechanism.as_str(),
        username,
        addrs
    );

    let client = Client::builder(addrs.clone())
        .with_client_id("sasl-auth-test")
        .with_sasl(mechanism, &username, &password)
        .with_metadata_ttl(Duration::from_secs(10))
        .build()
        .await
        .expect("Failed to build Client with SASL auth");

    // 1. 验证 metadata 可以正常获取
    client
        .refresh_metadata()
        .await
        .expect("Failed to refresh metadata after SASL auth");

    let brokers = client.metadata().get_all_brokers().await;
    println!("  Metadata OK: {} broker(s) in cluster", brokers.len());
    assert!(!brokers.is_empty(), "Expected at least 1 broker");

    // 2. 基本生产消费验证
    let topic = "sasl-auth-test-topic";

    // 创建主题（SASL 是单节点集群，rf=1）
    let result = client
        .admin()
        .create_topic(&NewTopic::new(topic, 1, 1))
        .await
        .unwrap();
    assert!(
        result.is_success() || result.already_exists(),
        "Create topic failed: {:?}",
        result.error_message
    );
    println!("  Topic '{}' created (rf=1)", topic);
    common::wait_for_topic_ready(&client, topic, 1).await;

    // 生产消息
    let producer = client.producer(ProducerConfig::new()).await;

    for i in 0..5 {
        let record = ProducerRecord::new(topic, bytes::Bytes::from(format!("sasl-msg-{}", i)));
        producer
            .send(record)
            .await
            .expect("Failed to produce message via SASL auth");
    }
    producer.flush().await.expect("Failed to flush producer");
    println!("  Produced 5 messages via SASL auth");

    // 消费消息（从最早偏移开始，避免错过已生产的消息）
    let mut consumer = client.consumer(ConsumerConfig::new("sasl-auth-test-group").with_earliest());

    consumer.subscribe(vec![topic.to_string()]).await.unwrap();

    // 等待分配
    for i in 0..10 {
        let assignment = consumer.group().assignment().await;
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
        match consumer.poll_timeout(Duration::from_millis(3000)).await {
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

#[tokio::test]
async fn test_sasl_invalid_credentials_rejected() {
    setup().await;
    // This test verifies that connecting with wrong SASL credentials
    // is gracefully rejected (not a panic or hang).
    let Some((mechanism, _username, _password)) = sasl_config_from_env() else {
        eprintln!("SKIP: SASL_MECHANISM not set, skipping invalid credential test");
        return;
    };

    let addrs = sasl_bootstrap_addrs();

    println!(
        "=== SASL Invalid Credentials Test: mechanism={}, bootstrap={:?} ===",
        mechanism.as_str(),
        addrs
    );

    let result = Client::builder(addrs.clone())
        .with_client_id("sasl-invalid-test")
        .with_sasl(mechanism, "wrong_user", "wrong_password")
        .with_metadata_ttl(Duration::from_secs(5))
        .build()
        .await;

    match result {
        Ok(_) => {
            // Some brokers might not reject at connect time (e.g., SASL/PLAIN
            // only rejects on first request). Verify metadata refresh fails.
            eprintln!("  NOTE: Build succeeded with invalid credentials, testing metadata...");
            let client = result.unwrap();
            let meta_result = client.refresh_metadata().await;
            assert!(
                meta_result.is_err(),
                "Expected metadata refresh to fail with invalid SASL credentials"
            );
            eprintln!(
                "  Metadata refresh correctly rejected: {:?}",
                meta_result.err().unwrap()
            );
            let _ = client.close().await;
        }
        Err(e) => {
            println!("  Build correctly rejected: {}", e);
        }
    }

    println!("=== SASL Invalid Credentials Test PASSED ===");
}
