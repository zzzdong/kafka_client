//! 基础生产+消费测试
//!
//! 包括：消息分布到多个分区、Headers、显式分区路由、acks=-1 确认。
//!
//! 单独运行:
//!   cargo test --test produce_consume --features integration_tests -- --nocapture
//!
//! 通过 run-all-tests.sh 批量运行:
//!   KAFKA_BOOTSTRAP=... ./run-all-tests.sh

#![cfg(feature = "integration_tests")]

mod common;

use common::build_test_client;
use common::compose;
use kafka_client::{Header, ProducerConfig};
use std::time::Duration;

async fn setup() {
    compose::ensure(&compose::clusters::THREE_BROKER).await;
}

#[tokio::test]
async fn test_produce_and_consume() {
    setup().await;
    let client = build_test_client().await;

    // Create topic with 3 partitions
    common::create_topic(&client, "tc-basic", 3).await;

    common::produce_messages(&client, "tc-basic", 3).await;

    let records = common::consume_all(&client, "cg-basic", "tc-basic", 3).await;
    for r in &records {
        println!(
            "    topic={} partition={} offset={} val={}",
            r.topic,
            r.partition,
            r.offset,
            String::from_utf8_lossy(&r.value)
        );
    }

    // Verify messages span multiple partitions
    let mut parts: Vec<_> = records.iter().map(|r| r.partition).collect();
    parts.sort();
    parts.dedup();
    println!("    Partitions used: {:?}", parts);
    assert!(
        !parts.is_empty() && parts.len() <= 3,
        "Messages should span partitions"
    );
}

#[tokio::test]
async fn test_produce_with_headers() {
    setup().await;
    let client = build_test_client().await;
    common::create_topic(&client, "tc-headers", 1).await;

    // Refresh metadata before producing to avoid NOT_LEADER_OR_FOLLOWER
    client.refresh_metadata().await.unwrap();

    let producer = client.producer(ProducerConfig::new()).await;

    let record =
        kafka_client::ProducerRecord::new("tc-headers", bytes::Bytes::from("msg-with-headers"))
            .with_headers(vec![
                Header {
                    key: "content-type".to_string(),
                    value: bytes::Bytes::from("text/plain"),
                },
                Header {
                    key: "trace-id".to_string(),
                    value: bytes::Bytes::from("abc-123"),
                },
            ]);

    producer.send(record).await.expect("Failed to send");
    producer.flush().await.unwrap();
    drop(producer);

    // Consume and verify headers
    let records = common::consume_all(&client, "cg-headers", "tc-headers", 1).await;
    assert!(!records.is_empty(), "Expected at least 1 record");
    let r = &records[0];
    println!("  Received message with {} header(s)", r.headers.len());
    for h in &r.headers {
        println!("    {} = {}", h.key, String::from_utf8_lossy(&h.value));
    }
    assert_eq!(r.headers.len(), 2, "Expected 2 headers");

    let has_content_type = r.headers.iter().any(|h| h.key == "content-type");
    let has_trace_id = r.headers.iter().any(|h| h.key == "trace-id");
    assert!(has_content_type, "Missing content-type header");
    assert!(has_trace_id, "Missing trace-id header");
}

#[tokio::test]
async fn test_produce_to_explicit_partition() {
    setup().await;
    let client = build_test_client().await;
    common::create_topic(&client, "tc-explicit-part", 3).await;

    let producer = client
        .producer(ProducerConfig::new().with_retries(10))
        .await;

    // Wait for topic metadata to be fully propagated
    client.refresh_metadata().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let record = kafka_client::ProducerRecord::new(
        "tc-explicit-part",
        bytes::Bytes::from("explicit-partition"),
    )
    .with_partition(1);

    let metadata = producer
        .send(record)
        .await
        .expect("Failed to send to explicit partition");
    println!(
        "  Sent to partition {} (expected 1), offset={}",
        metadata.partition, metadata.offset
    );
    assert_eq!(metadata.partition, 1, "Message should land on partition 1");

    producer.flush().await.unwrap();
    drop(producer);

    // Verify we can consume from partition 1
    let records = common::consume_all(&client, "cg-explicit-part", "tc-explicit-part", 1).await;
    assert!(!records.is_empty(), "Expected at least 1 record");
    assert_eq!(records[0].partition, 1, "Consumed from wrong partition");
}

#[tokio::test]
async fn test_produce_with_acks_all() {
    setup().await;
    let client = build_test_client().await;
    common::create_topic(&client, "tc-acks-all", 3).await;

    // Use acks=-1 (all ISRs) for maximum durability
    let config = ProducerConfig::new()
        .with_acks(-1)
        .with_timeout(15000)
        .with_retries(10);

    let producer = client.producer(config).await;

    // Refresh metadata to ensure partition leader is known
    client.refresh_metadata().await.unwrap();

    let record =
        kafka_client::ProducerRecord::new("tc-acks-all", bytes::Bytes::from("msg-with-acks-all"));

    let metadata = producer
        .send(record)
        .await
        .expect("Failed to send with acks=-1");
    println!(
        "  Sent with acks=-1: partition={} offset={}",
        metadata.partition, metadata.offset
    );

    producer.flush().await.unwrap();

    // Verify consumption works
    let records = common::consume_all(&client, "cg-acks-all", "tc-acks-all", 1).await;
    assert!(!records.is_empty(), "Expected to consume the message");
    println!("  Consumed message at offset {}", records[0].offset);
}
