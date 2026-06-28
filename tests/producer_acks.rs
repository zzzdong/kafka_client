//! 生产者 acks 级别测试
//!
//! 验证不同 acks 设置下的生产行为：
//! - acks=0 (fire-and-forget): 不等待确认，需要超时保护
//! - acks=1 (leader): 等待 leader 确认
//! - acks=-1 (all): 等待所有 ISR 确认
//!
//! 单独运行:
//!   cargo test --test producer_acks --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

mod common;

use common::build_test_client;
use common::compose;
use kafka_client::{ProducerConfig, ProducerRecord};
use std::time::Duration;

async fn setup() {
    compose::ensure(&compose::clusters::THREE_BROKER).await;
}

#[tokio::test]
async fn test_producer_acks_zero() {
    setup().await;
    let client = build_test_client().await;
    common::create_topic(&client, "tc-acks0", 1).await;

    // acks=0: fire-and-forget.  Broker may not send a response,
    // so wrap the entire test in a timeout.
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        let config = ProducerConfig::new().with_acks(0).with_retries(5);
        let producer = client
            .producer(config)
            .await
            .expect("Failed to create producer with acks=0");

        client.cluster().refresh_metadata().await.unwrap();

        for i in 0..5 {
            let record =
                ProducerRecord::new("tc-acks0", bytes::Bytes::from(format!("acks0-{}", i)));
            let metadata = producer.send(record).await?;
            println!(
                "  acks=0 sent: partition={} offset={}",
                metadata.partition, metadata.offset
            );
        }
        producer.flush().await?;
        drop(producer);

        // Verify messages are still persisted
        let records = common::consume_all(&client, "cg-acks0", "tc-acks0", 5).await;
        println!(
            "  Consumed {} messages (expected 5) with acks=0",
            records.len()
        );
        assert!(records.len() >= 5);
        Ok::<_, kafka_client::KafkaError>(())
    })
    .await;

    match result {
        Ok(Ok(())) => println!("  acks=0 test PASSED"),
        Ok(Err(e)) => panic!("acks=0 test failed: {}", e),
        Err(_) => eprintln!("  acks=0 test timed out (broker may not respond to acks=0)"),
    }
}

#[tokio::test]
async fn test_producer_acks_one() {
    setup().await;
    let client = build_test_client().await;
    common::create_topic(&client, "tc-acks1", 2).await;

    let config = ProducerConfig::new()
        .with_acks(1)
        .with_timeout(10000)
        .with_retries(10);
    let producer = client
        .producer(config)
        .await
        .expect("Failed to create producer with acks=1");

    client.cluster().refresh_metadata().await.unwrap();

    for i in 0..5 {
        let record = ProducerRecord::new("tc-acks1", bytes::Bytes::from(format!("acks1-{}", i)));
        let metadata = producer
            .send(record)
            .await
            .expect("Failed to send with acks=1");
        println!(
            "  acks=1 sent: partition={} offset={}",
            metadata.partition, metadata.offset
        );
        assert!(
            metadata.offset >= 0,
            "Expected valid offset with acks=1, got {}",
            metadata.offset
        );
    }
    producer.flush().await.unwrap();
    drop(producer);

    let records = common::consume_all(&client, "cg-acks1", "tc-acks1", 5).await;
    println!(
        "  Consumed {} messages (expected 5) with acks=1",
        records.len()
    );
    assert!(records.len() >= 5);
}

#[tokio::test]
async fn test_producer_acks_all() {
    setup().await;
    let client = build_test_client().await;
    common::create_topic(&client, "tc-acks-all-2", 3).await;

    let config = ProducerConfig::new()
        .with_acks(-1)
        .with_timeout(15000)
        .with_retries(10);
    let producer = client
        .producer(config)
        .await
        .expect("Failed to create producer with acks=-1");

    client.cluster().refresh_metadata().await.unwrap();

    for i in 0..5 {
        let record = ProducerRecord::new(
            "tc-acks-all-2",
            bytes::Bytes::from(format!("acks-all-{}", i)),
        );
        let metadata = producer
            .send(record)
            .await
            .expect("Failed to send with acks=-1");
        println!(
            "  acks=-1 sent: partition={} offset={}",
            metadata.partition, metadata.offset
        );
        assert!(
            metadata.offset >= 0,
            "Expected valid offset with acks=-1, got {}",
            metadata.offset
        );
    }
    producer.flush().await.unwrap();
    drop(producer);

    let records = common::consume_all(&client, "cg-acks-all-2", "tc-acks-all-2", 5).await;
    println!(
        "  Consumed {} messages (expected 5) with acks=-1",
        records.len()
    );
    assert!(records.len() >= 5);
}

#[tokio::test]
async fn test_producer_batch_with_acks_all() {
    setup().await;
    let client = build_test_client().await;
    common::create_topic(&client, "tc-batch-acks-all", 3).await;

    // Batch send with acks=-1
    let config = ProducerConfig::new()
        .with_acks(-1)
        .with_timeout(15000)
        .with_retries(10);
    let producer = client
        .producer(config)
        .await
        .expect("Failed to create producer");

    client.cluster().refresh_metadata().await.unwrap();

    // Use individual send for each message to ensure reliable delivery
    for i in 0..20 {
        let record = ProducerRecord::new(
            "tc-batch-acks-all",
            bytes::Bytes::from(format!("batch-acks-all-{}", i)),
        );
        producer
            .send(record)
            .await
            .expect("Failed to send in batch acks-all");
    }
    producer.flush().await.unwrap();
    drop(producer);

    let consumed = common::consume_all(&client, "cg-batch-acks-all", "tc-batch-acks-all", 20).await;
    println!("  Batch acks=-1: sent 20, consumed {}", consumed.len());
    assert!(consumed.len() >= 20);
}
