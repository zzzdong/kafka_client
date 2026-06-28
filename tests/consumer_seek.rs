//! 消费者偏移量 Seek 测试
//!
//! 验证消费者可以手动 Seek 到指定偏移量，重新消费历史消息。
//! 需要 3-broker 集群（consumer group 需要 coordinator）。
//!
//! 单独运行:
//!   cargo test --test consumer_seek --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

mod common;

use common::build_test_client;
use common::compose;
use kafka_client::{AutoOffsetReset, ConsumerConfig};
use std::time::Duration;
use tokio::time::sleep;

async fn setup() {
    compose::ensure(&compose::clusters::THREE_BROKER).await;
}

/// 创建一个唯一组 ID，避免跨测试运行的已提交偏移量干扰。
fn unique_group_id(prefix: &str) -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}-{}", prefix, ts)
}

#[tokio::test]
async fn test_consumer_seek_to_earliest() {
    setup().await;
    let client = build_test_client().await;

    common::create_topic(&client, "tc-seek", 2).await;
    common::produce_messages(&client, "tc-seek", 10).await;

    // Give metadata time to settle before consumer starts
    client.cluster().refresh_metadata().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    let group_id = unique_group_id("cg-seek-test");
    let seek_client = build_test_client().await;
    let mut consumer = seek_client.consumer(
        ConsumerConfig::new(&group_id)
            .with_auto_commit(false)
            .with_auto_offset_reset(AutoOffsetReset::Latest),
    );

    consumer
        .subscribe(vec!["tc-seek".to_string()])
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let a = consumer.group().assignment().await;
        let total: usize = a.values().map(|v| v.len()).sum();
        if total > 0 {
            println!("  Consumer joined group (group={})", group_id);
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("Consumer failed to join group within 30s");
        }
        sleep(Duration::from_secs(1)).await;
    }

    // Small extra delay so the reactor can resolve Latest offsets
    sleep(Duration::from_millis(500)).await;

    let records = consumer
        .poll_timeout(Duration::from_secs(3))
        .await
        .expect("First poll failed");
    println!(
        "  Latest consumer got {} messages (expected 0)",
        records.len()
    );
    // Latest consumer should ideally get 0 messages — if it got some
    // it means offset resolution is slow; fast-forward past them.
    if !records.is_empty() {
        let assignment = consumer.group().assignment().await;
        for (topic, partitions) in &assignment {
            for &p in partitions {
                let max_offset = records
                    .iter()
                    .filter(|r| r.topic == *topic && r.partition == p)
                    .map(|r| r.offset)
                    .max()
                    .unwrap_or(0)
                    + 1;
                consumer.offsets().set(topic, p, max_offset).await;
                println!("  Fast-forwarded {}/{} to offset {}", topic, p, max_offset);
            }
        }
        sleep(Duration::from_secs(1)).await;
        let empty = consumer
            .poll_timeout(Duration::from_secs(2))
            .await
            .expect("Second poll failed");
        println!("  After fast-forward: got {} messages", empty.len());
    }

    // Seek 各分区到 offset=0
    let assignment = consumer.group().assignment().await;
    for (topic, partitions) in &assignment {
        for &p in partitions {
            consumer.offsets().set(topic, p, 0).await;
            println!("  Seeked {}/{} to offset 0", topic, p);
        }
    }

    // 重新消费 - 应获取全部 10 条消息
    let mut all = Vec::new();
    let consume_deadline = std::time::Instant::now() + Duration::from_secs(20);
    while all.len() < 10 && std::time::Instant::now() < consume_deadline {
        let records = consumer
            .poll_timeout(Duration::from_millis(3000))
            .await
            .unwrap();
        let count = records.len();
        all.extend(records);
        if count > 0 {
            println!("  Poll returned {} messages (total: {})", count, all.len());
        }
    }

    println!("  Consumed {} messages after seek (expected 10)", all.len());
    assert!(
        all.len() >= 10,
        "Expected at least 10 messages after seek, got {}",
        all.len()
    );

    let mut values: Vec<_> = all
        .iter()
        .map(|r| String::from_utf8_lossy(&r.value).to_string())
        .collect();
    values.sort();
    println!("  Messages after seek (sorted): {:?}", values);
    for i in 0..10 {
        let expected = format!("msg-{}", i);
        assert!(
            values.contains(&expected),
            "Expected '{}' in consumed messages, got {:?}",
            expected,
            values
        );
    }
}
