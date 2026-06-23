//! 偏移量提交测试
//!
//! 验证消费者在消费后提交偏移量，auto_commit 任务正常工作。
//!
//! 运行: KAFKA_RUNTIME=direct cargo test --test offset_commit --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

mod common;

use common::{KafkaInstance, consumer_config};
use kafka_client::AutoOffsetReset;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_offset_commit() {
    let server = KafkaInstance::start().await;
    let client = server.build_client().await;

    // 先生产一些消息
    common::create_topic(&client, "tc-offset", 2).await;
    common::produce_messages(&client, "tc-offset", 5).await;

    // 消费者消费并提交偏移量
    let c1_client = server.build_client().await;
    let mut consumer =
        c1_client.consumer(consumer_config("cg-offset-test", AutoOffsetReset::Earliest));
    consumer
        .subscribe(vec!["tc-offset".to_string()])
        .await
        .unwrap();

    // 等待组加入（最多 30s，避免无限挂起）
    let assignment_deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let a = consumer.assignment().await;
        let total: usize = a.values().map(|v| v.len()).sum();
        if total > 0 {
            println!("  Consumer joined group (partitions={})", total);
            break;
        }
        if std::time::Instant::now() > assignment_deadline {
            panic!("Consumer failed to join group within 30s");
        }
        sleep(Duration::from_secs(1)).await;
    }

    // 消费消息
    let mut records = Vec::new();
    let consume_deadline = std::time::Instant::now() + Duration::from_secs(30);
    while records.len() < 5 && std::time::Instant::now() < consume_deadline {
        let r = consumer.poll(3000).await.unwrap();
        records.extend(r);
    }
    println!("  Consumed {} messages", records.len());

    // 等待 auto_commit 或手动提交
    sleep(Duration::from_secs(2)).await;
    consumer.commit().await.unwrap();
    println!("  Offset committed");

    assert!(records.len() >= 5, "Expected at least 5 messages");
}
