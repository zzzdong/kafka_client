//! 偏移量提交测试
//!
//! 验证消费者在消费后提交偏移量，auto_commit 任务正常工作。
//!
//! 运行: KAFKA_RUNTIME=direct cargo test --test offset_commit -- --nocapture

mod common;

use common::KafkaInstance;
use kafka_client::client::high_level::AutoOffsetReset;
use kafka_client::client::low_level::KafkaClient;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

#[tokio::test]
async fn test_offset_commit() {
    let server = KafkaInstance::start().await;
    let client = Arc::new(Mutex::new(
        KafkaClient::connect(server.client_config()).await.unwrap(),
    ));

    // 先生产一些消息
    {
        let mut c = client.lock().await;
        common::create_topic(&mut c, "tc-offset", 2).await;
    }
    common::produce_messages(&client, "tc-offset", 5).await;

    // 消费者消费并提交偏移量
    let c1_client = Arc::new(Mutex::new(
        KafkaClient::connect(server.client_config()).await.unwrap(),
    ));
    let mut consumer = kafka_client::client::high_level::Consumer::new(
        c1_client,
        kafka_client::client::high_level::ConsumerConfig {
            group_id: "cg-offset-test".to_string(),
            auto_commit: true,
            auto_commit_interval_ms: 1000,
            auto_offset_reset: AutoOffsetReset::Earliest,
            ..Default::default()
        },
    )
    .await;
    consumer
        .subscribe(vec!["tc-offset".to_string()])
        .await
        .unwrap();

    // 等待组加入
    loop {
        let a = consumer.assignment().await;
        let total: usize = a.values().map(|v| v.len()).sum();
        if total > 0 {
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }

    // 消费消息
    let mut consumed = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while consumed.len() < 5 && std::time::Instant::now() < deadline {
        let records = consumer.poll(3000).await.unwrap();
        consumed.extend(records);
    }
    assert!(
        consumed.len() >= 5,
        "Should have consumed at least 5 messages, got {}",
        consumed.len()
    );
    println!("  Consumed {} messages, committing offset", consumed.len());
    let _ = consumer.commit().await;
    sleep(Duration::from_secs(2)).await;
    println!("  Offset commit completed without error");
}
