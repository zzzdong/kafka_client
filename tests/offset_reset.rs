//! Earliest vs Latest 偏移量策略测试
//!
//! 验证 Earliest 消费者能从最早偏移量消费，Latest 消费者不阻塞。
//!
//! 运行: KAFKA_RUNTIME=direct cargo test --test offset_reset -- --nocapture

mod common;

use common::{KafkaInstance, consumer_config};
use kafka_client::client::high_level::AutoOffsetReset;
use kafka_client::client::low_level::KafkaClient;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

#[tokio::test]
async fn test_offset_reset_policies() {
    let server = KafkaInstance::start().await;
    let client = Arc::new(Mutex::new(
        KafkaClient::connect(server.client_config()).await.unwrap(),
    ));

    // Earliest 测试：先生产再消费
    {
        let mut c = client.lock().await;
        common::create_topic(&mut c, "tc-reset", 2).await;
    }
    common::produce_messages(&client, "tc-reset", 5).await;

    // Earliest 消费者应消费全部 5 条
    {
        let records = common::consume_all(&client, "cg-earliest", "tc-reset", 5).await;
        println!(
            "  Earliest consumer: {} messages (expected >=5)",
            records.len()
        );
        assert!(
            records.len() >= 5,
            "Earliest should get at least 5 messages"
        );
    }

    // Latest 测试
    {
        let mut c = client.lock().await;
        common::create_topic(&mut c, "tc-latest", 2).await;
    }
    common::produce_messages(&client, "tc-latest", 3).await;

    let latest_client = Arc::new(Mutex::new(
        KafkaClient::connect(server.client_config()).await.unwrap(),
    ));
    let mut consumer = kafka_client::client::high_level::Consumer::new(
        latest_client,
        consumer_config("cg-latest-test", AutoOffsetReset::Latest),
    )
    .await;
    consumer
        .subscribe(vec!["tc-latest".to_string()])
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

    // Latest 消费者可能 0 条，但不应阻塞
    let mut records = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    while std::time::Instant::now() < deadline {
        let r = consumer.poll(3000).await.unwrap();
        if r.is_empty() {
            break;
        }
        records.extend(r);
    }
    println!("  Latest consumer: {} messages (may be 0)", records.len());
}
