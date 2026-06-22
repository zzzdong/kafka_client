//! 批量消息测试
//!
//! 验证 100 条消息的批量生产和消费。
//!
//! 运行: KAFKA_RUNTIME=direct cargo test --test large_batch --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

mod common;

use common::KafkaInstance;
use kafka_client::client::core::KafkaClient;
use std::sync::Arc;

#[tokio::test]
async fn test_large_batch() {
    let server = KafkaInstance::start().await;
    let client = Arc::new(KafkaClient::connect(server.client_config()).await.unwrap());

    {
        let c = client.clone();
        common::create_topic(&c, "tc-large", 3).await;
    }

    common::produce_messages(&client, "tc-large", 100).await;

    let records = common::consume_all(&client, "cg-large", "tc-large", 100).await;
    println!("  Consumed {} messages from 'tc-large'", records.len());
    assert!(
        records.len() >= 100,
        "Expected at least 100, got {}",
        records.len()
    );
}
