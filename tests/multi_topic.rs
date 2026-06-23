//! 多主题生产消费测试
//!
//! 验证生产者可以同时向多个主题发送消息。
//!
//! 运行: KAFKA_RUNTIME=direct cargo test --test multi_topic --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

mod common;

use common::KafkaInstance;

#[tokio::test]
async fn test_produce_to_multiple_topics() {
    let server = KafkaInstance::start().await;
    let client = server.build_client().await;

    common::create_topic(&client, "tc-multi-a", 2).await;
    common::create_topic(&client, "tc-multi-b", 2).await;

    common::produce_messages(&client, "tc-multi-a", 5).await;
    common::produce_messages(&client, "tc-multi-b", 5).await;

    let ra = common::consume_all(&client, "cg-multi-a", "tc-multi-a", 5).await;
    let rb = common::consume_all(&client, "cg-multi-b", "tc-multi-b", 5).await;
    assert_eq!(ra.len(), 5);
    assert_eq!(rb.len(), 5);
}
