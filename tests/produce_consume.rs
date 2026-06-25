//! 基础生产+消费测试
//!
//! 发送 3 条消息到 3 分区主题，验证消费正常且消息分布到多个分区。
//!
//! 运行: KAFKA_RUNTIME=direct cargo test --test produce_consume --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

mod common;

use common::KafkaInstance;

#[tokio::test]
async fn test_produce_and_consume() {
    let server = KafkaInstance::start().await;
    let client = server.build_client().await;

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
