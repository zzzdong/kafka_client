//! 带 key 的生产消费测试
//!
//! 验证相同 key 的消息始终路由到同一分区。
//!
//! 运行: KAFKA_RUNTIME=direct cargo test --test produce_with_keys --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

mod common;

use common::KafkaInstance;
use kafka_client::client::core::KafkaClient;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_produce_with_keys() {
    let server = KafkaInstance::start().await;
    let client = Arc::new(Mutex::new(
        KafkaClient::connect(server.client_config()).await.unwrap(),
    ));

    {
        let mut c = client.lock().await;
        common::create_topic(&mut c, "tc-keys", 3).await;
    }

    // 9 messages with 3 distinct keys (3 each)
    common::produce_messages_with_keys(&client, "tc-keys", 9, 3).await;

    let records = common::consume_all(&client, "cg-keys", "tc-keys", 9).await;

    // Verify same key → same partition
    let mut key_partition: HashMap<String, i32> = HashMap::new();
    for r in &records {
        if let Some(ref key) = r.key {
            let key_str = String::from_utf8_lossy(key).to_string();
            match key_partition.get(&key_str) {
                Some(&p) => assert_eq!(
                    p, r.partition,
                    "Key '{}' routed to different partitions {} and {}",
                    key_str, p, r.partition
                ),
                None => {
                    key_partition.insert(key_str, r.partition);
                }
            }
        }
    }
    println!("    Key->partition mapping: {:?}", key_partition);
}
