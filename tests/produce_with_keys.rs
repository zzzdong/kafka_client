#![cfg(feature = "integration_tests")]

mod common;

use common::build_test_client;
use common::compose;
use std::collections::HashMap;

async fn setup() {
    compose::ensure(&compose::clusters::THREE_BROKER).await;
}

#[tokio::test]
async fn test_produce_with_keys() {
    setup().await;
    let client = build_test_client().await;

    common::create_topic(&client, "tc-keys", 3).await;
    common::produce_messages_with_keys(&client, "tc-keys", 9, 3).await;

    let records = common::consume_all(&client, "cg-keys", "tc-keys", 9).await;

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
