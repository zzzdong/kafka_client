#![cfg(feature = "integration_tests")]

mod common;

use common::compose;
use common::{build_test_client, consumer_config};
use kafka_client::AutoOffsetReset;
use std::time::Duration;
use tokio::time::sleep;

async fn setup() {
    compose::ensure(&compose::clusters::THREE_BROKER).await;
}

#[tokio::test]
async fn test_offset_commit() {
    setup().await;
    let client = build_test_client().await;

    common::create_topic(&client, "tc-offset", 2).await;
    common::produce_messages(&client, "tc-offset", 5).await;

    let c1_client = build_test_client().await;
    let mut consumer =
        c1_client.consumer(consumer_config("cg-offset-test", AutoOffsetReset::Earliest));
    consumer
        .subscribe(vec!["tc-offset".to_string()])
        .await
        .unwrap();

    let assignment_deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let a = consumer.group().assignment().await;
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

    let mut records = Vec::new();
    let consume_deadline = std::time::Instant::now() + Duration::from_secs(30);
    while records.len() < 5 && std::time::Instant::now() < consume_deadline {
        let r = consumer
            .poll_timeout(Duration::from_millis(3000))
            .await
            .unwrap();
        records.extend(r);
    }
    println!("  Consumed {} messages", records.len());

    sleep(Duration::from_secs(2)).await;
    consumer.offsets().commit().await.unwrap();
    println!("  Offset committed");

    assert!(records.len() >= 5, "Expected at least 5 messages");
}
