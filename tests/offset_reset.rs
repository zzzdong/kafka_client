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
async fn test_offset_reset_policies() {
    setup().await;
    let client = build_test_client().await;

    common::create_topic(&client, "tc-reset", 2).await;
    common::produce_messages(&client, "tc-reset", 5).await;

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

    common::create_topic(&client, "tc-latest", 2).await;
    common::produce_messages(&client, "tc-latest", 3).await;

    let latest_client = build_test_client().await;
    let mut consumer =
        latest_client.consumer(consumer_config("cg-latest-test", AutoOffsetReset::Latest));
    consumer
        .subscribe(vec!["tc-latest".to_string()])
        .await
        .unwrap();

    loop {
        let a = consumer.group().assignment().await;
        let total: usize = a.values().map(|v| v.len()).sum();
        if total > 0 {
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }

    let mut records = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    while std::time::Instant::now() < deadline {
        let r = consumer
            .poll_timeout(Duration::from_millis(3000))
            .await
            .unwrap();
        if r.is_empty() {
            break;
        }
        records.extend(r);
    }
    println!("  Latest consumer: {} messages (may be 0)", records.len());
}
