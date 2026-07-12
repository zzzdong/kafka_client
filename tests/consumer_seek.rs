//! Consumer offset seek test
//!
//! Verifies that a consumer can manually seek to a specific offset and
//! re-consume historical messages. Requires a 3-broker cluster (consumer
//! groups need a coordinator).
//!
//! Run individually:
//!   cargo test --test consumer_seek --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

mod common;

use common::build_test_client;
use common::compose;
use kafka_client::{AutoOffsetReset, ConsumerConfig};
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::sleep;

async fn setup() {
    compose::ensure(&compose::clusters::THREE_BROKER).await;
}

#[tokio::test]
async fn test_consumer_seek_to_earliest() {
    setup().await;
    let client = build_test_client().await;

    // Use a unique topic name per test run so that repeated executions or
    // leftover broker state do not pollute the assertion set.
    let topic = format!("tc-seek-{}", std::process::id());
    common::create_topic(&client, &topic, 2).await;
    common::produce_messages(&client, &topic, 10).await;

    // Give metadata time to settle before consumer starts
    client.refresh_metadata().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    let group_id = "cg-seek-test";
    let seek_client = build_test_client().await;
    let mut consumer = seek_client.consumer(
        ConsumerConfig::new()
            .with_group_id(group_id)
            .with_auto_commit(false)
            .with_auto_offset_reset(AutoOffsetReset::Latest),
    );

    consumer
        .subscribe(vec![topic.clone()])
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let a = consumer.group().assignment().await;
        let total: usize = a.values().map(|v| v.len()).sum();
        if total > 0 {
            println!("  Consumer joined group (group={})", group_id);
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("Consumer failed to join group within 30s");
        }
        sleep(Duration::from_secs(1)).await;
    }

    // Small extra delay so the reactor can resolve Latest offsets
    sleep(Duration::from_millis(500)).await;

    let records = consumer
        .poll_timeout(Duration::from_secs(3))
        .await
        .expect("First poll failed");
    println!(
        "  Latest consumer got {} messages (expected 0)",
        records.len()
    );
    // Latest consumer should ideally get 0 messages — if it got some
    // it means offset resolution is slow; fast-forward past them.
    if !records.is_empty() {
        let assignment = consumer.group().assignment().await;
        for (topic, partitions) in &assignment {
            for &p in partitions {
                let max_offset = records
                    .iter()
                    .filter(|r| r.topic == *topic && r.partition == p)
                    .map(|r| r.offset)
                    .max()
                    .unwrap_or(0)
                    + 1;
                consumer.offsets().set(topic, p, max_offset).await;
                println!("  Fast-forwarded {}/{} to offset {}", topic, p, max_offset);
            }
        }
        sleep(Duration::from_secs(1)).await;
        let empty = consumer
            .poll_timeout(Duration::from_secs(2))
            .await
            .expect("Second poll failed");
        println!("  After fast-forward: got {} messages", empty.len());
    }

    // Seek every assigned partition to offset 0.
    let assignment = consumer.group().assignment().await;
    for (t, partitions) in &assignment {
        for &p in partitions {
            consumer.offsets().set(t, p, 0).await;
            println!("  Seeked {}/{} to offset 0", t, p);
        }
    }

    // Re-consume. We should observe all 10 produced messages (duplicates are
    // acceptable because records may already be buffered before the seek).
    let mut all = Vec::new();
    let consume_deadline = std::time::Instant::now() + Duration::from_secs(20);
    while all.len() < 20 && std::time::Instant::now() < consume_deadline {
        let records = consumer
            .poll_timeout(Duration::from_millis(3000))
            .await
            .unwrap();
        let count = records.len();
        all.extend(records);
        if count > 0 {
            println!("  Poll returned {} messages (total: {})", count, all.len());
        }
    }

    println!("  Consumed {} messages after seek (expected 10)", all.len());
    // Fetching may return duplicates across partition boundaries or when
    // records were already buffered before the seek. The contract we assert
    // here is that every produced message is observable after seeking to
    // the earliest offset.
    let values: HashSet<_> = all
        .iter()
        .map(|r| String::from_utf8_lossy(&r.value).to_string())
        .collect();
    println!("  Unique messages after seek: {:?}", values);
    assert!(
        values.len() >= 10,
        "Expected at least 10 unique messages after seek, got {} unique from {} total: {:?}",
        values.len(),
        all.len(),
        values
    );
    for i in 0..10 {
        let expected = format!("msg-{}", i);
        assert!(
            values.contains(&expected),
            "Expected '{}' in consumed messages, got {:?}",
            expected,
            values
        );
    }
}
