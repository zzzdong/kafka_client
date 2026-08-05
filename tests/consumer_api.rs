//! Consumer API 集成测试。
//!
//! 覆盖:
//! - direct 模式手动 assign 消费
//! - direct 模式 seek 回到指定 offset 重放
//! - max_poll_records 限制
//! - try_poll 空缓冲立即返回 / poll_timeout 阻塞等待
//!
//! 单独运行:
//!   cargo test --test consumer_api --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

mod common;

use common::{build_test_client, compose, run_with_timeout};
use kafka_client::ConsumerConfig;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

async fn setup() {
    compose::ensure(&compose::clusters::THREE_BROKER).await;
}

fn unique(name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{name}-{nanos}")
}

#[tokio::test]
async fn test_direct_assign_and_seek() {
    run_with_timeout(async {
        setup().await;
        let client = build_test_client().await;
        let topic = unique("ca-assign");
        common::create_topic(&client, &topic, 2).await;
        common::wait_for_topic_ready(&client, &topic, 2).await;
        common::produce_messages(&client, &topic, 6).await;

        let mut consumer = client.consumer(ConsumerConfig::new().with_earliest());
        consumer
            .assign(topic.clone(), vec![0, 1])
            .await
            .expect("assign");

        let mut all = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while all.len() < 6 && std::time::Instant::now() < deadline {
            all.extend(
                consumer
                    .poll_timeout(Duration::from_millis(3000))
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(
            all.len(),
            6,
            "direct-mode consumer should read all messages"
        );
        let mut parts: Vec<_> = all.iter().map(|r| r.partition).collect();
        parts.sort();
        parts.dedup();
        assert!(
            parts.contains(&0) && parts.contains(&1),
            "both assigned partitions should be consumed: {parts:?}"
        );

        // Seek partition 0 back to the beginning and re-read its first record.
        consumer.seek(topic.clone(), 0, 0).await.expect("seek");
        let mut again = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while again.is_empty() && std::time::Instant::now() < deadline {
            again.extend(
                consumer
                    .poll_timeout(Duration::from_millis(3000))
                    .await
                    .unwrap(),
            );
        }
        assert!(
            again.iter().any(|r| r.partition == 0 && r.offset == 0),
            "seek to offset 0 should replay the first record"
        );

        client.close().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn test_max_poll_records() {
    run_with_timeout(async {
        setup().await;
        let client = build_test_client().await;
        let topic = unique("ca-maxpoll");
        common::create_topic(&client, &topic, 1).await;
        common::wait_for_topic_ready(&client, &topic, 1).await;
        common::produce_messages(&client, &topic, 5).await;

        let mut consumer = client.consumer(
            ConsumerConfig::new()
                .with_earliest()
                .with_max_poll_records(2),
        );
        consumer
            .assign(topic.clone(), vec![0])
            .await
            .expect("assign");

        let mut all = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while all.len() < 5 && std::time::Instant::now() < deadline {
            all.extend(
                consumer
                    .poll_timeout(Duration::from_millis(3000))
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(all.len(), 5, "max_poll_records must not lose records");

        client.close().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn test_try_poll_and_poll_timeout() {
    run_with_timeout(async {
        setup().await;
        let client = build_test_client().await;
        let topic = unique("ca-trypoll");
        common::create_topic(&client, &topic, 1).await;
        common::wait_for_topic_ready(&client, &topic, 1).await;
        common::produce_messages(&client, &topic, 3).await;

        let mut consumer = client.consumer(ConsumerConfig::new().with_earliest());
        consumer
            .assign(topic.clone(), vec![0])
            .await
            .expect("assign");

        // Nothing buffered yet: try_poll returns immediately and empty,
        // while poll_timeout blocks until data arrives.
        let first = consumer.try_poll().await.expect("try_poll");
        assert!(
            first.is_empty(),
            "try_poll should return empty when nothing is buffered yet"
        );

        let records = consumer
            .poll_timeout(Duration::from_secs(10))
            .await
            .expect("poll_timeout");
        assert!(!records.is_empty(), "poll_timeout should wait for data");

        // Records may arrive in several fetches (produce_messages sends each
        // message separately); consume everything before checking for "no
        // more data".
        let mut total = records.len();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while total < 3 && std::time::Instant::now() < deadline {
            total += consumer
                .poll_timeout(Duration::from_millis(3000))
                .await
                .unwrap()
                .len();
        }
        assert_eq!(total, 3, "all produced records should be consumed");

        // No more data: poll_timeout returns empty after the timeout.
        let empty = consumer
            .poll_timeout(Duration::from_millis(3000))
            .await
            .expect("poll_timeout empty");
        assert!(
            empty.is_empty(),
            "poll_timeout should time out with no data"
        );

        client.close().await.unwrap();
    })
    .await;
}
