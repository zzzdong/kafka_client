//! Producer API 集成测试。
//!
//! 覆盖:
//! - send_batch / send_direct / flush / close
//! - 幂等生产者默认开启时的正常生产 (ack 元数据有效)
//!
//! 单独运行:
//!   cargo test --test producer_api --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

mod common;

use common::{build_test_client, compose, run_with_timeout};
use kafka_client::{ProducerConfig, ProducerRecord};
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
async fn test_send_batch() {
    run_with_timeout(async {
        setup().await;
        let client = build_test_client().await;
        let topic = unique("pa-batch");
        common::create_topic(&client, &topic, 2).await;
        common::wait_for_topic_ready(&client, &topic, 2).await;

        let producer = client.producer(ProducerConfig::new().with_linger(1)).await;
        let records: Vec<ProducerRecord> = (0..10)
            .map(|i| ProducerRecord::new(topic.clone(), format!("batch-{i}").into()))
            .collect();
        let sent = producer.send_batch(records).await.expect("send_batch");
        assert_eq!(sent, 10, "all records should be buffered");
        producer.flush().await.expect("flush");

        let consumed = common::consume_all_timeout(
            &client,
            &unique("cg-batch"),
            &topic,
            10,
            Duration::from_secs(20),
        )
        .await;
        assert_eq!(consumed.len(), 10, "all batched messages should arrive");

        producer.close().await.expect("close");
        client.close().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn test_send_direct() {
    run_with_timeout(async {
        setup().await;
        let client = build_test_client().await;
        let topic = unique("pa-direct");
        common::create_topic(&client, &topic, 1).await;
        common::wait_for_topic_ready(&client, &topic, 1).await;

        // Idempotent producer falls back to the buffered path; use a
        // non-idempotent config so send_direct exercises the direct path.
        let producer = client
            .producer(ProducerConfig::new().with_idempotence(false))
            .await;
        for i in 0..5 {
            let meta = producer
                .send_direct(ProducerRecord::new(
                    topic.clone(),
                    format!("direct-{i}").into(),
                ))
                .await
                .expect("send_direct");
            assert!(meta.offset >= 0, "direct send should return a valid offset");
        }

        let consumed = common::consume_all_timeout(
            &client,
            &unique("cg-direct"),
            &topic,
            5,
            Duration::from_secs(20),
        )
        .await;
        assert_eq!(consumed.len(), 5, "all direct messages should arrive");

        client.close().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn test_flush_and_close() {
    run_with_timeout(async {
        setup().await;
        let client = build_test_client().await;
        let topic = unique("pa-flush");
        common::create_topic(&client, &topic, 1).await;
        common::wait_for_topic_ready(&client, &topic, 1).await;

        let producer = client
            .producer(
                ProducerConfig::new()
                    .with_linger(100)
                    .with_batch_size(10 * 1024 * 1024), // don't auto-flush
            )
            .await;
        for i in 0..3 {
            producer
                .send(ProducerRecord::new(
                    topic.clone(),
                    format!("flush-{i}").into(),
                ))
                .await
                .expect("send");
        }
        // Without flush the linger may not have fired yet; explicit flush must
        // deliver everything.
        producer.flush().await.expect("flush");
        producer.close().await.expect("close");

        let consumed = common::consume_all_timeout(
            &client,
            &unique("cg-flush"),
            &topic,
            3,
            Duration::from_secs(20),
        )
        .await;
        assert_eq!(consumed.len(), 3, "flush/close should deliver all messages");

        client.close().await.unwrap();
    })
    .await;
}
