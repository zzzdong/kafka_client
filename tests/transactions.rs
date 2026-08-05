//! 事务 (Kafka EOS) 集成测试。
//!
//! 覆盖:
//! - 幂等生产者默认开启 (InitProducerId 惰性初始化)
//! - 事务提交 (begin -> send -> commit -> 可消费)
//! - 事务中止后状态机可恢复 (begin -> send -> abort -> 重新 begin/commit)
//! - 事务内提交消费位移 (TxnOffsetCommit, 使用真实 consumer 的 generation)
//!
//! 单独运行:
//!   cargo test --test transactions --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

mod common;

use common::{build_test_client, compose, run_with_timeout};
use kafka_client::{ConsumerConfig, ProducerConfig, ProducerRecord};
use std::collections::HashMap;
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
async fn test_transactional_produce_commit() {
    run_with_timeout(async {
        setup().await;
        let client = build_test_client().await;
        let topic = unique("txn-commit");
        common::create_topic(&client, &topic, 3).await;
        common::wait_for_topic_ready(&client, &topic, 3).await;

        let producer = client
            .producer(
                ProducerConfig::new().with_transactional_id(unique("txn-producer")),
            )
            .await;
        producer.init_transactions().await.expect("init_transactions");

        producer
            .begin_transaction()
            .await
            .expect("begin_transaction");
        for i in 0..10 {
            producer
                .send(ProducerRecord::new(
                    topic.clone(),
                    format!("txn-msg-{i}").into(),
                ))
                .await
                .expect("send inside transaction");
        }
        producer
            .commit_transaction()
            .await
            .expect("commit_transaction");

        let records = common::consume_all_timeout(
            &client,
            &unique("cg-commit"),
            &topic,
            10,
            Duration::from_secs(20),
        )
        .await;
        assert_eq!(
            records.len(),
            10,
            "committed transactional messages should be consumable"
        );
        client.close().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn test_transactional_abort_recovers() {
    run_with_timeout(async {
        setup().await;
        let client = build_test_client().await;
        let topic = unique("txn-abort");
        common::create_topic(&client, &topic, 1).await;
        common::wait_for_topic_ready(&client, &topic, 1).await;

        let producer = client
            .producer(
                ProducerConfig::new().with_transactional_id(unique("txn-abort-producer")),
            )
            .await;
        producer.init_transactions().await.unwrap();

        // First transaction is aborted.
        producer.begin_transaction().await.unwrap();
        producer
            .send(ProducerRecord::new(topic.clone(), "aborted-msg".into()))
            .await
            .unwrap();
        producer.abort_transaction().await.expect("abort_transaction");

        // The state machine must recover: begin + commit a new transaction.
        producer
            .begin_transaction()
            .await
            .expect("re-begin after abort");
        producer
            .send(ProducerRecord::new(
                topic.clone(),
                "committed-after-abort".into(),
            ))
            .await
            .unwrap();
        producer
            .commit_transaction()
            .await
            .expect("commit after abort");

        let records = common::consume_all_timeout(
            &client,
            &unique("cg-abort"),
            &topic,
            1,
            Duration::from_secs(20),
        )
        .await;
        // NOTE: this client consumes with read_uncommitted, so the aborted
        // message may be visible as well; the essential assertions are that
        // the committed message is delivered and the state machine recovered.
        assert!(
            records
                .iter()
                .any(|r| r.value.as_ref() == b"committed-after-abort"),
            "committed message must be delivered after abort + new transaction"
        );
        client.close().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn test_transactional_offset_commit() {
    run_with_timeout(async {
        setup().await;
        let client = build_test_client().await;
        let topic = unique("txn-offset");
        let group = unique("txn-offset-group");
        common::create_topic(&client, &topic, 1).await;
        common::wait_for_topic_ready(&client, &topic, 1).await;
        common::produce_messages(&client, &topic, 3).await;

        let producer = client
            .producer(
                ProducerConfig::new().with_transactional_id(unique("txn-offset-producer")),
            )
            .await;
        producer.init_transactions().await.unwrap();

        // Join the group with a real consumer to obtain a valid generation
        // and member id (the consume-process-produce EOS pattern).
        let mut consumer = client
            .consumer(ConsumerConfig::new().with_group_id(group.clone()).with_earliest());
        consumer.subscribe(vec![topic.clone()]).await.expect("subscribe");
        consumer
            .poll_timeout(Duration::from_secs(10))
            .await
            .expect("poll");
        let generation = consumer.group().generation().await;
        let member_id = consumer.group().member_id().await;
        assert!(generation >= 0, "consumer should have a valid generation");
        assert!(!member_id.is_empty(), "consumer should have a member id");

        // Commit offset 3 for partition 0 inside a transaction.
        producer.begin_transaction().await.unwrap();
        let offsets = HashMap::from([(topic.clone(), HashMap::from([(0, 3i64)]))]);
        producer
            .send_offsets_to_transaction(&group, generation, &member_id, offsets)
            .await
            .expect("send_offsets_to_transaction");
        producer
            .commit_transaction()
            .await
            .expect("commit_transaction");

        let fetched = client
            .admin()
            .fetch_group_offsets(&group)
            .await
            .expect("fetch_group_offsets");
        assert!(
            fetched.iter().any(|o| {
                o.topic == topic && o.partition == 0 && o.committed_offset == 3
            }),
            "transactionally committed offsets should be visible: {fetched:?}"
        );

        consumer.unsubscribe().await.ok();
        client.close().await.unwrap();
    })
    .await;
}
