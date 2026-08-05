//! Admin API 集成测试。
//!
//! 覆盖:
//! - 主题: 创建(含手工副本分配) / 描述 / 列表 / 删除
//! - 集群: describe_cluster / get_broker_config
//! - 消费组: list_groups / describe_groups / fetch_group_offsets /
//!   commit_offsets / delete_group
//!
//! 单独运行:
//!   cargo test --test admin --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

mod common;

use common::{build_test_client, compose, run_with_timeout};
use kafka_client::admin::{NewTopic, OffsetCommitSpec};
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
async fn test_admin_topic_and_cluster_operations() {
    run_with_timeout(async {
        setup().await;
        let client = build_test_client().await;
        let admin = client.admin();

        // Cluster inspection.
        let cluster = admin.describe_cluster().await.expect("describe_cluster");
        assert!(
            !cluster.brokers.is_empty(),
            "expected at least one broker"
        );
        assert!(
            cluster.brokers.iter().all(|b| b.host != ""),
            "broker host should be reported"
        );

        // Broker config query.
        let max_bytes = admin.get_broker_config("max.message.bytes").await;
        assert!(max_bytes.is_some(), "max.message.bytes should be present");
        assert!(
            admin.get_broker_config("no.such.config.key").await.is_none(),
            "unknown config key should return None"
        );

        // Create with explicit replica assignments (exercises the
        // partition_index mapping; num_partitions/replication_factor must be
        // -1 for manual assignments).
        let assigned_topic = unique("admin-assigned");
        let broker_id = cluster.brokers[0].id;
        let result = admin
            .create_topic(
                &NewTopic::new(assigned_topic.clone(), -1, -1)
                    .with_replica_assignments(vec![vec![broker_id]]),
            )
            .await
            .expect("create_topic with replica assignments");
        assert!(
            result.is_success() || result.already_exists(),
            "create topic with assignments failed: {}",
            result.error_code
        );

        // Regular topic + describe + list + delete.
        let topic = unique("admin-topic");
        let result = admin
            .create_topic(&NewTopic::new(topic.clone(), 3, 1))
            .await
            .expect("create_topic");
        assert!(result.is_success() || result.already_exists());
        common::wait_for_topic_ready(&client, &topic, 3).await;

        let descriptions = admin
            .describe_topics(&[topic.clone()])
            .await
            .expect("describe_topics");
        assert_eq!(descriptions.len(), 1);
        assert_eq!(descriptions[0].partitions.len(), 3);
        assert!(
            descriptions[0].partitions.iter().all(|p| p.replicas.len() >= 1),
            "each partition should report replicas"
        );

        let listed = admin.list_topics().await.expect("list_topics");
        assert!(
            listed.iter().any(|t| t.name == topic),
            "listed topics should include the created topic"
        );

        let deleted = admin
            .delete_topics(&[topic.clone(), assigned_topic])
            .await
            .expect("delete_topics");
        for r in deleted {
            assert!(r.is_success(), "delete {} failed: {}", r.name, r.error_code);
        }

        client.close().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn test_admin_commit_and_fetch_offsets_roundtrip() {
    run_with_timeout(async {
        setup().await;
        let client = build_test_client().await;
        let admin = client.admin();
        let group = unique("admin-cg-commit");
        let topic = unique("admin-offset-topic");

        common::create_topic(&client, &topic, 1).await;
        common::wait_for_topic_ready(&client, &topic, 1).await;

        admin
            .commit_offsets(
                &group,
                &[OffsetCommitSpec {
                    topic: topic.clone(),
                    partition: 0,
                    offset: 42,
                    metadata: Some("admin".to_string()),
                }],
            )
            .await
            .expect("admin commit_offsets");

        let offsets = admin
            .fetch_group_offsets(&group)
            .await
            .expect("fetch_group_offsets");
        assert!(
            offsets.iter().any(|o| {
                o.topic == topic && o.partition == 0 && o.committed_offset == 42
            }),
            "committed offsets should be visible: {offsets:?}"
        );

        client.close().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn test_admin_group_lifecycle() {
    run_with_timeout(async {
        setup().await;
        let client = build_test_client().await;
        let admin = client.admin();
        let group = unique("admin-cg-lifecycle");
        let topic = unique("admin-group-topic");

        common::create_topic(&client, &topic, 1).await;
        common::wait_for_topic_ready(&client, &topic, 1).await;
        common::produce_messages(&client, &topic, 5).await;

        // Create the group with a real consumer and committed offsets.
        common::consume_all_timeout(
            &client,
            &group,
            &topic,
            5,
            Duration::from_secs(20),
        )
        .await;

        // Ensure the group has committed offsets (auto-commit may not have
        // fired yet after a fast consume) by committing explicitly.
        admin
            .commit_offsets(
                &group,
                &[OffsetCommitSpec {
                    topic: topic.clone(),
                    partition: 0,
                    offset: 5,
                    metadata: None,
                }],
            )
            .await
            .expect("admin commit_offsets");

        // list_groups should eventually include it.
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            let groups = admin.list_groups().await.expect("list_groups");
            if groups.iter().any(|g| g.group_id == group) {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("group {group} not listed by list_groups");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // describe_groups returns the group with its committed offsets.
        let described = admin
            .describe_groups(&[group.clone()])
            .await
            .expect("describe_groups");
        assert_eq!(described.len(), 1);
        assert_eq!(described[0].group_id, group);

        let offsets = admin
            .fetch_group_offsets(&group)
            .await
            .expect("fetch_group_offsets");
        assert_eq!(offsets.len(), 1, "one partition should have committed offsets");

        // delete_group after the consumer left.
        // The consumer left via LeaveGroup; the coordinator may need a moment
        // to mark the group empty before deletion is allowed.
        let delete_deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            match admin.delete_group(&group).await {
                Ok(()) => break,
                Err(e) if std::time::Instant::now() < delete_deadline => {
                    println!("  delete_group retry: {e}");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => panic!("delete_group failed: {e}"),
            }
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let groups = admin.list_groups().await.expect("list_groups");
            if !groups.iter().any(|g| g.group_id == group) {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("group {group} still listed after delete_group");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        client.close().await.unwrap();
    })
    .await;
}
