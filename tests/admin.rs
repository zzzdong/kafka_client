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
use kafka_client::admin::{
    AclBinding, AclBindingFilter, AclOperation, AclPermissionType, AclResourceType, NewTopic,
    OffsetCommitSpec,
};
use kafka_client::{ConsumerConfig, KafkaErrorCode};
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
        // Diagnostic: surface the broker's actual DescribeConfigs response
        // for BROKER resources (empty name and broker id variants).
        match admin.describe_configs(4, "").await {
            Ok(entries) => println!("  describe_configs(4, ''): {} entries", entries.len()),
            Err(e) => println!("  describe_configs(4, '') failed: {e}"),
        }
        if let Some(broker) = cluster.brokers.first() {
            match admin.describe_configs(4, &broker.id.to_string()).await {
                Ok(entries) => println!(
                    "  describe_configs(4, broker {}): {} entries",
                    broker.id,
                    entries.len()
                ),
                Err(e) => println!("  describe_configs(4, broker {}) failed: {e}", broker.id),
            }
        }
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

#[tokio::test]
async fn test_admin_acl_lifecycle() {
    run_with_timeout(async {
        setup().await;
        let client = build_test_client().await;
        let admin = client.admin();
        let topic = unique("admin-acl");
        common::create_topic(&client, &topic, 1).await;

        let binding = AclBinding::new(
            AclResourceType::Topic,
            topic.clone(),
            "User:alice",
            "*",
            AclOperation::Read,
            AclPermissionType::Allow,
        );
        let results = admin
            .create_acls(&[binding.clone()])
            .await
            .expect("create_acls");
        // The default test cluster has no authorizer configured; the ACL
        // APIs then return SECURITY_DISABLED. Skip gracefully so the suite
        // stays green there while still validating ACLs on authorizer-
        // enabled clusters.
        if results
            .iter()
            .all(|r| r.error_code == KafkaErrorCode::SECURITY_DISABLED)
        {
            println!("  SKIP: broker has no authorizer configured (SECURITY_DISABLED)");
            client.close().await.unwrap();
            return;
        }
        assert!(
            results.iter().all(|r| r.error_code.is_ok()),
            "create_acls results: {results:?}"
        );

        let filter = AclBindingFilter {
            resource_type: Some(AclResourceType::Topic),
            resource_name: Some(topic.clone()),
            ..Default::default()
        };
        let found = admin.describe_acls(&filter).await.expect("describe_acls");
        assert!(
            found
                .iter()
                .any(|a| a.principal == "User:alice" && a.operation == AclOperation::Read),
            "created ACL should be describable: {found:?}"
        );

        let deleted = admin
            .delete_acls(&[filter.clone()])
            .await
            .expect("delete_acls");
        assert!(deleted.iter().all(|d| d.error_code.is_ok()));
        let after = admin
            .describe_acls(&filter)
            .await
            .expect("describe_acls after delete");
        assert!(
            !after.iter().any(|a| a.principal == "User:alice"),
            "deleted ACL should be gone"
        );

        client.close().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn test_admin_config_alter_and_describe() {
    run_with_timeout(async {
        setup().await;
        let client = build_test_client().await;
        let admin = client.admin();
        let topic = unique("admin-config");
        common::create_topic(&client, &topic, 1).await;
        common::wait_for_topic_ready(&client, &topic, 1).await;

        admin
            .alter_topic_configs(&topic, &[("retention.ms".into(), "3600000".into())])
            .await
            .expect("alter_topic_configs");

        let entries = admin
            .describe_configs(2, &topic)
            .await
            .expect("describe_configs");
        assert!(
            entries.iter().any(|c| {
                c.name == "retention.ms" && c.value.as_deref() == Some("3600000")
            }),
            "altered config should be visible: {entries:?}"
        );

        client.close().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn test_admin_delete_records() {
    run_with_timeout(async {
        setup().await;
        let client = build_test_client().await;
        let admin = client.admin();
        let topic = unique("admin-delete-records");
        common::create_topic(&client, &topic, 1).await;
        common::wait_for_topic_ready(&client, &topic, 1).await;
        common::produce_messages(&client, &topic, 5).await;

        let results = admin
            .delete_records(&topic, &[(0, -1)])
            .await
            .expect("delete_records");
        assert_eq!(results.len(), 1);
        assert!(
            results[0].error_code.is_ok(),
            "delete_records error: {}",
            results[0].error_code
        );

        // Records before the deletion point must be gone.
        let mut consumer = client
            .consumer(ConsumerConfig::new().with_earliest());
        consumer.assign(topic.clone(), vec![0]).await.unwrap();
        let records = consumer
            .poll_timeout(Duration::from_secs(5))
            .await
            .expect("poll after delete");
        assert!(
            records.is_empty(),
            "deleted records should not be consumable (got {})",
            records.len()
        );

        client.close().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn test_admin_reset_group_offsets() {
    run_with_timeout(async {
        setup().await;
        let client = build_test_client().await;
        let admin = client.admin();
        let group = unique("admin-cg-reset");
        let topic = unique("admin-reset-topic");
        common::create_topic(&client, &topic, 1).await;
        common::wait_for_topic_ready(&client, &topic, 1).await;

        admin
            .reset_group_offsets(&group, &[(topic.clone(), 0, 7)])
            .await
            .expect("reset_group_offsets");
        let offsets = admin
            .fetch_group_offsets(&group)
            .await
            .expect("fetch_group_offsets");
        assert!(
            offsets
                .iter()
                .any(|o| o.topic == topic && o.partition == 0 && o.committed_offset == 7),
            "reset offsets should be visible: {offsets:?}"
        );

        client.close().await.unwrap();
    })
    .await;
}
