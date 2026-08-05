//! ACL 集成测试。
//!
//! ACL 管理 API (CreateAcls/DescribeAcls/DeleteAcls) 需要 broker 开启
//! authorizer；本测试使用独立的 ACL 集群（tests/docker-compose.acl.yml，
//! StandardAuthorizer + ANONYMOUS super user）。
//!
//! 单独运行:
//!   KAFKA_BOOTSTRAP_ACL=127.0.0.1:9098 KAFKA_CLUSTER_SIZE=1 \
//!     cargo test --test acl --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

mod common;

use common::compose;
use kafka_client::admin::{
    AclBinding, AclBindingFilter, AclOperation, AclPermissionType, AclResourceType,
};
use kafka_client::{Client, KafkaErrorCode};
use std::time::{SystemTime, UNIX_EPOCH};

async fn acl_client() -> Client {
    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP_ACL").unwrap_or_else(|_| "127.0.0.1:9098".to_string());
    Client::builder(vec![bootstrap])
        .with_client_id("acl-test")
        .build()
        .await
        .expect("failed to build ACL test client")
}

#[tokio::test]
async fn test_acl_lifecycle() {
    compose::ensure(&compose::clusters::ACL).await;
    let client = acl_client().await;
    let admin = client.admin();

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let topic = format!("acl-topic-{nanos}");
    common::create_topic(&client, &topic, 1).await;
    common::wait_for_topic_ready(&client, &topic, 1).await;

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
    // Fallback for clusters without an authorizer (e.g. the default one).
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
}
