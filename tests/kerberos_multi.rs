//! Kerberos/GSSAPI 多 broker 集成测试。
//!
//! 验证: 每台 broker 广告不同的主机名 (broker1/2/3.example.com) 并有独立
//! 的服务 principal (kafka/brokerN.example.com), 客户端对每台 broker 都
//! 用各自的 advertised host 认证, 而不是全局共享一个 hostname。
//!
//! 前提:
//! - tests/docker-compose.kerberos-multi.yml 集群 (KDC 端口 8889)
//! - 宿主机 /etc/hosts 把 broker1/2/3.example.com 解析到 127.0.0.1
//!
//! 单独运行:
//!   KAFKA_BOOTSTRAP_KERBEROS_MULTI=broker1.example.com:19096 \
//!   KERBEROS_KEYTAB_MULTI=tests/fixtures/kerberos-multi/keytabs/client.keytab \
//!   KERBEROS_KDC_PORT=8889 KAFKA_CLUSTER_SIZE=3 \
//!     cargo test --test kerberos_multi --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

mod common;

use kafka_client::KerberosCredentials;
use std::net::ToSocketAddrs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const BROKER_HOSTS: [&str; 3] = [
    "broker1.example.com",
    "broker2.example.com",
    "broker3.example.com",
];

/// 检查 broker 主机名在宿主机上可解析 (指向 127.0.0.1)。
fn ensure_hosts_resolve() {
    for host in BROKER_HOSTS {
        let mut resolved = format!("{host}:1")
            .to_socket_addrs()
            .unwrap_or_else(|e| panic!("cannot resolve {host}: {e}"));
        assert!(
            resolved.any(|a| a.ip().is_loopback()),
            "{host} does not resolve to 127.0.0.1 — add \
             '127.0.0.1 broker1.example.com broker2.example.com broker3.example.com' \
             to /etc/hosts (CI does this via sudo)"
        );
    }
}

#[tokio::test]
async fn test_kerberos_multi_broker_gssapi() {
    ensure_hosts_resolve();
    common::compose::ensure(&common::compose::clusters::KERBEROS_MULTI).await;

    let bootstrap = std::env::var("KAFKA_BOOTSTRAP_KERBEROS_MULTI")
        .unwrap_or_else(|_| "broker1.example.com:19096".to_string());
    let keytab_path = std::env::var("KERBEROS_KEYTAB_MULTI")
        .unwrap_or_else(|_| "tests/fixtures/kerberos-multi/keytabs/client.keytab".to_string());
    let kdc_port: u16 = std::env::var("KERBEROS_KDC_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8889);
    assert!(
        std::path::Path::new(&keytab_path).exists(),
        "Keytab not found at {keytab_path}. Is the KDC container running?"
    );

    let creds = KerberosCredentials::new("client@EXAMPLE.COM")
        .with_keytab(keytab_path)
        .with_realm("EXAMPLE.COM");

    // 故意设置一个全局 broker_hostname: 只有当"连接级主机名优先"的修复
    // 生效时, broker2/3 才能用各自 advertised host 认证通过。
    let client = kafka_client::Client::builder(vec![bootstrap])
        .with_kerberos(creds)
        .with_broker_hostname("broker1.example.com")
        .with_kdc("localhost", kdc_port)
        .build()
        .await
        .expect("Kerberos multi-broker client failed to connect");

    // Metadata 刷新会连接所有 3 个 broker, 每个都用自己 advertised 的
    // hostname 作为服务 principal。
    client
        .refresh_metadata()
        .await
        .expect("metadata refresh over GSSAPI");
    let brokers = client.metadata().get_all_brokers().await;
    assert_eq!(
        brokers.len(),
        3,
        "expected 3 brokers in metadata, got {}",
        brokers.len()
    );
    for b in &brokers {
        assert!(
            b.host.ends_with("example.com"),
            "broker {} should advertise a hostname, got {}",
            b.node_id,
            b.host
        );
    }
    println!("  Connected to all 3 brokers with per-broker principals");

    // 生产/消费: 3 分区 rf=3, 消息会落到不同 leader, 覆盖所有 broker 的
    // GSSAPI 认证路径。
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let topic = format!("krb-multi-{nanos}");
    common::create_topic(&client, &topic, 3).await;
    common::wait_for_topic_ready(&client, &topic, 3).await;
    common::produce_messages(&client, &topic, 6).await;

    let records = common::consume_all_timeout(
        &client,
        &format!("cg-{topic}"),
        &topic,
        6,
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(records.len(), 6, "all messages should be consumable");

    client.close().await.unwrap();
}
