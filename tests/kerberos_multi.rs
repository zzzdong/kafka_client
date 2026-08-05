//! Kerberos/GSSAPI 多 broker 集成测试。
//!
//! 验证: 每台 broker 广告不同的主机名 (broker1/2/3.example.com) 并有独立
//! 的服务 principal (kafka/brokerN.example.com), 客户端对每台 broker 都
//! 用各自的 advertised host 认证, 而不是全局共享一个 hostname。
//!
//! 前提:
//! - tests/docker-compose.kerberos-multi.yml 集群
//! - 通常通过该 compose 的 `test-runner` 服务运行 (容器网络内解析
//!   broker1/2/3.example.com 与 kdc-multi.example.com); 在宿主机直接运行则需要
//!   /etc/hosts 把 broker 主机名解析到 127.0.0.1。
//!
//! 单独运行:
//!   KAFKA_BOOTSTRAP_KERBEROS_MULTI=broker1.example.com:19096 \
//!   KERBEROS_KEYTAB_MULTI=tests/fixtures/kerberos-multi/keytabs/client.keytab \
//!   KERBEROS_KDC_HOST=localhost KERBEROS_KDC_PORT=8889 KAFKA_CLUSTER_SIZE=3 \
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

/// 检查 broker 主机名可解析 (容器网络内解析到 broker 容器 IP)。
fn ensure_hosts_resolve() {
    for host in BROKER_HOSTS {
        format!("{host}:1")
            .to_socket_addrs()
            .unwrap_or_else(|e| {
                panic!(
                    "cannot resolve {host}: {e} — run via the compose test-runner \
                     service, or add the broker hostnames to /etc/hosts"
                )
            })
            .next()
            .unwrap_or_else(|| {
                panic!(
                    "cannot resolve {host}: run via the compose test-runner service, \
                     or add the broker hostnames to /etc/hosts"
                )
            });
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
    let kdc_host = std::env::var("KERBEROS_KDC_HOST").unwrap_or_else(|_| "localhost".to_string());
    let kdc_port: u16 = std::env::var("KERBEROS_KDC_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8889);
    // The KDC container exports the keytab shortly after starting; wait for
    // it when running directly via `compose run` (no external wait step).
    let keytab_deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !std::path::Path::new(&keytab_path).exists() {
        if std::time::Instant::now() > keytab_deadline {
            panic!("Keytab not found at {keytab_path} after 30s. Is the KDC container running?");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // 多 broker stack 使用独立 realm (MULTI.EXAMPLE.COM), 以便与单节点
    // Kerberos stack (EXAMPLE.COM) 并行运行而不发生 KDC/keytab 串扰。
    let realm = std::env::var("KERBEROS_REALM").unwrap_or_else(|_| "MULTI.EXAMPLE.COM".to_string());

    let creds = KerberosCredentials::new(format!("client@{realm}"))
        .with_keytab(keytab_path)
        .with_realm(realm.clone());

    // 故意设置一个全局 broker_hostname: 只有当"连接级主机名优先"的修复
    // 生效时, broker2/3 才能用各自 advertised host 认证通过。
    let client = 'retry: loop {
        for attempt in 1..=20 {
            match kafka_client::Client::builder(vec![bootstrap.clone()])
                .with_kerberos(creds.clone())
                .with_broker_hostname("broker1.example.com")
                .with_kdc(kdc_host.clone(), kdc_port)
                .build()
                .await
            {
                Ok(c) => break 'retry c,
                Err(e) => {
                    eprintln!("  [attempt {attempt}/20] connect failed: {e}");
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        }
        panic!("Kerberos multi-broker client failed to connect after 20 attempts");
    };

    // Metadata 刷新会连接所有 3 个 broker, 每个都用自己 advertised 的
    // hostname 作为服务 principal。
    // broker 启动后是异步注册到 controller 的, 轮询直到 3 台全部出现。
    let metadata_deadline = std::time::Instant::now() + Duration::from_secs(60);
    let brokers = loop {
        client
            .refresh_metadata()
            .await
            .expect("metadata refresh over GSSAPI");
        let brokers = client.metadata().get_all_brokers().await;
        if brokers.len() == 3 || std::time::Instant::now() > metadata_deadline {
            break brokers;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert_eq!(
        brokers.len(),
        3,
        "expected 3 brokers in metadata after startup, got {}: {brokers:?}",
        brokers.len(),
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
