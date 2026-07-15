//! Kerberos / SASL-GSSAPI 认证集成测试。
//!
//! 前提: docker-compose.kerberos.yml 正在运行 (KDC + GSSAPI Kafka)。
//! 由 run-all-tests.sh 调度或手动 `docker compose -f tests/docker-compose.kerberos.yml up -d`。
//! 测试进程需要:
//! - 能访问 localhost:88 (KDC)
//! - 存在 keytab 文件 (由 KDC 容器生成)
//! - 能访问 localhost:9096 (Kafka SASL_PLAINTEXT)
//!
//! 环境变量:
//! - KAFKA_BOOTSTRAP (默认 localhost:9096)
//! - KERBEROS_KEYTAB (默认 tests/fixtures/kerberos/keytabs/client.keytab)
//! - KERBEROS_KDC_HOST (默认 localhost)
//! - KERBEROS_KDC_PORT (默认 88)

#![cfg(feature = "integration_tests")]

mod common;

use kafka_client::KerberosCredentials;
use std::time::Duration;

/// 解析 env 或回退默认值
fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::test]
async fn test_kerberos_connect_and_metadata() {
    common::compose::ensure(&common::compose::clusters::KERBEROS).await;

    let bootstrap = env_or("KAFKA_BOOTSTRAP_KERBEROS", "127.0.0.1:9096");
    let keytab_path = env_or(
        "KERBEROS_KEYTAB",
        "tests/fixtures/kerberos/keytabs/client.keytab",
    );
    let kdc_host = env_or("KERBEROS_KDC_HOST", "localhost");
    let kdc_port: u16 = std::env::var("KERBEROS_KDC_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8888);

    // 验证 keytab 文件存在
    assert!(
        std::path::Path::new(&keytab_path).exists(),
        "Keytab not found at {keytab_path}. Is the KDC container running?"
    );

    // 构建 Kerberos 凭证
    let creds = KerberosCredentials::new("client@EXAMPLE.COM")
        .with_keytab(keytab_path)
        .with_realm("EXAMPLE.COM");

    // Kafka broker 的 JAAS principal 是 kafka/localhost@EXAMPLE.COM
    // advertised listener: SASL_PLAINTEXT://localhost:9096
    let broker_hostname = "localhost";

    // Kafka 安全集群 (Kerberos) 启动较慢, 重试直到连接成功
    let client = 'retry: loop {
        for attempt in 1..=20 {
            eprintln!("  [attempt {attempt}/20] Connecting to {bootstrap}...");
            match kafka_client::Client::builder(vec![bootstrap.clone()])
                .with_kerberos(creds.clone())
                .with_broker_hostname(broker_hostname)
                .with_kdc(kdc_host.clone(), kdc_port)
                .build()
                .await
            {
                Ok(c) => break 'retry c,
                Err(e) => {
                    eprintln!("    failed: {e}");
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                }
            }
        }
        panic!("Kerberos client failed to connect after 20 attempts");
    };
    eprintln!("  Connected successfully!");

    // 验证 metadata 可正常刷新 (确认连接可用)
    client
        .refresh_metadata()
        .await
        .expect("Metadata refresh should succeed over GSSAPI connection");

    // 验证集群大小
    let cluster_size: usize = std::env::var("KAFKA_CLUSTER_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    common::assert_cluster_size(&client, cluster_size).await;

    // 验证基础的生产消费
    let topic = format!(
        "kerberos-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    common::create_topic(&client, &topic, 1).await;
    common::wait_for_topic_ready(&client, &topic, 1).await;

    let group_id = format!("kerberos-group-{}", topic);
    common::produce_messages(&client, &topic, 10).await;

    let records =
        common::consume_all_timeout(&client, &group_id, &topic, 10, Duration::from_secs(15)).await;
    assert_eq!(records.len(), 10, "Should consume 10 messages over GSSAPI");

    client.close().await.expect("Close should succeed");
}

#[tokio::test]
async fn test_kerberos_invalid_credentials() {
    common::compose::ensure(&common::compose::clusters::KERBEROS).await;

    let bootstrap = env_or("KAFKA_BOOTSTRAP_KERBEROS", "127.0.0.1:9096");
    // 使用故意错误的 principal
    let creds = KerberosCredentials::new("nonexistent@EXAMPLE.COM").with_keytab("/dev/null");

    let result = kafka_client::Client::builder(vec![bootstrap.clone()])
        .with_kerberos(creds)
        .with_kdc("localhost", 8888)
        .build()
        .await;

    assert!(
        result.is_err(),
        "Client build with invalid kerberos credentials should fail"
    );
}
