//! 独立的 Kerberos Service Ticket 获取测试。
//!
//! 验证 `krb5-gss` crate 的 `KerberosClient::acquire_service_ticket`
//! 能否通过 `TokioKdcTransport` 从真实 KDC 成功获取服务票据。
//!
//! 此测试**不依赖完整的 Kafka broker**，仅需要 KDC 容器运行即可。
//!
//! 前提: docker-compose.kerberos.yml 正在运行（KDC 容器）
//! 环境变量:
//! - KERBEROS_KEYTAB (默认 tests/fixtures/kerberos/keytabs/client.keytab)
//! - KERBEROS_KDC_HOST (默认 localhost)
//! - KERBEROS_KDC_PORT (默认 8888)

#![cfg(feature = "integration_tests")]

mod common;

use krb5_gss::{KerberosClient, KerberosCredentials, TokioKdcTransport};

/// 解析 env 或回退默认值
fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::test]
async fn test_kerberos_service_ticket() {
    common::compose::ensure(&common::compose::clusters::KERBEROS).await;

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
        .with_keytab(&keytab_path)
        .with_realm("EXAMPLE.COM");

    // 创建 KDC 传输层
    let transport = TokioKdcTransport::new(&kdc_host, kdc_port);

    // 创建 KerberosClient
    let client =
        KerberosClient::new(&creds).expect("Should create KerberosClient from valid keytab");

    // 目标服务: kafka/localhost@EXAMPLE.COM (KDC 中已添加的 principal)
    // 注意: KDC 中注册的是 kafka/localhost@EXAMPLE.COM, 所以 service sname = "kafka/localhost"
    let service = "kafka/localhost";

    eprintln!("  [kerberos_service_ticket] acquiring service ticket for '{service}'...");

    // 执行 service ticket 获取
    let ticket = client
        .acquire_service_ticket(&transport, service)
        .await
        .expect("Should acquire service ticket from KDC");

    eprintln!("  [kerberos_service_ticket] ticket acquired successfully!");

    // 验证票据内容
    assert!(
        !ticket.ticket_der.is_empty(),
        "ticket_der should not be empty"
    );
    assert!(
        ticket.ticket_der.len() > 20,
        "ticket_der length should be > 20 bytes, got {}",
        ticket.ticket_der.len()
    );
    assert!(
        !ticket.session_key.is_empty(),
        "session_key should not be empty"
    );
    assert_eq!(ticket.crealm, "EXAMPLE.COM", "crealm should be EXAMPLE.COM");
    eprintln!(
        "  [kerberos_service_ticket] ticket_der={} bytes, session_key={} bytes, etype={:?}",
        ticket.ticket_der.len(),
        ticket.session_key.len(),
        ticket.session_etype,
    );
}

#[tokio::test]
async fn test_kerberos_service_ticket_invalid_creds() {
    common::compose::ensure(&common::compose::clusters::KERBEROS).await;

    let kdc_host = env_or("KERBEROS_KDC_HOST", "localhost");
    let kdc_port: u16 = std::env::var("KERBEROS_KDC_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8888);

    // 使用不存在的 keytab 路径
    let creds = KerberosCredentials::new("nonexistent@EXAMPLE.COM")
        .with_keytab("/dev/null")
        .with_realm("EXAMPLE.COM");

    let _transport = TokioKdcTransport::new(&kdc_host, kdc_port);

    let result = KerberosClient::new(&creds);
    assert!(
        result.is_err(),
        "KerberosClient::new with invalid keytab should fail"
    );
}
