//! TLS encryption integration test
//!
//! Tests that the client can connect to a TLS-enabled Kafka broker and
//! perform basic produce/consume operations, including CA certificate
//! verification.
//! Requires TLS single-node cluster (docker-compose.tls.yml).
//!
//! The broker is configured with `ssl.client.auth=required` (mutual TLS),
//! so all clients must present a valid client certificate.
//!
//! # Prerequisites
//!
//! ```bash
//! cd tests
//! ./gen-certs.sh                          # generate TLS certificates
//! ```
//!
//! # Run
//!
//! Auto-starts docker-compose.tls.yml if KAFKA_BOOTSTRAP_TLS not set:
//!   cargo test --test tls --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

mod common;

use common::compose;
use kafka_client::protocol::create_topics_request::CreatableTopic;
use kafka_client::protocol::{CreateTopicsRequest, CreateTopicsResponse};
use kafka_client::{ConsumerConfig, KafkaClient, ProducerConfig, ProducerRecord, TlsConfig};
use std::time::Duration;

/// Ensure TLS cluster is ready (auto-starts if needed)
async fn setup() {
    compose::ensure(&compose::clusters::TLS).await;
}

/// Read TLS bootstrap address from environment.
fn tls_bootstrap_addrs() -> Vec<std::net::SocketAddr> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP_TLS")
        .or_else(|_| std::env::var("KAFKA_BOOTSTRAP"))
        .unwrap_or_else(|_| "127.0.0.1:9093".to_string());
    bootstrap
        .split(',')
        .map(|s| {
            s.trim()
                .parse()
                .expect("Invalid KAFKA_BOOTSTRAP_TLS address")
        })
        .collect()
}

/// Returns the path to the fixtures/tls directory (absolute path).
fn tls_fixtures_dir() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/tls")
}

/// Common TLS config with client certificates for mTLS.
/// The broker requires `ssl.client.auth=required`, so every connection
/// must present a client certificate signed by the test CA.
///
/// Uses verify_certificate=true since the CA cert is available from
/// gen-certs.sh.  NoCertificateVerification (dangerous mode) is
/// incompatible with the Kafka broker's TLS stack in rustls 0.23
/// (causes AlertReceived(HandshakeFailure)), so we always verify.
fn base_tls_config() -> TlsConfig {
    let dir = tls_fixtures_dir();
    TlsConfig {
        domain: "localhost".to_string(),
        verify_certificate: true,
        ca_cert_path: Some(format!("{}/ca-cert.pem", dir)),
        client_cert_path: Some(format!("{}/broker-cert.pem", dir)),
        client_key_path: Some(format!("{}/broker-key.pem", dir)),
    }
}

/// Build a TLS client with mTLS certs and CA verification.
///
/// Retries internally since TLS broker startup may lag behind TCP ready.
async fn build_tls_client() -> KafkaClient {
    let addrs = tls_bootstrap_addrs();
    let tls_config = base_tls_config();

    for attempt in 1..=10 {
        let result = KafkaClient::builder(addrs.clone())
            .with_client_id("tls-integration-test")
            .with_tls_config(tls_config.clone())
            .with_metadata_ttl(Duration::from_secs(10))
            .build()
            .await;
        match result {
            Ok(client) => return client,
            Err(e) => {
                if attempt < 10 {
                    eprintln!(
                        "  TLS connect attempt {}/10 failed: {}, retrying...",
                        attempt, e
                    );
                    tokio::time::sleep(Duration::from_secs(3)).await;
                } else {
                    panic!(
                        "Failed to build KafkaClient with TLS config after 10 attempts: {}",
                        e
                    );
                }
            }
        }
    }
    unreachable!()
}

#[tokio::test]
async fn test_tls_connect_and_metadata() {
    setup().await;
    let client = build_tls_client().await;

    client
        .cluster()
        .refresh_metadata()
        .await
        .expect("Failed to refresh metadata over TLS connection");

    let brokers = client.metadata().get_all_brokers().await;
    println!("  Metadata OK: {} broker(s) in cluster", brokers.len());
    assert!(!brokers.is_empty(), "Expected at least 1 broker via TLS");

    if let Err(e) = client.close().await {
        eprintln!("  Close warning: {}", e);
    }
    println!("=== TLS Connect + Metadata Test PASSED ===");
}

#[tokio::test]
async fn test_tls_produce_and_consume() {
    setup().await;
    let client = build_tls_client().await;

    let topic = "tls-produce-consume-test";
    let request = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.to_string(),
            num_partitions: 1,
            replication_factor: 1,
            assignments: vec![],
            configs: vec![],
        }],
        timeout_ms: 10000,
        validate_only: false,
    };

    let response: CreateTopicsResponse = client
        .cluster()
        .send_to_any_broker(&request)
        .await
        .expect("Failed to create topic via TLS");
    for t in &response.topics {
        assert!(
            t.error_code == 0 || t.error_code == 36,
            "Create topic '{}' failed: error_code {} ({:?})",
            topic,
            t.error_code,
            t.error_message
        );
    }
    println!("  Topic '{}' created", topic);
    common::wait_for_topic_ready(&client, topic, 1).await;

    let producer = client
        .producer(ProducerConfig::new())
        .await
        .expect("Failed to create producer over TLS");

    for i in 0..5 {
        let record = ProducerRecord::new(topic, bytes::Bytes::from(format!("tls-msg-{}", i)));
        producer
            .send(record)
            .await
            .expect("Failed to produce message via TLS");
    }
    producer.flush().await.expect("Failed to flush producer");
    println!("  Produced 5 messages via TLS");

    let mut consumer = client.consumer(ConsumerConfig::new("tls-test-group").with_earliest());

    consumer
        .subscribe(vec![topic.to_string()])
        .await
        .expect("Failed to subscribe via TLS");

    for i in 0..10 {
        let assignment = consumer.group().assignment().await;
        let has_partitions: usize = assignment.values().map(|v| v.len()).sum();
        if has_partitions > 0 {
            println!("  Consumer joined group after ~{}s", i + 1);
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    let mut all = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while all.len() < 5 && std::time::Instant::now() < deadline {
        match consumer.poll_timeout(Duration::from_millis(3000)).await {
            Ok(records) => all.extend(records),
            Err(e) => eprintln!("  WARNING: Poll error: {}", e),
        }
    }

    println!("  Consumed {}/5 messages via TLS", all.len());
    assert!(
        all.len() >= 5,
        "Expected at least 5 messages, got {}",
        all.len()
    );

    for r in &all {
        let value = String::from_utf8_lossy(&r.value);
        println!("    {}", value);
    }

    if let Err(e) = client.close().await {
        eprintln!("  Close warning: {}", e);
    }
    println!("=== TLS Produce + Consume Test PASSED ===");
}

#[tokio::test]
async fn test_tls_with_ca_cert_verification() {
    setup().await;
    let client = build_tls_client().await;

    client
        .cluster()
        .refresh_metadata()
        .await
        .expect("Failed to refresh metadata with CA cert verification");

    let brokers = client.metadata().get_all_brokers().await;
    println!(
        "  CA-verified TLS (mTLS): {} broker(s) in cluster",
        brokers.len()
    );
    assert!(
        !brokers.is_empty(),
        "Expected at least 1 broker via TLS with CA verification"
    );

    let topic = "tls-ca-verify-test";
    let request = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.to_string(),
            num_partitions: 1,
            replication_factor: 1,
            assignments: vec![],
            configs: vec![],
        }],
        timeout_ms: 10000,
        validate_only: false,
    };

    let response: CreateTopicsResponse = client
        .cluster()
        .send_to_any_broker(&request)
        .await
        .expect("Failed to create topic via CA-verified TLS");
    for t in &response.topics {
        assert!(
            t.error_code == 0 || t.error_code == 36,
            "Create topic '{}' failed: error_code {} ({:?})",
            topic,
            t.error_code,
            t.error_message
        );
    }
    println!("  Topic '{}' created via CA-verified TLS", topic);
    common::wait_for_topic_ready(&client, topic, 1).await;

    let producer = client
        .producer(ProducerConfig::new())
        .await
        .expect("Failed to create producer over CA-verified TLS");

    let record = ProducerRecord::new(topic, bytes::Bytes::from("ca-verified-msg"));
    producer
        .send(record)
        .await
        .expect("Failed to send via CA-verified TLS");
    producer.flush().await.unwrap();
    drop(producer);

    let records = common::consume_all(&client, "cg-tls-ca", topic, 1).await;
    assert!(
        !records.is_empty(),
        "Expected at least 1 record via CA-verified TLS"
    );
    println!(
        "  Consumed message: {}",
        String::from_utf8_lossy(&records[0].value)
    );

    if let Err(e) = client.close().await {
        eprintln!("  Close warning: {}", e);
    }
    println!("=== TLS CA Certificate Verification Test PASSED ===");
}
