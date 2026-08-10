//! Sequential-connection example — demonstrates **L3** serial phase.
//!
//! [`kafka_client::connection::Builder::build_sequential`] performs the
//! ApiVersions handshake and SASL authentication, then hands back a
//! [`kafka_client::connection::SequentialConnection`]. This connection type is
//! strictly **serial**: one request at a time, with strict correlation-ID
//! checking — exactly the guarantee you want during the pre-auth / handshake
//! phase.
//!
//! Once the connection is established you typically promote it to a pipelined,
//! out-of-order [`kafka_client::connection::ConnectionHandle`] via
//! [`into_pipeline`](kafka_client::connection::SequentialConnection::into_pipeline)
//! so multiple requests can be in flight concurrently. This example shows both
//! the serial phase and the hand-off.
//!
//! **Use this when you need:** a strict serial connection for handshake-style
//! sequences, or to run a few requests back-to-back before enabling pipelining.
//!
//! # Usage
//!
//! ```bash
//! # Default: connects to localhost:9092
//! cargo run --example sequential_connection
//!
//! # Custom bootstrap server
//! KAFKA_BOOTSTRAP=192.168.1.100:9092 cargo run --example sequential_connection
//! ```

use kafka_client::connection::Builder as ConnectionBuilder;
use kafka_client::protocol::MetadataRequest;
use kafka_client::transport::SecurityProtocol;
use std::net::SocketAddr;

fn get_bootstrap_addr() -> SocketAddr {
    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    bootstrap
        .parse()
        .expect("Invalid bootstrap address format. Expected: host:port")
}

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let addr = get_bootstrap_addr();
    println!("=== Sequential Connection Example (L3: serial phase) ===");
    println!("Target: {}", addr);
    println!("Protocol: Plaintext (no TLS/SASL)");
    println!();

    // [1] Build a SequentialConnection (ApiVersions handshake + SASL done).
    println!("[1] Building SequentialConnection...");
    let mut seq_conn = match ConnectionBuilder::new(
        addr,
        SecurityProtocol::Plaintext,
        "kafka-client".to_string(),
        "0.1.0".to_string(),
    )
    .with_client_id("sequential-example".to_string())
    .build_sequential()
    .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: Failed to connect: {}", e);
            std::process::exit(1);
        }
    };

    // [2] Send requests strictly serially. Each `send_request` blocks until its
    // own response arrives and the correlation ID matches.
    println!("\n[2] Sending serial MetadataRequest...");
    let meta_req = MetadataRequest {
        topics: None,
        allow_auto_topic_creation: true,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false,
    };
    let resp = match seq_conn
        .send_request::<_, kafka_client::protocol::MetadataResponse>(&meta_req)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("ERROR: Failed to send request: {}", e);
            std::process::exit(1);
        }
    };
    println!("  cluster_id: {:?}", resp.cluster_id);
    println!("  brokers: {}", resp.brokers.len());

    // [3] Promote to a pipelined ConnectionHandle for concurrent requests.
    println!("\n[3] Promoting to pipelined ConnectionHandle...");
    let conn = seq_conn.into_pipeline();
    println!("  now using ConnectionHandle — multiple requests can be in flight");

    // Prove pipelining works: fire two metadata requests without awaiting the
    // first, then collect both. The reactor correlates responses by ID.
    println!("\n[4] Firing two requests concurrently...");
    let f1 = conn.send_request::<_, kafka_client::protocol::MetadataResponse>(&meta_req);
    let f2 = conn.send_request::<_, kafka_client::protocol::MetadataResponse>(&meta_req);
    let (r1, r2) = tokio::join!(f1, f2);
    match (r1, r2) {
        (Ok(a), Ok(b)) => println!(
            "  two concurrent requests OK ({} and {} brokers)",
            a.brokers.len(),
            b.brokers.len()
        ),
        (Err(e), _) | (_, Err(e)) => {
            eprintln!("ERROR: concurrent request failed: {}", e);
            std::process::exit(1);
        }
    }

    println!("\nDone. Connection will be closed on exit.");
}
