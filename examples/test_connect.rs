use kafka_client::client::low_level::{ClientConfig, KafkaClient};
use kafka_client::transport::SecurityProtocol;

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let config = ClientConfig {
        bootstrap_servers: vec!["127.0.0.1:9092".parse().unwrap()],
        security_protocol: SecurityProtocol::Plaintext,
        client_id: "test-connect".to_string(),
        metadata_ttl: std::time::Duration::from_secs(10),
    };

    println!("=== Connecting to Kafka at 127.0.0.1:9092 ===");
    match KafkaClient::connect(config).await {
        Ok(c) => {
            println!("SUCCESS: Connected!");
            let brokers = c.metadata().get_all_brokers().await;
            println!("Discovered {} brokers:", brokers.len());
            for b in &brokers {
                println!("  Broker {}: {}:{}", b.node_id, b.host, b.port);
            }
        }
        Err(e) => println!("FAILED: {:?}", e),
    }
}
