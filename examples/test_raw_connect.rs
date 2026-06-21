use kafka_client::connection::Builder as ConnectionBuilder;
use kafka_client::protocol::MetadataRequest;
use kafka_client::transport::SecurityProtocol;

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let builder = ConnectionBuilder::new(
        "127.0.0.1:9092".parse().unwrap(),
        SecurityProtocol::Plaintext,
        "kafka-client".to_string(),
        "0.1.0".to_string(),
    )
    .with_client_id("test-client".to_string());

    let mut conn = match builder.build().await {
        Ok(c) => {
            tracing::info!("Connection built");
            c
        }
        Err(e) => {
            eprintln!("FAIL: {:?}", e);
            return;
        }
    };

    let meta_req = MetadataRequest {
        topics: None,
        allow_auto_topic_creation: true,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false,
    };

    match conn
        .send_request::<_, kafka_client::protocol::MetadataResponse>(&meta_req)
        .await
    {
        Ok(resp) => println!(
            "OK: {} brokers, cluster_id={:?}",
            resp.brokers.len(),
            resp.cluster_id
        ),
        Err(e) => println!("FAIL: {:?}", e),
    }
}
