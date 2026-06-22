use bytes::Bytes;
use kafka_client::client::consumer::{AutoOffsetReset, Consumer, ConsumerConfig};
use kafka_client::client::core::{ClientConfig, KafkaClient};
use kafka_client::client::producer::{Producer, ProducerConfig, ProducerRecord};
use kafka_client::protocol::create_topics_request::CreatableTopic;
use kafka_client::protocol::{CreateTopicsRequest, CreateTopicsResponse};
use kafka_client::transport::SecurityProtocol;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let config = ClientConfig {
        bootstrap_servers: vec!["127.0.0.1:9092".parse().unwrap()],
        security_protocol: SecurityProtocol::Plaintext,
        client_id: "test-produce-consume".to_string(),
        metadata_ttl: Duration::from_secs(10),
    };

    let client = std::sync::Arc::new(tokio::sync::Mutex::new(
        KafkaClient::connect(config).await.unwrap(),
    ));

    let topic = "test-topic-v15";

    // Create topic via Admin API
    {
        let create_req = CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.to_string(),
                num_partitions: 3,
                replication_factor: 1,
                assignments: vec![],
                configs: vec![],
            }],
            timeout_ms: 5000,
            validate_only: false,
        };
        let mut c = client.lock().await;
        let addr = c.any_broker_address().unwrap();
        let resp: CreateTopicsResponse = c.send_request(addr, 19, &create_req).await.unwrap();
        for t in &resp.topics {
            println!("  topic {} error_code {}", t.name, t.error_code);
        }
    }

    // 等待 broker 元数据同步并刷新缓存
    tokio::time::sleep(Duration::from_secs(2)).await;
    {
        let mut c = client.lock().await;
        let meta = c.refresh_metadata().await.unwrap();
        println!(
            "metadata: {} brokers, {} topics",
            meta.brokers.len(),
            meta.topics.len()
        );
        for t in &meta.topics {
            println!(
                "  topic {} partitions={}",
                t.name.as_deref().unwrap_or("?"),
                t.partitions.len()
            );
        }
    }

    let producer_config = ProducerConfig {
        acks: 1,
        timeout_ms: 5000,
        retries: 3,
        batch_size: 16384,
        linger_ms: 50,
        ..Default::default()
    };

    let producer = Producer::new(client.clone(), producer_config).await;
    for i in 0..3 {
        let rec = ProducerRecord::new(topic, Bytes::from(format!("hello-{}", i)))
            .with_key(Bytes::from(format!("key-{}", i)));
        producer.send(rec).await.unwrap();
    }
    producer.flush().await;
    println!("Produced 3 messages to {}", topic);

    let consumer_config = ConsumerConfig {
        group_id: "".to_string(),
        auto_commit: true,
        auto_commit_interval_ms: 1000,
        auto_offset_reset: AutoOffsetReset::Earliest,
        min_bytes: 1,
        max_bytes: 1048576,
        partition_max_bytes: 1048576,
        max_wait_ms: 5000,
        session_timeout_ms: 45000,
        rebalance_timeout_ms: 60000,
        heartbeat_interval_ms: 3000,
        partition_assignment_strategy:
            kafka_client::client::consumer::PartitionAssignmentStrategy::Range,
    };

    let mut consumer = Consumer::new(client.clone(), consumer_config).await;
    consumer.subscribe(vec![topic.to_string()]).await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut records = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while records.len() < 3 && std::time::Instant::now() < deadline {
        if let Ok(recs) = consumer.poll(3000).await {
            records.extend(recs);
        }
    }
    println!("Consumed {} messages", records.len());
    for r in &records {
        println!(
            "  partition={} offset={} key={:?} value={:?}",
            r.partition,
            r.offset,
            r.key.as_ref().map(|k| String::from_utf8_lossy(k)),
            String::from_utf8_lossy(&r.value)
        );
    }
}
