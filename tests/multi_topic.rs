//! 多主题生产消费测试
//!
//! 验证生产者可以同时向多个主题发送消息。
//!
//! 运行: KAFKA_RUNTIME=direct cargo test --test multi_topic -- --nocapture

mod common;

use common::{KafkaInstance, default_producer_config};
use kafka_client::client::high_level::{Producer, ProducerRecord};
use kafka_client::client::low_level::KafkaClient;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_produce_to_multiple_topics() {
    let server = KafkaInstance::start().await;
    let client = Arc::new(Mutex::new(
        KafkaClient::connect(server.client_config()).await.unwrap(),
    ));

    {
        let mut c = client.lock().await;
        common::create_topic(&mut c, "tc-multi-a", 2).await;
        common::create_topic(&mut c, "tc-multi-b", 2).await;
    }

    let producer = Producer::new(client.clone(), default_producer_config()).await;
    for i in 0..5 {
        producer
            .send(ProducerRecord::new(
                "tc-multi-a",
                bytes::Bytes::from(format!("a-{}", i)),
            ))
            .await
            .unwrap();
        producer
            .send(ProducerRecord::new(
                "tc-multi-b",
                bytes::Bytes::from(format!("b-{}", i)),
            ))
            .await
            .unwrap();
    }
    producer.flush().await;

    let ra = common::consume_all(&client, "cg-multi-a", "tc-multi-a", 5).await;
    let rb = common::consume_all(&client, "cg-multi-b", "tc-multi-b", 5).await;
    assert_eq!(ra.len(), 5);
    assert_eq!(rb.len(), 5);
}
