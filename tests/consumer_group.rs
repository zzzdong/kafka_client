//! 消费者组分配测试
//!
//! 验证单个消费者加入消费者组后能获得分区分配。
//!
//! 运行: KAFKA_RUNTIME=direct cargo test --test consumer_group -- --nocapture

mod common;

use common::{KafkaInstance, consumer_config};
use kafka_client::client::high_level::AutoOffsetReset;
use kafka_client::client::low_level::KafkaClient;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

#[tokio::test]
async fn test_consumer_group_assignment() {
    let server = KafkaInstance::start().await;
    let client = Arc::new(Mutex::new(
        KafkaClient::connect(server.client_config()).await.unwrap(),
    ));

    {
        let mut c = client.lock().await;
        common::create_topic(&mut c, "tc-group", 3).await;
    }

    // 消费者加入组，验证获得分区分配
    let c1_client = Arc::new(Mutex::new(
        KafkaClient::connect(server.client_config()).await.unwrap(),
    ));

    let mut c1 = kafka_client::client::high_level::Consumer::new(
        c1_client,
        consumer_config("cg-group-test", AutoOffsetReset::Earliest),
    )
    .await;
    c1.subscribe(vec!["tc-group".to_string()]).await.unwrap();

    // 轮询等待组加入
    let assignment = loop {
        let a = c1.assignment().await;
        let total: usize = a.values().map(|v| v.len()).sum();
        if total > 0 {
            break a;
        }
        sleep(Duration::from_secs(1)).await;
    };

    println!("  Consumer assignment: {:?}", assignment);
    let total: usize = assignment.values().map(|v| v.len()).sum();
    assert!(total > 0, "Consumer should have partitions assigned");
    assert_eq!(total, 3, "Should have all 3 partitions");
}
