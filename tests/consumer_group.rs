//! 消费者组分配测试
//!
//! 验证单个消费者加入消费者组后能获得分区分配。
//!
//! 运行: cargo test --test consumer_group --features integration_tests -- --nocapture
//! （需要先启动 docker compose 集群）

#![cfg(feature = "integration_tests")]

mod common;

use common::{build_test_client, consumer_config};
use kafka_client::AutoOffsetReset;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_consumer_group_assignment() {
    let client = build_test_client().await;

    common::create_topic(&client, "tc-group", 3).await;

    // 消费者加入组，验证获得分区分配
    let c1_client = build_test_client().await;

    let mut c1 = c1_client.consumer(consumer_config("cg-group-test", AutoOffsetReset::Earliest));
    c1.subscribe(vec!["tc-group".to_string()]).await.unwrap();

    // 轮询等待组加入
    let assignment = loop {
        let a = c1.group().assignment().await;
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
