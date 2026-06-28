#![cfg(feature = "integration_tests")]

mod common;

use common::build_test_client;
use common::compose;

async fn setup() {
    compose::ensure(&compose::clusters::THREE_BROKER).await;
}

#[tokio::test]
async fn test_large_batch() {
    setup().await;
    let client = build_test_client().await;

    common::create_topic(&client, "tc-large", 3).await;
    common::produce_messages(&client, "tc-large", 100).await;

    let records = common::consume_all(&client, "cg-large", "tc-large", 100).await;
    println!("  Consumed {} messages from 'tc-large'", records.len());
    assert!(
        records.len() >= 100,
        "Expected at least 100, got {}",
        records.len()
    );
}
