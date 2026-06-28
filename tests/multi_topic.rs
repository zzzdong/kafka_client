#![cfg(feature = "integration_tests")]

mod common;

use common::build_test_client;
use common::compose;

async fn setup() {
    compose::ensure(&compose::clusters::THREE_BROKER).await;
}

#[tokio::test]
async fn test_produce_to_multiple_topics() {
    setup().await;
    let client = build_test_client().await;

    common::create_topic(&client, "tc-multi-a", 2).await;
    common::create_topic(&client, "tc-multi-b", 2).await;

    common::produce_messages(&client, "tc-multi-a", 5).await;
    common::produce_messages(&client, "tc-multi-b", 5).await;

    let ra = common::consume_all(&client, "cg-multi-a", "tc-multi-a", 5).await;
    let rb = common::consume_all(&client, "cg-multi-b", "tc-multi-b", 5).await;
    assert_eq!(ra.len(), 5);
    assert_eq!(rb.len(), 5);
}
