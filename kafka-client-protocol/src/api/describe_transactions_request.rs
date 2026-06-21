//! Auto-generated from Kafka protocol
//! Message: DescribeTransactionsRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 65,
    msg_type = "request",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct DescribeTransactionsRequest {
    /// Array of transactionalIds to include in describe results. If empty, then no results will be returned.
    #[kafka(versions = "0+")]
    pub transactional_ids: Vec<String>,
}
