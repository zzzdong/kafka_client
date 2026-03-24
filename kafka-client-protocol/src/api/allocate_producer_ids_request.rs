//! Auto-generated from Kafka protocol
//! Message: AllocateProducerIdsRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 67, msg_type = "request", valid_versions = "0", flexible_versions = "0+")]
pub struct AllocateProducerIdsRequest {
    /// The ID of the requesting broker.
    #[kafka(versions = "0+")]
    pub broker_id: i32,
    /// The epoch of the requesting broker.
    #[kafka(versions = "0+", default = -1)]
    pub broker_epoch: i64,
}

