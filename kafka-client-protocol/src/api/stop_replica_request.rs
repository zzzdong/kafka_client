//! Auto-generated from Kafka protocol
//! Message: StopReplicaRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 5, msg_type = "request", valid_versions = "none")]
pub struct StopReplicaRequest {}
