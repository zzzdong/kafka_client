//! Auto-generated from Kafka protocol
//! Message: ControlledShutdownResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 7, msg_type = "response", valid_versions = "none")]
pub struct ControlledShutdownResponse {}
