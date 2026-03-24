//! Auto-generated from Kafka protocol
//! Message: ExpireDelegationTokenRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 40, msg_type = "request", valid_versions = "1-2", flexible_versions = "2+")]
pub struct ExpireDelegationTokenRequest {
    /// The HMAC of the delegation token to be expired.
    #[kafka(versions = "0+")]
    pub hmac: Bytes,
    /// The expiry time period in milliseconds.
    #[kafka(versions = "0+")]
    pub expiry_time_period_ms: i64,
}

