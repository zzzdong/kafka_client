//! Auto-generated from Kafka protocol
//! Message: RenewDelegationTokenRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 39, msg_type = "request", valid_versions = "1-2", flexible_versions = "2+")]
pub struct RenewDelegationTokenRequest {
    /// The HMAC of the delegation token to be renewed.
    #[kafka(versions = "0+")]
    pub hmac: Bytes,
    /// The renewal time period in milliseconds.
    #[kafka(versions = "0+")]
    pub renew_period_ms: i64,
}

