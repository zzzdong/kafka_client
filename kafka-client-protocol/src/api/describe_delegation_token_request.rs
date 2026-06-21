//! Auto-generated from Kafka protocol
//! Message: DescribeDelegationTokenRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribeDelegationTokenOwner {
    /// The owner principal type.
    #[kafka(versions = "0+")]
    pub principal_type: String,
    /// The owner principal name.
    #[kafka(versions = "0+")]
    pub principal_name: String,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 41,
    msg_type = "request",
    valid_versions = "1-3",
    flexible_versions = "2+"
)]
pub struct DescribeDelegationTokenRequest {
    /// Each owner that we want to describe delegation tokens for, or null to describe all tokens.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub owners: Option<Vec<DescribeDelegationTokenOwner>>,
}
