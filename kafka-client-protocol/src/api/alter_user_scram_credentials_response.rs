//! Auto-generated from Kafka protocol
//! Message: AlterUserScramCredentialsResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AlterUserScramCredentialsResult {
    /// The user name.
    #[kafka(versions = "0+")]
    pub user: String,
    /// The error code.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The error message, if any.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 51, msg_type = "response", valid_versions = "0", flexible_versions = "0+")]
pub struct AlterUserScramCredentialsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The results for deletions and alterations, one per affected user.
    #[kafka(versions = "0+")]
    pub results: Vec<AlterUserScramCredentialsResult>,
}

