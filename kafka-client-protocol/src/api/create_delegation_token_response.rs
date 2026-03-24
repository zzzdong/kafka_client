//! Auto-generated from Kafka protocol
//! Message: CreateDelegationTokenResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 38, msg_type = "response", valid_versions = "1-3", flexible_versions = "2+")]
pub struct CreateDelegationTokenResponse {
    /// The top-level error, or zero if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The principal type of the token owner.
    #[kafka(versions = "0+")]
    pub principal_type: String,
    /// The name of the token owner.
    #[kafka(versions = "0+")]
    pub principal_name: String,
    /// The principal type of the requester of the token.
    #[kafka(versions = "3+")]
    pub token_requester_principal_type: String,
    /// The principal type of the requester of the token.
    #[kafka(versions = "3+")]
    pub token_requester_principal_name: String,
    /// When this token was generated.
    #[kafka(versions = "0+")]
    pub issue_timestamp_ms: i64,
    /// When this token expires.
    #[kafka(versions = "0+")]
    pub expiry_timestamp_ms: i64,
    /// The maximum lifetime of this token.
    #[kafka(versions = "0+")]
    pub max_timestamp_ms: i64,
    /// The token UUID.
    #[kafka(versions = "0+")]
    pub token_id: String,
    /// HMAC of the delegation token.
    #[kafka(versions = "0+")]
    pub hmac: Bytes,
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
}

