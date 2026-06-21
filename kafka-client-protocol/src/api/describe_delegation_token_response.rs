//! Auto-generated from Kafka protocol
//! Message: DescribeDelegationTokenResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribedDelegationTokenRenewer {
    /// The renewer principal type.
    #[kafka(versions = "0+")]
    pub principal_type: String,
    /// The renewer principal name.
    #[kafka(versions = "0+")]
    pub principal_name: String,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribedDelegationToken {
    /// The token principal type.
    #[kafka(versions = "0+")]
    pub principal_type: String,
    /// The token principal name.
    #[kafka(versions = "0+")]
    pub principal_name: String,
    /// The principal type of the requester of the token.
    #[kafka(versions = "3+")]
    pub token_requester_principal_type: String,
    /// The principal type of the requester of the token.
    #[kafka(versions = "3+")]
    pub token_requester_principal_name: String,
    /// The token issue timestamp in milliseconds.
    #[kafka(versions = "0+")]
    pub issue_timestamp: i64,
    /// The token expiry timestamp in milliseconds.
    #[kafka(versions = "0+")]
    pub expiry_timestamp: i64,
    /// The token maximum timestamp length in milliseconds.
    #[kafka(versions = "0+")]
    pub max_timestamp: i64,
    /// The token ID.
    #[kafka(versions = "0+")]
    pub token_id: String,
    /// The token HMAC.
    #[kafka(versions = "0+")]
    pub hmac: Bytes,
    /// Those who are able to renew this token before it expires.
    #[kafka(versions = "0+")]
    pub renewers: Vec<DescribedDelegationTokenRenewer>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 41,
    msg_type = "response",
    valid_versions = "1-3",
    flexible_versions = "2+"
)]
pub struct DescribeDelegationTokenResponse {
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The tokens.
    #[kafka(versions = "0+")]
    pub tokens: Vec<DescribedDelegationToken>,
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
}
