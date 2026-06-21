//! Auto-generated from Kafka protocol
//! Message: DescribeUserScramCredentialsResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct CredentialInfo {
    /// The SCRAM mechanism.
    #[kafka(versions = "0+")]
    pub mechanism: i8,
    /// The number of iterations used in the SCRAM credential.
    #[kafka(versions = "0+")]
    pub iterations: i32,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribeUserScramCredentialsResult {
    /// The user name.
    #[kafka(versions = "0+")]
    pub user: String,
    /// The user-level error code.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The user-level error message, if any.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
    /// The mechanism and related information associated with the user's SCRAM credentials.
    #[kafka(versions = "0+")]
    pub credential_infos: Vec<CredentialInfo>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 50,
    msg_type = "response",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct DescribeUserScramCredentialsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The message-level error code, 0 except for user authorization or infrastructure issues.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The message-level error message, if any.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
    /// The results for descriptions, one per user.
    #[kafka(versions = "0+")]
    pub results: Vec<DescribeUserScramCredentialsResult>,
}
