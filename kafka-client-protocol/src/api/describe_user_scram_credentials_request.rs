//! Auto-generated from Kafka protocol
//! Message: DescribeUserScramCredentialsRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct UserName {
    /// The user name.
    #[kafka(versions = "0+")]
    pub name: String,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 50,
    msg_type = "request",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct DescribeUserScramCredentialsRequest {
    /// The users to describe, or null/empty to describe all users.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub users: Option<Vec<UserName>>,
}
