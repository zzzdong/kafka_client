//! Auto-generated from Kafka protocol
//! Message: CreateDelegationTokenRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct CreatableRenewers {
    /// The type of the Kafka principal.
    #[kafka(versions = "0+")]
    pub principal_type: String,
    /// The name of the Kafka principal.
    #[kafka(versions = "0+")]
    pub principal_name: String,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 38,
    msg_type = "request",
    valid_versions = "1-3",
    flexible_versions = "2+"
)]
pub struct CreateDelegationTokenRequest {
    /// The principal type of the owner of the token. If it's null it defaults to the token request principal.
    #[kafka(versions = "3+", nullable_versions = "3+")]
    pub owner_principal_type: Option<String>,
    /// The principal name of the owner of the token. If it's null it defaults to the token request principal.
    #[kafka(versions = "3+", nullable_versions = "3+")]
    pub owner_principal_name: Option<String>,
    /// A list of those who are allowed to renew this token before it expires.
    #[kafka(versions = "0+")]
    pub renewers: Vec<CreatableRenewers>,
    /// The maximum lifetime of the token in milliseconds, or -1 to use the server side default.
    #[kafka(versions = "0+")]
    pub max_lifetime_ms: i64,
}
