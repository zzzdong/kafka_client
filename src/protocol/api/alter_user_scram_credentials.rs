//! AlterUserScramCredentials API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 51

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// AlterUserScramCredentialsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 51, valid_versions = "0", flexible_versions = "0+")]
pub struct AlterUserScramCredentialsRequest {
    #[kafka(versions = "0+")]
    pub deletions: Vec<AlterUserScramCredentialsRequestScramCredentialDeletion>,
    #[kafka(versions = "0+")]
    pub upsertions: Vec<AlterUserScramCredentialsRequestScramCredentialUpsertion>,
}


/// AlterUserScramCredentialsRequestScramCredentialDeletion
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterUserScramCredentialsRequestScramCredentialDeletion {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub mechanism: i8,
}

/// AlterUserScramCredentialsRequestScramCredentialUpsertion
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterUserScramCredentialsRequestScramCredentialUpsertion {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub mechanism: i8,
    #[kafka(versions = "0+")]
    pub iterations: i32,
    #[kafka(versions = "0+")]
    pub salt: Vec<u8>,
    #[kafka(versions = "0+")]
    pub salted_password: Vec<u8>,
}
/// AlterUserScramCredentialsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 51, valid_versions = "0", flexible_versions = "0+")]
pub struct AlterUserScramCredentialsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub results: Vec<AlterUserScramCredentialsResponseAlterUserScramCredentialsResult>,
}


/// AlterUserScramCredentialsResponseAlterUserScramCredentialsResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterUserScramCredentialsResponseAlterUserScramCredentialsResult {
    #[kafka(versions = "0+")]
    pub user: String,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
}
