//! DescribeUserScramCredentials API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 50

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// DescribeUserScramCredentialsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 50, valid_versions = "0", flexible_versions = "0+")]
pub struct DescribeUserScramCredentialsRequest {
    #[kafka(versions = "0+")]
    pub users: Vec<DescribeUserScramCredentialsRequestUserName>,
}


/// DescribeUserScramCredentialsRequestUserName
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeUserScramCredentialsRequestUserName {
    #[kafka(versions = "0+")]
    pub name: String,
}
/// DescribeUserScramCredentialsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 50, valid_versions = "0", flexible_versions = "0+")]
pub struct DescribeUserScramCredentialsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub results: Vec<DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult>,
}


/// DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult {
    #[kafka(versions = "0+")]
    pub user: String,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub credential_infos: Vec<DescribeUserScramCredentialsResponseCredentialInfo>,
}

/// DescribeUserScramCredentialsResponseCredentialInfo
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeUserScramCredentialsResponseCredentialInfo {
    #[kafka(versions = "0+")]
    pub mechanism: i8,
    #[kafka(versions = "0+")]
    pub iterations: i32,
}
