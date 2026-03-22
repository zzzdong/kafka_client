//! DescribeCluster API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 60

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// DescribeClusterRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 60, valid_versions = "0-2", flexible_versions = "0+")]
pub struct DescribeClusterRequest {
    #[kafka(versions = "0+")]
    pub include_cluster_authorized_operations: bool,
    #[kafka(versions = "1+")]
    pub endpoint_type: i8,
    #[kafka(versions = "2+")]
    pub include_fenced_brokers: bool,
}

/// DescribeClusterResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 60, valid_versions = "0-2", flexible_versions = "0+")]
pub struct DescribeClusterResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "1+")]
    pub endpoint_type: i8,
    #[kafka(versions = "0+")]
    pub cluster_id: String,
    #[kafka(versions = "0+")]
    pub controller_id: i32,
    #[kafka(versions = "0+")]
    pub brokers: Vec<DescribeClusterResponseDescribeClusterBroker>,
    #[kafka(versions = "0+")]
    pub cluster_authorized_operations: i32,
}


/// DescribeClusterResponseDescribeClusterBroker
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeClusterResponseDescribeClusterBroker {
    #[kafka(versions = "0+")]
    pub broker_id: i32,
    #[kafka(versions = "0+")]
    pub host: String,
    #[kafka(versions = "0+")]
    pub port: i32,
    #[kafka(versions = "0+")]
    pub rack: String,
    #[kafka(versions = "2+")]
    pub is_fenced: bool,
}
