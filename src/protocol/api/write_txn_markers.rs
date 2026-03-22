//! WriteTxnMarkers API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 27

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// WriteTxnMarkersRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 27, valid_versions = "1-2", flexible_versions = "1+")]
pub struct WriteTxnMarkersRequest {
    #[kafka(versions = "0+")]
    pub markers: Vec<WriteTxnMarkersRequestWritableTxnMarker>,
}


/// WriteTxnMarkersRequestWritableTxnMarker
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct WriteTxnMarkersRequestWritableTxnMarker {
    #[kafka(versions = "0+")]
    pub producer_id: i64,
    #[kafka(versions = "0+")]
    pub producer_epoch: i16,
    #[kafka(versions = "0+")]
    pub transaction_result: bool,
    #[kafka(versions = "0+")]
    pub topics: Vec<WriteTxnMarkersRequestWritableTxnMarkerTopic>,
    #[kafka(versions = "0+")]
    pub coordinator_epoch: i32,
    #[kafka(versions = "2+")]
    pub transaction_version: i8,
}

/// WriteTxnMarkersRequestWritableTxnMarkerTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct WriteTxnMarkersRequestWritableTxnMarkerTopic {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partition_indexes: Vec<i32>,
}
/// WriteTxnMarkersResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 27, valid_versions = "1-2", flexible_versions = "1+")]
pub struct WriteTxnMarkersResponse {
    #[kafka(versions = "0+")]
    pub markers: Vec<WriteTxnMarkersResponseWritableTxnMarkerResult>,
}


/// WriteTxnMarkersResponseWritableTxnMarkerResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct WriteTxnMarkersResponseWritableTxnMarkerResult {
    #[kafka(versions = "0+")]
    pub producer_id: i64,
    #[kafka(versions = "0+")]
    pub topics: Vec<WriteTxnMarkersResponseWritableTxnMarkerTopicResult>,
}

/// WriteTxnMarkersResponseWritableTxnMarkerTopicResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct WriteTxnMarkersResponseWritableTxnMarkerTopicResult {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<WriteTxnMarkersResponseWritableTxnMarkerPartitionResult>,
}

/// WriteTxnMarkersResponseWritableTxnMarkerPartitionResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct WriteTxnMarkersResponseWritableTxnMarkerPartitionResult {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
}
