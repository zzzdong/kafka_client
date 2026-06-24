//! Auto-generated from Kafka protocol
//! Message: DescribeLogDirsResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribeLogDirsPartition {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The size of the log segments in this partition in bytes.
    #[kafka(versions = "0+")]
    pub partition_size: i64,
    /// The lag of the log's LEO w.r.t. partition's HW (if it is the current log for the partition) or current replica's LEO (if it is the future log for the partition).
    #[kafka(versions = "0+")]
    pub offset_lag: i64,
    /// True if this log is created by AlterReplicaLogDirsRequest and will replace the current log of the replica in the future.
    #[kafka(versions = "0+")]
    pub is_future_key: bool,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribeLogDirsTopic {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub name: String,
    /// The partitions.
    #[kafka(versions = "0+")]
    pub partitions: Vec<DescribeLogDirsPartition>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribeLogDirsResult {
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The absolute log directory path.
    #[kafka(versions = "0+")]
    pub log_dir: String,
    /// The topics.
    #[kafka(versions = "0+")]
    pub topics: Vec<DescribeLogDirsTopic>,
    /// The total size in bytes of the volume the log directory is in. This value does not include the size of data stored in remote storage.
    #[kafka(versions = "4+", default = -1)]
    pub total_bytes: i64,
    /// The usable size in bytes of the volume the log directory is in. This value does not include the size of data stored in remote storage.
    #[kafka(versions = "4+", default = -1)]
    pub usable_bytes: i64,
    /// True if this log directory is cordoned.
    #[kafka(versions = "5+", default = false)]
    pub is_cordoned: bool,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 35,
    msg_type = "response",
    valid_versions = "1-5",
    flexible_versions = "2+"
)]
pub struct DescribeLogDirsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "3+")]
    pub error_code: i16,
    /// The log directories.
    #[kafka(versions = "0+")]
    pub results: Vec<DescribeLogDirsResult>,
}
