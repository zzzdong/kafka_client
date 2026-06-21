//! Auto-generated from Kafka protocol
//! Message: FetchRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ReplicaState {
    /// The replica ID of the follower, or -1 if this request is from a consumer.
    #[kafka(versions = "15+", default = -1)]
    pub replica_id: i32,
    /// The epoch of this follower, or -1 if not available.
    #[kafka(versions = "15+", default = -1)]
    pub replica_epoch: i64,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct FetchPartition {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition: i32,
    /// The current leader epoch of the partition.
    #[kafka(versions = "9+", nullable_versions = "9+", default = -1)]
    pub current_leader_epoch: i32,
    /// The message offset.
    #[kafka(versions = "0+")]
    pub fetch_offset: i64,
    /// The epoch of the last fetched record or -1 if there is none.
    #[kafka(versions = "12+", default = -1)]
    pub last_fetched_epoch: i32,
    /// The earliest available offset of the follower replica.  The field is only used when the request is sent by the follower.
    #[kafka(versions = "5+", nullable_versions = "5+", default = -1)]
    pub log_start_offset: i64,
    /// The maximum bytes to fetch from this partition.  See KIP-74 for cases where this limit may not be honored.
    #[kafka(versions = "0+")]
    pub partition_max_bytes: i32,
    /// The directory id of the follower fetching.
    #[kafka(
        versions = "17+",
        nullable_versions = "17+",
        tag = 0,
        tagged_versions = "17+"
    )]
    pub replica_directory_id: Option<Uuid>,
    /// The high-watermark known by the replica. -1 if the high-watermark is not known and 9223372036854775807 if the feature is not supported.
    #[kafka(
        versions = "18+",
        nullable_versions = "18+",
        tag = 1,
        tagged_versions = "18+",
        default = 9223372036854775807
    )]
    pub high_watermark: i64,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct FetchTopic {
    /// The name of the topic to fetch.
    #[kafka(versions = "0-12", nullable_versions = "0-12")]
    pub topic: Option<String>,
    /// The unique topic ID.
    #[kafka(versions = "13+", nullable_versions = "13+")]
    pub topic_id: Option<Uuid>,
    /// The partitions to fetch.
    #[kafka(versions = "0+")]
    pub partitions: Vec<FetchPartition>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ForgottenTopic {
    /// The topic name.
    #[kafka(versions = "7-12", nullable_versions = "7-12")]
    pub topic: Option<String>,
    /// The unique topic ID.
    #[kafka(versions = "13+", nullable_versions = "13+")]
    pub topic_id: Option<Uuid>,
    /// The partitions indexes to forget.
    #[kafka(versions = "7+")]
    pub partitions: Vec<i32>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 1,
    msg_type = "request",
    valid_versions = "4-18",
    flexible_versions = "12+"
)]
pub struct FetchRequest {
    /// The clusterId if known. This is used to validate metadata fetches prior to broker registration.
    #[kafka(versions = "12+", nullable_versions = "12+", tag = 0, tagged_versions = "12+", default = None)]
    pub cluster_id: Option<String>,
    /// The broker ID of the follower, of -1 if this request is from a consumer.
    #[kafka(versions = "0-14", default = -1)]
    pub replica_id: i32,
    /// The state of the replica in the follower.
    #[kafka(versions = "15+", tag = 1, tagged_versions = "15+")]
    pub replica_state: ReplicaState,
    /// The maximum time in milliseconds to wait for the response.
    #[kafka(versions = "0+")]
    pub max_wait_ms: i32,
    /// The minimum bytes to accumulate in the response.
    #[kafka(versions = "0+")]
    pub min_bytes: i32,
    /// The maximum bytes to fetch.  See KIP-74 for cases where this limit may not be honored.
    #[kafka(versions = "3+", nullable_versions = "3+", default = 0x7fffffff)]
    pub max_bytes: i32,
    /// This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
    #[kafka(versions = "4+", nullable_versions = "4+", default = 0)]
    pub isolation_level: i8,
    /// The fetch session ID.
    #[kafka(versions = "7+", nullable_versions = "7+", default = 0)]
    pub session_id: i32,
    /// The fetch session epoch, which is used for ordering requests in a session.
    #[kafka(versions = "7+", nullable_versions = "7+", default = -1)]
    pub session_epoch: i32,
    /// The topics to fetch.
    #[kafka(versions = "0+")]
    pub topics: Vec<FetchTopic>,
    /// In an incremental fetch request, the partitions to remove.
    #[kafka(versions = "7+")]
    pub forgotten_topics_data: Vec<ForgottenTopic>,
    /// Rack ID of the consumer making this request.
    #[kafka(versions = "11+", nullable_versions = "11+", default = Some(""))]
    pub rack_id: Option<String>,
}
