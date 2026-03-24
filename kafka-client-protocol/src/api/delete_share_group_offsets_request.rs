//! Auto-generated from Kafka protocol
//! Message: DeleteShareGroupOffsetsRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DeleteShareGroupOffsetsRequestTopic {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub topic_name: String,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 92, msg_type = "request", valid_versions = "0", flexible_versions = "0+")]
pub struct DeleteShareGroupOffsetsRequest {
    /// The group identifier.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The topics to delete offsets for.
    #[kafka(versions = "0+")]
    pub topics: Vec<DeleteShareGroupOffsetsRequestTopic>,
}

