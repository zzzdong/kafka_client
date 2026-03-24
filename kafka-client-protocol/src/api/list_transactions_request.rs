//! Auto-generated from Kafka protocol
//! Message: ListTransactionsRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 66, msg_type = "request", valid_versions = "0-2", flexible_versions = "0+")]
pub struct ListTransactionsRequest {
    /// The transaction states to filter by: if empty, all transactions are returned; if non-empty, then only transactions matching one of the filtered states will be returned.
    #[kafka(versions = "0+")]
    pub state_filters: Vec<String>,
    /// The producerIds to filter by: if empty, all transactions will be returned; if non-empty, only transactions which match one of the filtered producerIds will be returned.
    #[kafka(versions = "0+")]
    pub producer_id_filters: Vec<i64>,
    /// Duration (in millis) to filter by: if < 0, all transactions will be returned; otherwise, only transactions running longer than this duration will be returned.
    #[kafka(versions = "1+", default = -1)]
    pub duration_filter: i64,
    /// The transactional ID regular expression pattern to filter by: if it is empty or null, all transactions are returned; Otherwise then only the transactions matching the given regular expression will be returned.
    #[kafka(versions = "2+", nullable_versions = "2+", default = None)]
    pub transactional_id_pattern: Option<String>,
}

