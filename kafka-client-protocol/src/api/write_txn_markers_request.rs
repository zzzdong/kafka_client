//! Auto-generated from Kafka protocol
//! Message: WriteTxnMarkersRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct WritableTxnMarkerTopic {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub name: String,
    /// The indexes of the partitions to write transaction markers for.
    #[kafka(versions = "0+")]
    pub partition_indexes: Vec<i32>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct WritableTxnMarker {
    /// The current producer ID.
    #[kafka(versions = "0+")]
    pub producer_id: i64,
    /// The current epoch associated with the producer ID.
    #[kafka(versions = "0+")]
    pub producer_epoch: i16,
    /// The result of the transaction to write to the partitions (false = ABORT, true = COMMIT).
    #[kafka(versions = "0+")]
    pub transaction_result: bool,
    /// Each topic that we want to write transaction marker(s) for.
    #[kafka(versions = "0+")]
    pub topics: Vec<WritableTxnMarkerTopic>,
    /// Epoch associated with the transaction state partition hosted by this transaction coordinator.
    #[kafka(versions = "0+")]
    pub coordinator_epoch: i32,
    /// Transaction version of the marker. Ex: 0/1 = legacy (TV0/TV1), 2 = TV2 etc.
    #[kafka(versions = "2+", default = 0)]
    pub transaction_version: i8,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 27,
    msg_type = "request",
    valid_versions = "1-2",
    flexible_versions = "1+"
)]
pub struct WriteTxnMarkersRequest {
    /// The transaction markers to be written.
    #[kafka(versions = "0+")]
    pub markers: Vec<WritableTxnMarker>,
}
