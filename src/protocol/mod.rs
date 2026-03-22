//! Kafka 协议模块
//!
//! 提供 Kafka 协议消息的编解码支持

pub mod api;
pub mod codec;
pub mod collection;
pub mod error;
pub mod header;
pub mod message;
pub mod primitive;
pub mod uuid;
pub mod version;

// 重新导出核心类型
pub use error::{ProtocolError, ProtocolResult};
pub use header::*;
pub use message::{Message, RequestMessage, ResponseMessage, VersionedDecode, VersionedEncode};
pub use uuid::Uuid;
pub use version::VersionRange;

// 从 codec 和 primitive 导出编解码函数
pub use codec::{
    decode_array,
    decode_compact_array,
    decode_compact_bytes,
    decode_compact_nullable_array,
    decode_compact_nullable_bytes,
    decode_compact_nullable_string,
    decode_compact_string,
    decode_nullable_string,
    decode_string,
    decode_tagged_fields,
    decode_unsigned_varint,
    encode_array,
    encode_compact_array,
    encode_compact_bytes,
    encode_compact_nullable_array,
    encode_compact_nullable_bytes,
    encode_compact_nullable_string,
    encode_compact_string,
    encode_nullable_string,
    // 传统格式
    encode_string,
    encode_tagged_fields,
    // Flexible 格式
    encode_unsigned_varint,
    skip_tagged_fields,
};

// 旧版 trait 别名（向后兼容）
pub use message::VersionedDecode as VersionedKafkaDecode;
pub use message::VersionedEncode as VersionedKafkaEncode;

// 导出 derive 宏
pub use kafka_protocol_derive::{KafkaMessage, KafkaRequest, KafkaResponse};
