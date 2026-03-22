//! 请求/响应头定义

use crate::protocol::codec::*;
use crate::protocol::error::ProtocolResult;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// 请求头 v1（传统格式）
#[derive(Debug, Clone)]
pub struct RequestHeaderV1 {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl RequestHeaderV1 {
    pub fn new(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<String>,
    ) -> Self {
        Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
        }
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_i16(self.api_key);
        buf.put_i16(self.api_version);
        buf.put_i32(self.correlation_id);
        encode_nullable_string(buf, &self.client_id);
    }
}

/// 请求头 v2（flexible format）
#[derive(Debug, Clone)]
pub struct RequestHeaderV2 {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub tagged_fields: Vec<TaggedField>,
}

impl RequestHeaderV2 {
    pub fn new(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<String>,
    ) -> Self {
        Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
            tagged_fields: Vec::new(),
        }
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_i16(self.api_key);
        buf.put_i16(self.api_version);
        buf.put_i32(self.correlation_id);
        encode_compact_nullable_string(buf, &self.client_id);
        encode_tagged_fields(buf, &[]);
    }
}

/// Tagged field（用于 flexible format 扩展）
#[derive(Debug, Clone)]
pub struct TaggedField {
    pub tag: u32,
    pub data: Bytes,
}

impl TaggedField {
    pub fn new(tag: u32, data: Bytes) -> Self {
        Self { tag, data }
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        encode_unsigned_varint(buf, self.tag);
        encode_unsigned_varint(buf, self.data.len() as u32);
        buf.extend_from_slice(&self.data);
    }

    pub fn decode(buf: &mut Bytes) -> ProtocolResult<Self> {
        let tag = decode_unsigned_varint(buf);
        let len = decode_unsigned_varint(buf) as usize;
        if buf.remaining() < len {
            return Err(crate::protocol::error::ProtocolError::InsufficientData {
                expected: len,
                actual: buf.remaining(),
            });
        }
        let data = buf.copy_to_bytes(len);
        Ok(Self { tag, data })
    }
}

/// 响应头 v0（传统格式）
#[derive(Debug, Clone)]
pub struct ResponseHeaderV0 {
    pub correlation_id: i32,
}

impl ResponseHeaderV0 {
    pub fn decode(buf: &mut Bytes) -> ProtocolResult<Self> {
        if buf.remaining() < 4 {
            return Err(crate::protocol::error::ProtocolError::InsufficientData {
                expected: 4,
                actual: buf.remaining(),
            });
        }
        Ok(Self {
            correlation_id: buf.get_i32(),
        })
    }
}

/// 响应头 v1（flexible format）
#[derive(Debug, Clone)]
pub struct ResponseHeaderV1 {
    pub correlation_id: i32,
    pub tagged_fields: Vec<TaggedField>,
}

impl ResponseHeaderV1 {
    pub fn decode(buf: &mut Bytes) -> ProtocolResult<Self> {
        if buf.remaining() < 4 {
            return Err(crate::protocol::error::ProtocolError::InsufficientData {
                expected: 4,
                actual: buf.remaining(),
            });
        }
        let correlation_id = buf.get_i32();
        let tagged_fields = decode_tagged_fields(buf)?;

        Ok(Self {
            correlation_id,
            tagged_fields,
        })
    }
}

/// 请求头枚举（根据版本选择）
#[derive(Debug, Clone)]
pub enum RequestHeader {
    V1(RequestHeaderV1),
    V2(RequestHeaderV2),
}

impl RequestHeader {
    /// 创建 V1 请求头（传统格式）
    pub fn new_v1(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<String>,
    ) -> Self {
        RequestHeader::V1(RequestHeaderV1::new(
            api_key,
            api_version,
            correlation_id,
            client_id,
        ))
    }

    /// 创建 V2 请求头（flexible 格式）
    pub fn new_v2(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<String>,
    ) -> Self {
        RequestHeader::V2(RequestHeaderV2::new(
            api_key,
            api_version,
            correlation_id,
            client_id,
        ))
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        match self {
            RequestHeader::V1(header) => header.encode(buf),
            RequestHeader::V2(header) => header.encode(buf),
        }
    }

    pub fn correlation_id(&self) -> i32 {
        match self {
            RequestHeader::V1(header) => header.correlation_id,
            RequestHeader::V2(header) => header.correlation_id,
        }
    }

    pub fn api_key(&self) -> i16 {
        match self {
            RequestHeader::V1(header) => header.api_key,
            RequestHeader::V2(header) => header.api_key,
        }
    }

    pub fn api_version(&self) -> i16 {
        match self {
            RequestHeader::V1(header) => header.api_version,
            RequestHeader::V2(header) => header.api_version,
        }
    }
}

/// 响应头枚举
#[derive(Debug, Clone)]
pub enum ResponseHeader {
    V0(ResponseHeaderV0),
    V1(ResponseHeaderV1),
}

impl ResponseHeader {
    pub fn decode(buf: &mut Bytes, use_flexible: bool) -> ProtocolResult<Self> {
        if use_flexible {
            Ok(ResponseHeader::V1(ResponseHeaderV1::decode(buf)?))
        } else {
            Ok(ResponseHeader::V0(ResponseHeaderV0::decode(buf)?))
        }
    }

    pub fn correlation_id(&self) -> i32 {
        match self {
            ResponseHeader::V0(header) => header.correlation_id,
            ResponseHeader::V1(header) => header.correlation_id,
        }
    }
}
