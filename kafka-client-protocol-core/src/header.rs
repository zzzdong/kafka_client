// src/header.rs
//! Kafka 协议头定义

use crate::{
    ProtocolError, ProtocolResult, decode_compact_nullable_string, decode_nullable_string,
    decode_unsigned_varint, encode_compact_nullable_string, encode_nullable_string,
    encode_unsigned_varint,
};
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
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_i16(self.api_key);
        buf.put_i16(self.api_version);
        buf.put_i32(self.correlation_id);
        encode_nullable_string(buf, &self.client_id);
    }

    pub fn decode(buf: &mut Bytes) -> ProtocolResult<Self> {
        if buf.remaining() < 6 {
            return Err(ProtocolError::insufficient_data(6, buf.remaining()));
        }
        let api_key = buf.get_i16();
        let api_version = buf.get_i16();
        let correlation_id = buf.get_i32();
        let client_id = decode_nullable_string(buf)?;

        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
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
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_i16(self.api_key);
        buf.put_i16(self.api_version);
        buf.put_i32(self.correlation_id);
        encode_compact_nullable_string(buf, &self.client_id);

        // 编码 tagged fields
        encode_unsigned_varint(buf, self.tagged_fields.len() as u32);
        for field in &self.tagged_fields {
            field.encode(buf);
        }
    }

    pub fn decode(buf: &mut Bytes) -> ProtocolResult<Self> {
        if buf.remaining() < 6 {
            return Err(ProtocolError::insufficient_data(6, buf.remaining()));
        }
        let api_key = buf.get_i16();
        let api_version = buf.get_i16();
        let correlation_id = buf.get_i32();
        let client_id = decode_compact_nullable_string(buf)?;

        let tagged_fields = decode_tagged_fields(buf)?;

        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
            tagged_fields,
        })
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
            return Err(ProtocolError::insufficient_data(4, buf.remaining()));
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
            return Err(ProtocolError::insufficient_data(4, buf.remaining()));
        }
        let correlation_id = buf.get_i32();
        let tagged_fields = decode_tagged_fields(buf)?;

        Ok(Self {
            correlation_id,
            tagged_fields,
        })
    }
}

/// 标签字段
#[derive(Debug, Clone, PartialEq)]
pub struct TaggedField {
    pub tag: u32,
    pub data: Bytes,
}

impl TaggedField {
    pub fn encode(&self, buf: &mut BytesMut) {
        encode_unsigned_varint(buf, self.tag);
        encode_unsigned_varint(buf, self.data.len() as u32);
        buf.extend_from_slice(&self.data);
    }

    pub fn decode(buf: &mut Bytes) -> ProtocolResult<Self> {
        let tag = decode_unsigned_varint(buf);
        let len = decode_unsigned_varint(buf) as usize;
        let data = if len > 0 {
            if buf.remaining() < len {
                return Err(ProtocolError::insufficient_data(len, buf.remaining()));
            }
            buf.copy_to_bytes(len)
        } else {
            Bytes::new()
        };

        Ok(Self { tag, data })
    }
}

/// 解码 tagged fields
pub fn decode_tagged_fields(buf: &mut Bytes) -> ProtocolResult<Vec<TaggedField>> {
    let num_fields = decode_unsigned_varint(buf) as usize;
    let mut fields = Vec::with_capacity(num_fields);
    for _ in 0..num_fields {
        fields.push(TaggedField::decode(buf)?);
    }
    Ok(fields)
}

/// 跳过 tagged fields（用于解码时忽略未知字段）
pub fn skip_tagged_fields(buf: &mut Bytes) {
    let num_fields = decode_unsigned_varint(buf);
    for _ in 0..num_fields {
        let _tag = decode_unsigned_varint(buf);
        let len = decode_unsigned_varint(buf);
        if len > 0 {
            let _ = buf.copy_to_bytes(len as usize);
        }
    }
}

/// 请求头枚举
#[derive(Debug, Clone)]
pub enum RequestHeader {
    V1(RequestHeaderV1),
    V2(RequestHeaderV2),
}

impl RequestHeader {
    pub fn new_v1(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<String>,
    ) -> Self {
        RequestHeader::V1(RequestHeaderV1 {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }

    pub fn new_v2(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<String>,
    ) -> Self {
        RequestHeader::V2(RequestHeaderV2 {
            api_key,
            api_version,
            correlation_id,
            client_id,
            tagged_fields: Vec::new(),
        })
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        match self {
            RequestHeader::V1(h) => h.encode(buf),
            RequestHeader::V2(h) => h.encode(buf),
        }
    }

    pub fn api_key(&self) -> i16 {
        match self {
            RequestHeader::V1(h) => h.api_key,
            RequestHeader::V2(h) => h.api_key,
        }
    }

    pub fn api_version(&self) -> i16 {
        match self {
            RequestHeader::V1(h) => h.api_version,
            RequestHeader::V2(h) => h.api_version,
        }
    }

    pub fn correlation_id(&self) -> i32 {
        match self {
            RequestHeader::V1(h) => h.correlation_id,
            RequestHeader::V2(h) => h.correlation_id,
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
            ResponseHeader::V0(h) => h.correlation_id,
            ResponseHeader::V1(h) => h.correlation_id,
        }
    }
}
