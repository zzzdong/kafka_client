//! 基础编解码函数

use crate::protocol::Message;
use crate::protocol::error::{ProtocolError, ProtocolResult};
use crate::protocol::header::TaggedField;
use bytes::{Buf, BufMut, Bytes, BytesMut};

// ============= 传统格式 =============

/// 编码字符串（长度 INT16 + UTF-8）
pub fn encode_string(buf: &mut BytesMut, s: &str) {
    buf.put_i16(s.len() as i16);
    buf.put_slice(s.as_bytes());
}

/// 解码字符串
pub fn decode_string(buf: &mut Bytes) -> ProtocolResult<String> {
    if buf.remaining() < 2 {
        return Err(ProtocolError::InsufficientData {
            expected: 2,
            actual: buf.remaining(),
        });
    }
    let len = buf.get_i16();
    if len < 0 {
        return Ok(String::new());
    }
    if buf.remaining() < len as usize {
        return Err(ProtocolError::InsufficientData {
            expected: len as usize,
            actual: buf.remaining(),
        });
    }
    let bytes = buf.copy_to_bytes(len as usize);
    Ok(String::from_utf8_lossy(&bytes).to_string())
}

/// 编码可空字符串（-1 表示 null）
pub fn encode_nullable_string(buf: &mut BytesMut, s: &Option<String>) {
    match s {
        Some(s) => {
            buf.put_i16(s.len() as i16);
            buf.put_slice(s.as_bytes());
        }
        None => {
            buf.put_i16(-1);
        }
    }
}

/// 解码可空字符串
pub fn decode_nullable_string(buf: &mut Bytes) -> ProtocolResult<Option<String>> {
    if buf.remaining() < 2 {
        return Err(ProtocolError::InsufficientData {
            expected: 2,
            actual: buf.remaining(),
        });
    }
    let len = buf.get_i16();
    if len < 0 {
        return Ok(None);
    }
    if buf.remaining() < len as usize {
        return Err(ProtocolError::InsufficientData {
            expected: len as usize,
            actual: buf.remaining(),
        });
    }
    let bytes = buf.copy_to_bytes(len as usize);
    Ok(Some(String::from_utf8_lossy(&bytes).to_string()))
}

/// 编码数组
pub fn encode_array<T, F>(buf: &mut BytesMut, items: &[T], mut encode_item: F)
where
    F: FnMut(&mut BytesMut, &T),
{
    buf.put_i32(items.len() as i32);
    for item in items {
        encode_item(buf, item);
    }
}

pub fn encode_message_array<T, F>(buf: &mut BytesMut, items: &[T], mut encode_item: F) -> ProtocolResult<()>
where
    F: FnMut(&mut BytesMut, &T) -> ProtocolResult<()>,
    T: Message,
{
    buf.put_i32(items.len() as i32);
    for item in items {
        encode_item(buf, item)?;
    }
    Ok(())
}


/// 解码数组
pub fn decode_array<T, F>(buf: &mut Bytes, mut decode_item: F) -> ProtocolResult<Vec<T>>
where
    F: FnMut(&mut Bytes) -> ProtocolResult<T>,
{
    if buf.remaining() < 4 {
        return Err(ProtocolError::InsufficientData {
            expected: 4,
            actual: buf.remaining(),
        });
    }
    let len = buf.get_i32();
    if len < 0 {
        return Ok(Vec::new());
    }

    let mut items = Vec::with_capacity(len as usize);
    for _ in 0..len {
        items.push(decode_item(buf)?);
    }
    Ok(items)
}

// ============= Flexible Format =============

/// 编码无符号变长整数
pub fn encode_unsigned_varint(buf: &mut BytesMut, mut value: u32) {
    while value >= 0x80 {
        buf.put_u8((value & 0x7F) as u8 | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

/// 解码无符号变长整数
pub fn decode_unsigned_varint(buf: &mut Bytes) -> u32 {
    let mut value = 0u32;
    let mut shift = 0;

    loop {
        let byte = buf.get_u8();
        value |= ((byte & 0x7F) as u32) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            break;
        }
    }

    value
}

/// 编码 Compact 字符串（长度 VARINT + 1 + UTF-8）
pub fn encode_compact_string(buf: &mut BytesMut, s: &str) {
    encode_unsigned_varint(buf, s.len() as u32 + 1);
    buf.put_slice(s.as_bytes());
}

/// 解码 Compact 字符串
pub fn decode_compact_string(buf: &mut Bytes) -> ProtocolResult<String> {
    let len = decode_unsigned_varint(buf) as usize;
    if len == 0 {
        return Ok(String::new());
    }
    if buf.remaining() < len - 1 {
        return Err(ProtocolError::InsufficientData {
            expected: len - 1,
            actual: buf.remaining(),
        });
    }
    let bytes = buf.copy_to_bytes(len - 1);
    Ok(String::from_utf8_lossy(&bytes).to_string())
}

/// 编码 Compact 可空字符串（0 表示 null）
pub fn encode_compact_nullable_string(buf: &mut BytesMut, s: &Option<String>) {
    match s {
        Some(s) => {
            encode_unsigned_varint(buf, s.len() as u32 + 1);
            buf.put_slice(s.as_bytes());
        }
        None => {
            encode_unsigned_varint(buf, 0);
        }
    }
}

/// 解码 Compact 可空字符串
pub fn decode_compact_nullable_string(buf: &mut Bytes) -> ProtocolResult<Option<String>> {
    let len = decode_unsigned_varint(buf) as usize;
    if len == 0 {
        return Ok(None);
    }
    if buf.remaining() < len - 1 {
        return Err(ProtocolError::InsufficientData {
            expected: len - 1,
            actual: buf.remaining(),
        });
    }
    let bytes = buf.copy_to_bytes(len - 1);
    Ok(Some(String::from_utf8_lossy(&bytes).to_string()))
}

/// 编码 Compact 数组
pub fn encode_compact_array<T, F>(buf: &mut BytesMut, items: &[T], mut encode_item: F)
where
    F: FnMut(&mut BytesMut, &T),
{
    encode_unsigned_varint(buf, items.len() as u32 + 1);
    for item in items {
        encode_item(buf, item);
    }
}

/// 编码 Compact 数组
pub fn encode_compact_message_array<T, F>(buf: &mut BytesMut, items: &[T], mut encode_item: F) -> ProtocolResult<()>
where
    F: FnMut(&mut BytesMut, &T) -> ProtocolResult<()>,
    T: Message,
{
    encode_unsigned_varint(buf, items.len() as u32 + 1);
    for item in items {
        encode_item(buf, item)?;
    }
    Ok(())
}

/// 解码 Compact 数组
pub fn decode_compact_array<T, F>(buf: &mut Bytes, mut decode_item: F) -> ProtocolResult<Vec<T>>
where
    F: FnMut(&mut Bytes) -> ProtocolResult<T>,
{
    let len = decode_unsigned_varint(buf) as usize;
    if len == 0 {
        return Ok(Vec::new());
    }

    let mut items = Vec::with_capacity(len - 1);
    for _ in 0..len - 1 {
        items.push(decode_item(buf)?);
    }
    Ok(items)
}

/// 编码 Compact 可空数组
pub fn encode_compact_nullable_array<T, F>(
    buf: &mut BytesMut,
    items: &Option<Vec<T>>,
    mut encode_item: F,
) where
    F: FnMut(&mut BytesMut, &T),
{
    match items {
        Some(items) => {
            encode_unsigned_varint(buf, items.len() as u32 + 1);
            for item in items {
                encode_item(buf, item);
            }
        }
        None => {
            encode_unsigned_varint(buf, 0);
        }
    }
}

/// 解码 Compact 可空数组
pub fn decode_compact_nullable_array<T, F>(
    buf: &mut Bytes,
    mut decode_item: F,
) -> ProtocolResult<Option<Vec<T>>>
where
    F: FnMut(&mut Bytes) -> ProtocolResult<T>,
{
    let len = decode_unsigned_varint(buf) as usize;
    if len == 0 {
        return Ok(None);
    }

    let mut items = Vec::with_capacity(len - 1);
    for _ in 0..len - 1 {
        items.push(decode_item(buf)?);
    }
    Ok(Some(items))
}

/// 编码 tagged fields
pub fn encode_tagged_fields(buf: &mut BytesMut, fields: &[(u32, Vec<u8>)]) {
    encode_unsigned_varint(buf, fields.len() as u32);
    for (tag, data) in fields {
        encode_unsigned_varint(buf, *tag);
        encode_unsigned_varint(buf, data.len() as u32);
        buf.extend_from_slice(data);
    }
}

/// 解码 tagged fields
pub fn decode_tagged_fields(buf: &mut Bytes) -> ProtocolResult<Vec<TaggedField>> {
    let num_fields: u32 = decode_unsigned_varint(buf);
    let mut fields = Vec::with_capacity(num_fields as usize);

    for _ in 0..num_fields {
        let tag = decode_unsigned_varint(buf);
        let len = decode_unsigned_varint(buf) as usize;
        if buf.remaining() < len {
            return Err(ProtocolError::InsufficientData {
                expected: len,
                actual: buf.remaining(),
            });
        }
        let data = buf.copy_to_bytes(len);
        fields.push(TaggedField::new(tag, data));
    }

    Ok(fields)
}

/// 跳过 tagged fields（忽略所有）
pub fn skip_tagged_fields(buf: &mut Bytes) -> ProtocolResult<()> {
    let num_fields = decode_unsigned_varint(buf);
    for _ in 0..num_fields {
        let _tag = decode_unsigned_varint(buf);
        let len = decode_unsigned_varint(buf) as usize;
        if buf.remaining() < len {
            return Err(ProtocolError::InsufficientData {
                expected: len,
                actual: buf.remaining(),
            });
        }
        if len > 0 {
            let _ = buf.copy_to_bytes(len);
        }
    }
    Ok(())
}

/// 编码 Compact Bytes
pub fn encode_compact_bytes(buf: &mut BytesMut, data: &[u8]) {
    encode_unsigned_varint(buf, data.len() as u32 + 1);
    buf.extend_from_slice(data);
}

/// 解码 Compact Bytes
pub fn decode_compact_bytes(buf: &mut Bytes) -> ProtocolResult<Bytes> {
    let len = decode_unsigned_varint(buf) as usize;
    if len == 0 {
        return Ok(Bytes::new());
    }
    if buf.remaining() < len - 1 {
        return Err(ProtocolError::InsufficientData {
            expected: len - 1,
            actual: buf.remaining(),
        });
    }
    Ok(buf.copy_to_bytes(len - 1))
}

/// 编码 Compact 可空 Bytes
pub fn encode_compact_nullable_bytes(buf: &mut BytesMut, data: &Option<Vec<u8>>) {
    match data {
        Some(data) => {
            encode_unsigned_varint(buf, data.len() as u32 + 1);
            buf.extend_from_slice(data);
        }
        None => {
            encode_unsigned_varint(buf, 0);
        }
    }
}

/// 解码 Compact 可空 Bytes
pub fn decode_compact_nullable_bytes(buf: &mut Bytes) -> ProtocolResult<Option<Bytes>> {
    let len = decode_unsigned_varint(buf) as usize;
    if len == 0 {
        return Ok(None);
    }
    if buf.remaining() < len - 1 {
        return Err(ProtocolError::InsufficientData {
            expected: len - 1,
            actual: buf.remaining(),
        });
    }
    Ok(Some(buf.copy_to_bytes(len - 1)))
}
