// kafka-client-protocol-core/src/codec.rs
//! 基础编解码辅助函数

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::error::{ProtocolError, ProtocolResult};

// ============================================================================
// 变长整数
// ============================================================================

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

/// 计算无符号变长整数编码长度
pub fn varint_len(mut value: u32) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        len += 1;
        value >>= 7;
    }
    len
}

// ============================================================================
// 字符串
// ============================================================================

/// 传统字符串编码（长度 INT16 + UTF-8）
pub fn encode_string(buf: &mut BytesMut, s: &str) {
    buf.put_i16(s.len() as i16);
    buf.put_slice(s.as_bytes());
}

/// 传统字符串解码
pub fn decode_string(buf: &mut Bytes) -> ProtocolResult<String> {
    if buf.remaining() < 2 {
        return Err(ProtocolError::insufficient_data(2, buf.remaining()));
    }
    let len = buf.get_i16();
    if len < 0 {
        return Ok(String::new());
    }
    if buf.remaining() < len as usize {
        return Err(ProtocolError::insufficient_data(len as usize, buf.remaining()));
    }
    let bytes = buf.copy_to_bytes(len as usize);
    Ok(String::from_utf8_lossy(&bytes).to_string())
}

/// 传统可空字符串编码（-1 表示 null）
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

/// 传统可空字符串解码
pub fn decode_nullable_string(buf: &mut Bytes) -> ProtocolResult<Option<String>> {
    if buf.remaining() < 2 {
        return Err(ProtocolError::insufficient_data(2, buf.remaining()));
    }
    let len = buf.get_i16();
    if len < 0 {
        return Ok(None);
    }
    if buf.remaining() < len as usize {
        return Err(ProtocolError::insufficient_data(len as usize, buf.remaining()));
    }
    let bytes = buf.copy_to_bytes(len as usize);
    Ok(Some(String::from_utf8_lossy(&bytes).to_string()))
}

/// Compact 字符串编码（长度 VARINT+1 + UTF-8）
pub fn encode_compact_string(buf: &mut BytesMut, s: &str) {
    encode_unsigned_varint(buf, s.len() as u32 + 1);
    buf.put_slice(s.as_bytes());
}

/// Compact 字符串解码
pub fn decode_compact_string(buf: &mut Bytes) -> ProtocolResult<String> {
    let len = decode_unsigned_varint(buf) as usize;
    if len == 0 {
        return Ok(String::new());
    }
    if buf.remaining() < len - 1 {
        return Err(ProtocolError::insufficient_data(len - 1, buf.remaining()));
    }
    let bytes = buf.copy_to_bytes(len - 1);
    Ok(String::from_utf8_lossy(&bytes).to_string())
}

/// Compact 可空字符串编码（0 表示 null）
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

/// Compact 可空字符串解码
pub fn decode_compact_nullable_string(buf: &mut Bytes) -> ProtocolResult<Option<String>> {
    let len = decode_unsigned_varint(buf) as usize;
    if len == 0 {
        return Ok(None);
    }
    if buf.remaining() < len - 1 {
        return Err(ProtocolError::insufficient_data(len - 1, buf.remaining()));
    }
    let bytes = buf.copy_to_bytes(len - 1);
    Ok(Some(String::from_utf8_lossy(&bytes).to_string()))
}

/// 传统字符串大小
pub fn string_size(s: &str) -> usize {
    2 + s.len()
}

/// Compact 字符串大小
pub fn compact_string_size(s: &str) -> usize {
    varint_len(s.len() as u32 + 1) + s.len()
}

/// 可空字符串大小（传统格式）
pub fn nullable_string_size(s: &Option<String>) -> usize {
    match s {
        Some(s) => 2 + s.len(),
        None => 2,
    }
}

/// 可空字符串大小（Compact 格式）
pub fn compact_nullable_string_size(s: &Option<String>) -> usize {
    match s {
        Some(s) => varint_len(s.len() as u32 + 1) + s.len(),
        None => 1,
    }
}

// ============================================================================
// 字节数组
// ============================================================================

/// 传统字节数组编码（长度 INT32 + 数据）
pub fn encode_bytes(buf: &mut BytesMut, data: &[u8]) {
    buf.put_i32(data.len() as i32);
    buf.extend_from_slice(data);
}

/// 传统字节数组解码
pub fn decode_bytes(buf: &mut Bytes) -> ProtocolResult<Bytes> {
    if buf.remaining() < 4 {
        return Err(ProtocolError::insufficient_data(4, buf.remaining()));
    }
    let len = buf.get_i32();
    if len < 0 {
        return Ok(Bytes::new());
    }
    if buf.remaining() < len as usize {
        return Err(ProtocolError::insufficient_data(len as usize, buf.remaining()));
    }
    Ok(buf.copy_to_bytes(len as usize))
}

/// Compact 字节数组编码（长度 VARINT+1 + 数据）
pub fn encode_compact_bytes(buf: &mut BytesMut, data: &[u8]) {
    encode_unsigned_varint(buf, data.len() as u32 + 1);
    buf.extend_from_slice(data);
}

/// Compact 字节数组解码
pub fn decode_compact_bytes(buf: &mut Bytes) -> ProtocolResult<Bytes> {
    let len = decode_unsigned_varint(buf) as usize;
    if len == 0 {
        return Ok(Bytes::new());
    }
    if buf.remaining() < len - 1 {
        return Err(ProtocolError::insufficient_data(len - 1, buf.remaining()));
    }
    Ok(buf.copy_to_bytes(len - 1))
}

/// 传统字节数组大小
pub fn bytes_size(data: &[u8]) -> usize {
    4 + data.len()
}

/// Compact 字节数组大小
pub fn compact_bytes_size(data: &[u8]) -> usize {
    varint_len(data.len() as u32 + 1) + data.len()
}

// ============================================================================
// 数组
// ============================================================================

/// 传统数组编码（长度 INT32 + 元素）
pub fn encode_array<T, F, E>(buf: &mut BytesMut, items: &[T], mut encode_item: F) -> Result<(), E>
where
    F: FnMut(&mut BytesMut, &T) -> Result<(), E>,
{
    buf.put_i32(items.len() as i32);
    for item in items {
        encode_item(buf, item)?;
    }
    Ok(())
}

/// 传统数组解码
pub fn decode_array<T, F>(buf: &mut Bytes, decode_item: F) -> ProtocolResult<Vec<T>>
where
    F: Fn(&mut Bytes) -> ProtocolResult<T>,
{
    if buf.remaining() < 4 {
        return Err(ProtocolError::insufficient_data(4, buf.remaining()));
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

/// Compact 数组编码（长度 VARINT+1 + 元素）
pub fn encode_compact_array<T, F, E>(
    buf: &mut BytesMut,
    items: &[T],
    mut encode_item: F,
) -> Result<(), E>
where
    F: FnMut(&mut BytesMut, &T) -> Result<(), E>,
{
    encode_unsigned_varint(buf, items.len() as u32 + 1);
    for item in items {
        encode_item(buf, item)?;
    }
    Ok(())
}

/// Compact 数组解码
pub fn decode_compact_array<T, F>(buf: &mut Bytes, decode_item: F) -> ProtocolResult<Vec<T>>
where
    F: Fn(&mut Bytes) -> ProtocolResult<T>,
{
    let len = decode_unsigned_varint(buf) as usize;
    if len == 0 {
        return Ok(Vec::new());
    }
    let count = len - 1;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(decode_item(buf)?);
    }
    Ok(items)
}

/// 传统数组大小
pub fn array_size<T, F>(items: &[T], size_item: F) -> usize
where
    F: Fn(&T) -> usize,
{
    let mut total = 4;
    for item in items {
        total += size_item(item);
    }
    total
}

/// Compact 数组大小
pub fn compact_array_size<T, F>(items: &[T], size_item: F) -> usize
where
    F: Fn(&T) -> usize,
{
    let mut total = varint_len(items.len() as u32 + 1);
    for item in items {
        total += size_item(item);
    }
    total
}