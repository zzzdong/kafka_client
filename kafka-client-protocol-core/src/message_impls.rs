// kafka-client-protocol-core/src/message_impls.rs
//! 为基础类型实现 Message trait

use crate::codec::*;
use crate::error::{ProtocolError, ProtocolResult};
use crate::message::Message;
use bytes::{Buf, BufMut, Bytes, BytesMut};

// ============================================================================
// 整数类型
// ============================================================================

macro_rules! impl_message_for_int {
    ($type:ty, $size:expr, $getter:ident, $putter:ident) => {
        impl Message for $type {
            fn type_name() -> &'static str {
                stringify!($type)
            }

            fn max_version() -> i16 {
                i16::MAX
            }

            fn min_version() -> i16 {
                0
            }

            fn flexible_version() -> Option<i16> {
                None
            }

            fn encode(&self, buf: &mut BytesMut, _version: i16) -> ProtocolResult<()> {
                buf.$putter(*self);
                Ok(())
            }

            fn flexible_encode(&self, buf: &mut BytesMut, _version: i16) -> ProtocolResult<()> {
                buf.$putter(*self);
                Ok(())
            }

            fn decode(buf: &mut Bytes, _version: i16) -> ProtocolResult<Self> {
                if buf.remaining() < $size {
                    return Err(ProtocolError::insufficient_data($size, buf.remaining()));
                }
                Ok(buf.$getter())
            }

            fn flexible_decode(buf: &mut Bytes, _version: i16) -> ProtocolResult<Self> {
                if buf.remaining() < $size {
                    return Err(ProtocolError::insufficient_data($size, buf.remaining()));
                }
                Ok(buf.$getter())
            }

            fn size(&self, _version: i16) -> usize {
                $size
            }

            fn flexible_size(&self, _version: i16) -> usize {
                $size
            }
        }
    };
}

impl_message_for_int!(i8, 1, get_i8, put_i8);
impl_message_for_int!(i16, 2, get_i16, put_i16);
impl_message_for_int!(i32, 4, get_i32, put_i32);
impl_message_for_int!(i64, 8, get_i64, put_i64);
impl_message_for_int!(u8, 1, get_u8, put_u8);
impl_message_for_int!(u16, 2, get_u16, put_u16);
impl_message_for_int!(u32, 4, get_u32, put_u32);
impl_message_for_int!(u64, 8, get_u64, put_u64);
impl_message_for_int!(f64, 8, get_f64, put_f64);

// ============================================================================
// bool 类型
// ============================================================================

impl Message for bool {
    fn type_name() -> &'static str {
        "bool"
    }

    fn max_version() -> i16 {
        i16::MAX
    }

    fn min_version() -> i16 {
        0
    }

    fn flexible_version() -> Option<i16> {
        None
    }

    fn encode(&self, buf: &mut BytesMut, _version: i16) -> ProtocolResult<()> {
        buf.put_i8(if *self { 1 } else { 0 });
        Ok(())
    }

    fn flexible_encode(&self, buf: &mut BytesMut, _version: i16) -> ProtocolResult<()> {
        buf.put_i8(if *self { 1 } else { 0 });
        Ok(())
    }

    fn decode(buf: &mut Bytes, _version: i16) -> ProtocolResult<Self> {
        if buf.remaining() < 1 {
            return Err(ProtocolError::insufficient_data(1, buf.remaining()));
        }
        Ok(buf.get_i8() != 0)
    }

    fn flexible_decode(buf: &mut Bytes, _version: i16) -> ProtocolResult<Self> {
        if buf.remaining() < 1 {
            return Err(ProtocolError::insufficient_data(1, buf.remaining()));
        }
        Ok(buf.get_i8() != 0)
    }

    fn size(&self, _version: i16) -> usize {
        1
    }

    fn flexible_size(&self, _version: i16) -> usize {
        1
    }
}

// ============================================================================
// String 类型
// ============================================================================

impl Message for String {
    fn type_name() -> &'static str {
        "String"
    }

    fn max_version() -> i16 {
        i16::MAX
    }

    fn min_version() -> i16 {
        0
    }

    fn flexible_version() -> Option<i16> {
        Some(9)
    }

    fn encode(&self, buf: &mut BytesMut, _version: i16) -> ProtocolResult<()> {
        encode_string(buf, self);
        Ok(())
    }

    fn flexible_encode(&self, buf: &mut BytesMut, _version: i16) -> ProtocolResult<()> {
        encode_compact_string(buf, self);
        Ok(())
    }

    fn decode(buf: &mut Bytes, _version: i16) -> ProtocolResult<Self> {
        decode_string(buf)
    }

    fn flexible_decode(buf: &mut Bytes, _version: i16) -> ProtocolResult<Self> {
        decode_compact_string(buf)
    }

    fn size(&self, _version: i16) -> usize {
        string_size(self)
    }

    fn flexible_size(&self, _version: i16) -> usize {
        compact_string_size(self)
    }
}

// ============================================================================
// Bytes 类型
// ============================================================================

impl Message for Bytes {
    fn type_name() -> &'static str {
        "Bytes"
    }

    fn max_version() -> i16 {
        i16::MAX
    }

    fn min_version() -> i16 {
        0
    }

    fn flexible_version() -> Option<i16> {
        Some(9)
    }

    fn encode(&self, buf: &mut BytesMut, _version: i16) -> ProtocolResult<()> {
        encode_bytes(buf, self);
        Ok(())
    }

    fn flexible_encode(&self, buf: &mut BytesMut, _version: i16) -> ProtocolResult<()> {
        encode_compact_bytes(buf, self);
        Ok(())
    }

    fn decode(buf: &mut Bytes, _version: i16) -> ProtocolResult<Self> {
        decode_bytes(buf)
    }

    fn flexible_decode(buf: &mut Bytes, _version: i16) -> ProtocolResult<Self> {
        decode_compact_bytes(buf)
    }

    fn size(&self, _version: i16) -> usize {
        bytes_size(self)
    }

    fn flexible_size(&self, _version: i16) -> usize {
        compact_bytes_size(self)
    }
}

// ============================================================================
// Option<T> 类型
// ============================================================================

impl<T: Message> Message for Option<T> {
    fn type_name() -> &'static str {
        "Option"
    }

    fn max_version() -> i16 {
        T::max_version()
    }

    fn min_version() -> i16 {
        T::min_version()
    }

    fn flexible_version() -> Option<i16> {
        T::flexible_version()
    }

    fn encode(&self, buf: &mut BytesMut, version: i16) -> ProtocolResult<()> {
        match self {
            Some(v) => v.encode(buf, version),
            None => {
                buf.put_i32(-1);
                Ok(())
            }
        }
    }

    fn flexible_encode(&self, buf: &mut BytesMut, version: i16) -> ProtocolResult<()> {
        match self {
            Some(v) => v.flexible_encode(buf, version),
            None => {
                encode_unsigned_varint(buf, 0);
                Ok(())
            }
        }
    }

    fn decode(buf: &mut Bytes, version: i16) -> ProtocolResult<Self> {
        if buf.remaining() < 4 {
            return Err(ProtocolError::insufficient_data(4, buf.remaining()));
        }
        let len = buf.get_i32();
        if len < 0 {
            Ok(None)
        } else {
            let value = T::decode(buf, version)?;
            Ok(Some(value))
        }
    }

    fn flexible_decode(buf: &mut Bytes, version: i16) -> ProtocolResult<Self> {
        // Compact nullable encoding: varint(0) = null (single 0x00 byte).
        // Non-null values always start with a non-zero varint byte.
        // We can peek at the first byte without consuming it:
        //   - 0x00 → null, consume 1 byte and return None
        //   - otherwise → pass through to T::flexible_decode directly
        if buf.is_empty() {
            return Err(ProtocolError::insufficient_data(1, 0));
        }
        if buf[0] == 0 {
            buf.advance(1);
            return Ok(None);
        }
        let value = T::flexible_decode(buf, version)?;
        Ok(Some(value))
    }

    fn size(&self, version: i16) -> usize {
        match self {
            Some(v) => {
                let inner_size = v.size(version);
                // In standard format, Option has 4-byte null flag
                4 + inner_size
            }
            None => 4,
        }
    }

    fn flexible_size(&self, version: i16) -> usize {
        match self {
            Some(v) => {
                let inner_size = v.flexible_size(version);
                // Need to add the varint prefix overhead that the encoder writes
                // In compact format, Option encodes as varint(inner_size + 1)
                varint_len((inner_size as u32) + 1) + inner_size
            }
            None => 1,
        }
    }
}

// ============================================================================
// Vec<T> 类型
// ============================================================================

impl<T: Message> Message for Vec<T> {
    fn type_name() -> &'static str {
        "Vec"
    }

    fn max_version() -> i16 {
        T::max_version()
    }

    fn min_version() -> i16 {
        T::min_version()
    }

    fn flexible_version() -> Option<i16> {
        T::flexible_version()
    }

    fn encode(&self, buf: &mut BytesMut, version: i16) -> ProtocolResult<()> {
        encode_array(buf, self, |b, item| item.encode(b, version))?;
        Ok(())
    }

    fn flexible_encode(&self, buf: &mut BytesMut, version: i16) -> ProtocolResult<()> {
        encode_compact_array(buf, self, |b, item| item.flexible_encode(b, version))?;
        Ok(())
    }

    fn decode(buf: &mut Bytes, version: i16) -> ProtocolResult<Self> {
        decode_array(buf, |b| T::decode(b, version))
    }

    fn flexible_decode(buf: &mut Bytes, version: i16) -> ProtocolResult<Self> {
        decode_compact_array(buf, |b| T::flexible_decode(b, version))
    }

    fn size(&self, version: i16) -> usize {
        array_size(self, |item| item.size(version))
    }

    fn flexible_size(&self, version: i16) -> usize {
        compact_array_size(self, |item| item.flexible_size(version))
    }
}

// ============================================================================
// Uuid 类型
// ============================================================================

impl Message for uuid::Uuid {
    fn type_name() -> &'static str {
        "Uuid"
    }

    fn max_version() -> i16 {
        i16::MAX
    }

    fn min_version() -> i16 {
        0
    }

    fn flexible_version() -> Option<i16> {
        None
    }

    fn encode(&self, buf: &mut BytesMut, _version: i16) -> ProtocolResult<()> {
        buf.put_slice(self.as_bytes());
        Ok(())
    }

    fn flexible_encode(&self, buf: &mut BytesMut, _version: i16) -> ProtocolResult<()> {
        buf.put_slice(self.as_bytes());
        Ok(())
    }

    fn decode(buf: &mut Bytes, _version: i16) -> ProtocolResult<Self> {
        if buf.remaining() < 16 {
            return Err(ProtocolError::insufficient_data(16, buf.remaining()));
        }
        let mut bytes = [0u8; 16];
        buf.copy_to_slice(&mut bytes);
        Ok(uuid::Uuid::from_bytes(bytes))
    }

    fn flexible_decode(buf: &mut Bytes, _version: i16) -> ProtocolResult<Self> {
        if buf.remaining() < 16 {
            return Err(ProtocolError::insufficient_data(16, buf.remaining()));
        }
        let mut bytes = [0u8; 16];
        buf.copy_to_slice(&mut bytes);
        Ok(uuid::Uuid::from_bytes(bytes))
    }

    fn size(&self, _version: i16) -> usize {
        16
    }

    fn flexible_size(&self, _version: i16) -> usize {
        16
    }
}
