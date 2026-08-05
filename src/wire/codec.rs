//! Kafka frame codec

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Kafka frame
#[derive(Debug, Clone)]
pub struct KafkaFrame {
    pub data: Bytes,
}

impl KafkaFrame {
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }
}

/// Kafka codec for business phase
pub struct KafkaCodec {
    max_frame_size: usize,
}

impl KafkaCodec {
    pub fn new() -> Self {
        Self {
            max_frame_size: 100 * 1024 * 1024,
        }
    }

    pub fn new_with_max_frame_size(max_frame_size: usize) -> Self {
        Self { max_frame_size }
    }
}

impl Default for KafkaCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for KafkaCodec {
    type Item = KafkaFrame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // 1. Check if we have enough bytes for length prefix (4 bytes)
        if src.len() < 4 {
            return Ok(None);
        }

        // 2. Read length
        let raw_size = i32::from_be_bytes([src[0], src[1], src[2], src[3]]);
        if raw_size < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Negative frame size",
            ));
        }
        let size = raw_size as usize;

        // 3. Check size limit
        if size > self.max_frame_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Frame too large",
            ));
        }

        // 4. Check if we have complete frame
        if src.len() < 4 + size {
            src.reserve(4 + size - src.len());
            return Ok(None);
        }

        // 5. Extract frame data
        src.advance(4);
        let data = src.split_to(size).freeze();

        Ok(Some(KafkaFrame { data }))
    }
}

impl Encoder<KafkaFrame> for KafkaCodec {
    type Error = io::Error;

    fn encode(&mut self, item: KafkaFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let len = item.data.len();
        if len > i32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Frame too large to encode",
            ));
        }
        dst.put_i32(len as i32);
        dst.extend_from_slice(&item.data);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let mut codec = KafkaCodec::new();
        let mut buf = BytesMut::new();
        let payload = Bytes::from_static(&[0x00, 0x01, 0x02, 0x03, 0x04]);
        codec
            .encode(KafkaFrame::new(payload.clone()), &mut buf)
            .unwrap();

        let frame = codec.decode(&mut buf).unwrap().expect("complete frame");
        assert_eq!(frame.data, payload);
        assert!(buf.is_empty(), "buffer should be fully consumed");
    }

    #[test]
    fn decode_empty_and_partial_frames() {
        let mut codec = KafkaCodec::new();
        let mut buf = BytesMut::new();
        // No data at all.
        assert!(codec.decode(&mut buf).unwrap().is_none());

        // Only a partial length prefix.
        buf.extend_from_slice(&[0x00, 0x00]);
        assert!(codec.decode(&mut buf).unwrap().is_none());

        // Full length prefix but no payload yet.
        buf.extend_from_slice(&[0x00, 0x02]);
        assert!(codec.decode(&mut buf).unwrap().is_none());

        // Complete the payload.
        buf.extend_from_slice(&[0xaa, 0xbb]);
        let frame = codec.decode(&mut buf).unwrap().expect("complete frame");
        assert_eq!(frame.data.as_ref(), &[0xaa, 0xbb]);
    }

    #[test]
    fn decode_multiple_frames_in_one_buffer() {
        let mut codec = KafkaCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(KafkaFrame::new(Bytes::from_static(&[1])), &mut buf).unwrap();
        codec.encode(KafkaFrame::new(Bytes::from_static(&[2, 3])), &mut buf).unwrap();

        let first = codec.decode(&mut buf).unwrap().expect("first frame");
        let second = codec.decode(&mut buf).unwrap().expect("second frame");
        assert_eq!(first.data.as_ref(), &[1]);
        assert_eq!(second.data.as_ref(), &[2, 3]);
        assert!(buf.is_empty());
    }

    #[test]
    fn decode_rejects_negative_length() {
        let mut codec = KafkaCodec::new();
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&(-1i32).to_be_bytes());
        let err = codec.decode(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn decode_rejects_oversized_frame() {
        let mut codec = KafkaCodec::new_with_max_frame_size(8);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&16i32.to_be_bytes());
        buf.extend_from_slice(&[0u8; 16]);
        let err = codec.decode(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn encode_rejects_oversized_payload() {
        let mut codec = KafkaCodec::new();
        let mut buf = BytesMut::new();
        let too_big = Bytes::from(vec![0u8; i32::MAX as usize + 1]);
        let err = codec.encode(KafkaFrame::new(too_big), &mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn decode_zero_length_frame() {
        let mut codec = KafkaCodec::new();
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&0i32.to_be_bytes());
        let frame = codec.decode(&mut buf).unwrap().expect("empty frame");
        assert!(frame.data.is_empty());
    }
}
