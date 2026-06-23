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
        let size = i32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

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
        dst.put_i32(item.data.len() as i32);
        dst.extend_from_slice(&item.data);
        Ok(())
    }
}
