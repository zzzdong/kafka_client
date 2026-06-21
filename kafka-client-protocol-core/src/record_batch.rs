// src/record_batch.rs
//! Kafka Record Batch 完整实现
//!
//! 参考: https://kafka.apache.org/documentation/#recordbatch

use crate::error::{ProtocolError, ProtocolResult};
use crate::message::Message;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Kafka Record Batch - 消息批次
///
/// 这是 Kafka 消息格式的核心结构，包含多条 Record
#[derive(Debug, Clone, PartialEq)]
pub struct RecordBatch {
    /// 基础偏移量 (8 bytes)
    pub base_offset: i64,
    /// 批次长度 (4 bytes) - 不包含 base_offset 和 length 本身
    pub batch_length: i32,
    /// 分区 leader epoch (4 bytes)
    pub partition_leader_epoch: i32,
    /// Magic 字节 (1 byte) - 当前版本为 2
    pub magic: i8,
    /// CRC 校验 (4 bytes) - 覆盖从 attributes 到 records 结束的所有数据
    pub crc: u32,
    /// 属性 (2 bytes) - 包含压缩类型等信息
    pub attributes: i16,
    /// 最后一条记录的偏移量增量 (4 bytes)
    pub last_offset_delta: i32,
    /// 第一条记录的时间戳 (8 bytes)
    pub first_timestamp: i64,
    /// 最大时间戳 (8 bytes)
    pub max_timestamp: i64,
    /// 生产者 ID (8 bytes) - 用于幂等性
    pub producer_id: i64,
    /// 生产者 epoch (2 bytes)
    pub producer_epoch: i16,
    /// 基础序列号 (4 bytes)
    pub base_sequence: i32,
    /// 记录数量 (4 bytes)
    pub records_count: i32,
    /// 记录列表
    pub records: Vec<Record>,
}

impl Default for RecordBatch {
    fn default() -> Self {
        Self {
            base_offset: 0,
            batch_length: 0,
            partition_leader_epoch: -1,
            magic: 2,
            crc: 0,
            attributes: 0,
            last_offset_delta: 0,
            first_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records_count: 0,
            records: Vec::new(),
        }
    }
}

/// Kafka Record - 单条消息
///
/// 使用变长编码，包含消息键、值和头信息
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Record {
    /// 消息长度 (varint)
    pub length: i32,
    /// 消息属性 (1 byte)
    pub attributes: i8,
    /// 时间戳增量 (varint)
    pub timestamp_delta: i64,
    /// 偏移量增量 (varint)
    pub offset_delta: i32,
    /// 消息键 (varint bytes, -1 表示 null)
    pub key: Option<Bytes>,
    /// 消息值 (varint bytes, -1 表示 null)
    pub value: Option<Bytes>,
    /// 消息头数量 (varint)
    pub headers_count: i32,
    /// 消息头列表
    pub headers: Vec<Header>,
}

/// 消息头 - 键值对
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Header {
    /// 头键 (varint string)
    pub key: String,
    /// 头值 (varint bytes, -1 表示 null)
    pub value: Option<Bytes>,
}

/// 压缩类型
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(i16)]
pub enum CompressionType {
    /// 无压缩
    None = 0,
    /// GZIP
    Gzip = 1,
    /// Snappy
    Snappy = 2,
    /// LZ4
    Lz4 = 3,
    /// Zstandard
    Zstd = 4,
}

impl Default for CompressionType {
    fn default() -> Self {
        CompressionType::None
    }
}

impl CompressionType {
    /// 从 attributes 提取压缩类型
    pub fn from_attributes(attrs: i16) -> Self {
        match attrs & 0x07 {
            0 => CompressionType::None,
            1 => CompressionType::Gzip,
            2 => CompressionType::Snappy,
            3 => CompressionType::Lz4,
            4 => CompressionType::Zstd,
            _ => CompressionType::None,
        }
    }

    /// 转换为 attributes 中的压缩类型位
    pub fn to_bits(self) -> i16 {
        self as i16
    }
}

// ============================================================================
// RecordBatch 实现
// ============================================================================

impl RecordBatch {
    /// 创建新的 RecordBatch
    pub fn new(base_offset: i64) -> Self {
        Self {
            base_offset,
            magic: 2,
            ..Default::default()
        }
    }

    /// 添加记录
    pub fn add_record(&mut self, record: Record) {
        self.records.push(record);
        self.records_count = self.records.len() as i32;
        self.last_offset_delta = self.records_count - 1;
    }

    /// 设置压缩类型
    pub fn with_compression(mut self, compression: CompressionType) -> Self {
        self.attributes = (self.attributes & !0x07) | compression.to_bits();
        self
    }

    /// 获取压缩类型
    pub fn compression_type(&self) -> CompressionType {
        CompressionType::from_attributes(self.attributes)
    }

    /// 计算 CRC32C（Castagnoli）校验值
    fn calculate_crc(data: &[u8]) -> u32 {
        crc32c::crc32c(data)
    }

    /// 编码为字节数组（用于计算 CRC）
    fn encode_to_bytes(&self) -> ProtocolResult<BytesMut> {
        let mut buf = BytesMut::new();

        // 编码 records
        let mut records_buf = BytesMut::new();
        for record in &self.records {
            record.encode(&mut records_buf)?;
        }

        // 计算 batch_length: 从 partition_leader_epoch 到 records 结束
        let _batch_length = 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + records_buf.len() as i32;

        // 写入 batch 头部（不包含 base_offset 和 batch_length）
        buf.put_i32(self.partition_leader_epoch);
        buf.put_i8(self.magic);
        // CRC 占位，稍后填充
        let crc_position = buf.len();
        buf.put_u32(0);
        buf.put_i16(self.attributes);
        buf.put_i32(self.last_offset_delta);
        buf.put_i64(self.first_timestamp);
        buf.put_i64(self.max_timestamp);
        buf.put_i64(self.producer_id);
        buf.put_i16(self.producer_epoch);
        buf.put_i32(self.base_sequence);
        buf.put_i32(self.records_count);
        buf.extend_from_slice(&records_buf);

        // 计算 CRC：从 attributes 开始（跳过 partition_leader_epoch、magic 和 CRC 本身）
        let crc_data = &buf[9..];
        let crc = Self::calculate_crc(crc_data);

        // 回填 CRC
        let crc_bytes = crc.to_be_bytes();
        buf[crc_position..crc_position + 4].copy_from_slice(&crc_bytes);

        Ok(buf)
    }

    /// 从字节数组解码 RecordBatch（不包含 base_offset 和 batch_length）
    fn decode_from_bytes(buf: &mut Bytes) -> ProtocolResult<Self> {
        if buf.remaining() < 4 {
            return Err(ProtocolError::insufficient_data(4, buf.remaining()));
        }
        let partition_leader_epoch = buf.get_i32();

        if buf.remaining() < 1 {
            return Err(ProtocolError::insufficient_data(1, buf.remaining()));
        }
        let magic = buf.get_i8();

        if magic != 2 {
            return Err(ProtocolError::invalid_data(format!(
                "Unsupported RecordBatch magic: {}, expected 2",
                magic
            )));
        }

        if buf.remaining() < 4 {
            return Err(ProtocolError::insufficient_data(4, buf.remaining()));
        }
        let crc = buf.get_u32();

        // 保存当前位置用于 CRC 校验
        let _data_start = buf.len();

        if buf.remaining() < 2 {
            return Err(ProtocolError::insufficient_data(2, buf.remaining()));
        }
        let attributes = buf.get_i16();

        if buf.remaining() < 4 {
            return Err(ProtocolError::insufficient_data(4, buf.remaining()));
        }
        let last_offset_delta = buf.get_i32();

        if buf.remaining() < 8 {
            return Err(ProtocolError::insufficient_data(8, buf.remaining()));
        }
        let first_timestamp = buf.get_i64();

        if buf.remaining() < 8 {
            return Err(ProtocolError::insufficient_data(8, buf.remaining()));
        }
        let max_timestamp = buf.get_i64();

        if buf.remaining() < 8 {
            return Err(ProtocolError::insufficient_data(8, buf.remaining()));
        }
        let producer_id = buf.get_i64();

        if buf.remaining() < 2 {
            return Err(ProtocolError::insufficient_data(2, buf.remaining()));
        }
        let producer_epoch = buf.get_i16();

        if buf.remaining() < 4 {
            return Err(ProtocolError::insufficient_data(4, buf.remaining()));
        }
        let base_sequence = buf.get_i32();

        if buf.remaining() < 4 {
            return Err(ProtocolError::insufficient_data(4, buf.remaining()));
        }
        let records_count = buf.get_i32();

        // 解码 records
        let mut records = Vec::with_capacity(records_count as usize);
        for _ in 0..records_count {
            records.push(Record::decode(buf)?);
        }

        // TODO: 验证 CRC
        // let data_end = buf.len();
        // let data = &buf[data_end..data_start];
        // let calculated_crc = Self::calculate_crc(data);
        // if calculated_crc != crc {
        //     return Err(ProtocolError::invalid_format("CRC mismatch"));
        // }

        Ok(RecordBatch {
            base_offset: 0,  // 由外层设置
            batch_length: 0, // 由外层计算
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            first_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records_count,
            records,
        })
    }
}

// ============================================================================
// Record 实现
// ============================================================================

impl Record {
    /// 创建新的 Record
    pub fn new(offset_delta: i32, timestamp_delta: i64) -> Self {
        Self {
            offset_delta,
            timestamp_delta,
            ..Default::default()
        }
    }

    /// 设置消息键
    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// 设置消息值
    pub fn with_value(mut self, value: impl Into<Bytes>) -> Self {
        self.value = Some(value.into());
        self
    }

    /// 添加消息头
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.push(Header {
            key: key.into(),
            value: Some(value.into()),
        });
        self.headers_count = self.headers.len() as i32;
        self
    }

    /// 编码 Record
    fn encode(&self, buf: &mut BytesMut) -> ProtocolResult<()> {
        let mut record_buf = BytesMut::new();

        // 属性
        record_buf.put_i8(self.attributes);

        // 时间戳增量 (varint)
        encode_varint(&mut record_buf, self.timestamp_delta);

        // 偏移量增量 (varint)
        encode_varint(&mut record_buf, self.offset_delta as i64);

        // 键 (varint bytes)
        match &self.key {
            Some(key) => {
                encode_varint(&mut record_buf, key.len() as i64);
                record_buf.extend_from_slice(key);
            }
            None => {
                encode_varint(&mut record_buf, -1);
            }
        }

        // 值 (varint bytes)
        match &self.value {
            Some(value) => {
                encode_varint(&mut record_buf, value.len() as i64);
                record_buf.extend_from_slice(value);
            }
            None => {
                encode_varint(&mut record_buf, -1);
            }
        }

        // 头数量 (varint)
        encode_varint(&mut record_buf, self.headers.len() as i64);

        // 头
        for header in &self.headers {
            header.encode(&mut record_buf)?;
        }

        // 写入长度 + 内容
        encode_varint(buf, record_buf.len() as i64);
        buf.extend_from_slice(&record_buf);

        Ok(())
    }

    /// 解码 Record
    fn decode(buf: &mut Bytes) -> ProtocolResult<Self> {
        let length = decode_varint(buf)? as i32;

        let _record_start = buf.len();
        let _expected_end = _record_start.saturating_sub(length as usize);
        let _ = _expected_end; // 暂时未使用

        if buf.remaining() < length as usize {
            return Err(ProtocolError::insufficient_data(
                length as usize,
                buf.remaining(),
            ));
        }

        let attributes = buf.get_i8();
        let timestamp_delta = decode_varint(buf)?;
        let offset_delta = decode_varint(buf)? as i32;

        // 键
        let key_len = decode_varint(buf)?;
        let key = if key_len >= 0 {
            let len = key_len as usize;
            if buf.remaining() < len {
                return Err(ProtocolError::insufficient_data(len, buf.remaining()));
            }
            Some(buf.copy_to_bytes(len))
        } else {
            None
        };

        // 值
        let value_len = decode_varint(buf)?;
        let value = if value_len >= 0 {
            let len = value_len as usize;
            if buf.remaining() < len {
                return Err(ProtocolError::insufficient_data(len, buf.remaining()));
            }
            Some(buf.copy_to_bytes(len))
        } else {
            None
        };

        // 头
        let headers_count = decode_varint(buf)? as i32;
        let mut headers = Vec::with_capacity(headers_count as usize);
        for _ in 0..headers_count {
            headers.push(Header::decode(buf)?);
        }

        Ok(Record {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers_count,
            headers,
        })
    }
}

// ============================================================================
// Header 实现
// ============================================================================

impl Header {
    /// 编码 Header
    fn encode(&self, buf: &mut BytesMut) -> ProtocolResult<()> {
        // 键 (varint string)
        encode_varint(buf, self.key.len() as i64);
        buf.extend_from_slice(self.key.as_bytes());

        // 值 (varint bytes)
        match &self.value {
            Some(value) => {
                encode_varint(buf, value.len() as i64);
                buf.extend_from_slice(value);
            }
            None => {
                encode_varint(buf, -1);
            }
        }

        Ok(())
    }

    /// 解码 Header
    fn decode(buf: &mut Bytes) -> ProtocolResult<Self> {
        // 键
        let key_len = decode_varint(buf)?;
        if key_len < 0 {
            return Err(ProtocolError::invalid_data("Header key cannot be null"));
        }
        let key_len = key_len as usize;
        if buf.remaining() < key_len {
            return Err(ProtocolError::insufficient_data(key_len, buf.remaining()));
        }
        let key = String::from_utf8_lossy(&buf.copy_to_bytes(key_len)).to_string();

        // 值
        let value_len = decode_varint(buf)?;
        let value = if value_len >= 0 {
            let len = value_len as usize;
            if buf.remaining() < len {
                return Err(ProtocolError::insufficient_data(len, buf.remaining()));
            }
            Some(buf.copy_to_bytes(len))
        } else {
            None
        };

        Ok(Header { key, value })
    }
}

// ============================================================================
// Message trait 实现
// ============================================================================

impl Message for RecordBatch {
    fn type_name() -> &'static str {
        "RecordBatch"
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
        // 写入 base_offset
        buf.put_i64(self.base_offset);

        // 编码 batch 内容
        let batch_bytes = self.encode_to_bytes()?;

        // 写入 batch_length
        buf.put_i32(batch_bytes.len() as i32);

        // 写入 batch 内容
        buf.extend_from_slice(&batch_bytes);

        Ok(())
    }

    fn decode(buf: &mut Bytes, _version: i16) -> ProtocolResult<Self> {
        if buf.remaining() < 8 {
            return Err(ProtocolError::insufficient_data(8, buf.remaining()));
        }
        let base_offset = buf.get_i64();

        if buf.remaining() < 4 {
            return Err(ProtocolError::insufficient_data(4, buf.remaining()));
        }
        let batch_length = buf.get_i32();

        if buf.remaining() < batch_length as usize {
            return Err(ProtocolError::insufficient_data(
                batch_length as usize,
                buf.remaining(),
            ));
        }

        let mut batch_data = buf.copy_to_bytes(batch_length as usize);
        let mut batch = Self::decode_from_bytes(&mut batch_data)?;
        batch.base_offset = base_offset;
        batch.batch_length = batch_length;

        Ok(batch)
    }

    fn size(&self, _version: i16) -> usize {
        8 + 4 + self.encode_to_bytes().map(|b| b.len()).unwrap_or(0)
    }
}

// ============================================================================
// 辅助函数
// ============================================================================

/// ZigZag 编码 - 将有符号整数映射到无符号整数
fn zigzag_encode(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

/// ZigZag 解码
fn zigzag_decode(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 1) as i64))
}

/// 编码变长整数
fn encode_varint(buf: &mut BytesMut, value: i64) {
    let mut n = zigzag_encode(value);
    while n >= 0x80 {
        buf.put_u8(((n & 0x7f) as u8) | 0x80);
        n >>= 7;
    }
    buf.put_u8(n as u8);
}

/// 解码变长整数
fn decode_varint(buf: &mut Bytes) -> ProtocolResult<i64> {
    let mut result: u64 = 0;
    let mut shift = 0;

    loop {
        if !buf.has_remaining() {
            return Err(ProtocolError::insufficient_data(1, 0));
        }

        let byte = buf.get_u8();
        result |= ((byte & 0x7f) as u64) << shift;

        if (byte & 0x80) == 0 {
            break;
        }

        shift += 7;
        if shift >= 64 {
            return Err(ProtocolError::invalid_data("Varint too long"));
        }
    }

    Ok(zigzag_decode(result))
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_batch_encode_decode() {
        let mut batch = RecordBatch::new(1000);
        batch.first_timestamp = 1234567890;
        batch.max_timestamp = 1234567890;
        batch.producer_id = 100;
        batch.producer_epoch = 0;
        batch.base_sequence = 0;

        let record = Record::new(0, 0)
            .with_key(b"key1".as_slice())
            .with_value(b"value1".as_slice())
            .with_header("header-key", b"header-value".as_slice());

        batch.add_record(record);

        let mut buf = BytesMut::new();
        batch.encode(&mut buf, 0).unwrap();

        let decoded = RecordBatch::decode(&mut buf.freeze(), 0).unwrap();

        assert_eq!(decoded.base_offset, 1000);
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].key.as_ref().unwrap().as_ref(), b"key1");
        assert_eq!(
            decoded.records[0].value.as_ref().unwrap().as_ref(),
            b"value1"
        );
    }

    #[test]
    fn test_varint() {
        let mut buf = BytesMut::new();
        encode_varint(&mut buf, 0);
        encode_varint(&mut buf, -1);
        encode_varint(&mut buf, 1);
        encode_varint(&mut buf, i64::MAX);
        encode_varint(&mut buf, i64::MIN);

        let mut bytes = Bytes::from(buf.freeze());
        assert_eq!(decode_varint(&mut bytes).unwrap(), 0);
        assert_eq!(decode_varint(&mut bytes).unwrap(), -1);
        assert_eq!(decode_varint(&mut bytes).unwrap(), 1);
        assert_eq!(decode_varint(&mut bytes).unwrap(), i64::MAX);
        assert_eq!(decode_varint(&mut bytes).unwrap(), i64::MIN);
    }
}
