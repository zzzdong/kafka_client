//! Keytab (FILE v2) parser: extracts the long-term key for a principal from a keytab file.
use crate::error::{KerberosError, Result};
use crate::kerberos::crypto::Etype;
use std::fs;

#[derive(Debug, Clone)]
pub struct KeytabEntry {
    pub principal: String,
    pub realm: String,
    pub timestamp: u32,
    pub kvno: u8,
    pub etype: Etype,
    pub key: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct Keytab {
    pub entries: Vec<KeytabEntry>,
}

impl Keytab {
    pub fn parse_file(path: &str) -> Result<Self> {
        let data =
            fs::read(path).map_err(|e| KerberosError::Keytab(format!("read {path}: {e}")))?;
        Self::parse(&data)
    }

    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < 2 || data[0] != 0x05 || data[1] != 0x02 {
            return Err(KerberosError::Keytab(
                "not a v2 keytab (expected magic 0x0502)".into(),
            ));
        }
        let mut off = 2;
        let mut entries = Vec::new();
        while off + 4 <= data.len() {
            let len = i32::from_be_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]])
                as usize;
            off += 4;
            if len == 0 || len > data.len() - off {
                break;
            }
            if let Ok(e) = parse_entry(&data[off..off + len]) {
                entries.push(e);
            }
            off += len;
        }
        Ok(Self { entries })
    }

    pub fn find(&self, principal: &str, realm: &str) -> Option<&KeytabEntry> {
        self.entries
            .iter()
            .find(|e| e.principal == principal && e.realm == realm)
    }
}

/// 从 `buf` 的 `off` 处读取一个 big-endian `u16`，并推进 `off`。
/// 越界时返回错误而非 panic（防止畸形 keytab 触发索引越界）。
fn read_u16(buf: &[u8], off: &mut usize) -> Result<u16> {
    if *off + 2 > buf.len() {
        return Err(KerberosError::Keytab("truncated keytab entry (u16)".into()));
    }
    let v = u16::from_be_bytes([buf[*off], buf[*off + 1]]);
    *off += 2;
    Ok(v)
}

/// 从 `buf` 的 `off` 处读取一个 big-endian `u32`，并推进 `off`。
fn read_u32(buf: &[u8], off: &mut usize) -> Result<u32> {
    if *off + 4 > buf.len() {
        return Err(KerberosError::Keytab("truncated keytab entry (u32)".into()));
    }
    let v = u32::from_be_bytes([buf[*off], buf[*off + 1], buf[*off + 2], buf[*off + 3]]);
    *off += 4;
    Ok(v)
}

/// 从 `buf` 的 `off` 处读取一个 `u8`，并推进 `off`。
fn read_u8(buf: &[u8], off: &mut usize) -> Result<u8> {
    if *off + 1 > buf.len() {
        return Err(KerberosError::Keytab("truncated keytab entry (u8)".into()));
    }
    let v = buf[*off];
    *off += 1;
    Ok(v)
}

fn parse_entry(buf: &[u8]) -> Result<KeytabEntry> {
    let mut off = 0;
    let num_comp = read_u16(buf, &mut off)? as usize;
    let realm = read_str(buf, &mut off)?;
    let mut comps = Vec::new();
    for _ in 0..num_comp {
        comps.push(read_str(buf, &mut off)?);
    }
    let principal = comps.join("/");
    let _name_type = read_u32(buf, &mut off)?;
    let timestamp = read_u32(buf, &mut off)?;
    let kvno = read_u8(buf, &mut off)?;
    let enctype = read_u16(buf, &mut off)?;
    let etype = Etype::from_u32(enctype as u32)
        .ok_or_else(|| KerberosError::Keytab(format!("unsupported etype {enctype}")))?;
    // key 长度: v2 使用 2 字节长度
    let key_len = read_u16(buf, &mut off)? as usize;
    if off + key_len > buf.len() {
        return Err(KerberosError::Keytab("truncated key".into()));
    }
    let key = buf[off..off + key_len].to_vec();
    Ok(KeytabEntry {
        principal,
        realm,
        timestamp,
        kvno,
        etype,
        key,
    })
}

fn read_str(buf: &[u8], off: &mut usize) -> Result<String> {
    if *off + 2 > buf.len() {
        return Err(KerberosError::Keytab("truncated string length".into()));
    }
    let len = u16::from_be_bytes([buf[*off], buf[*off + 1]]) as usize;
    *off += 2;
    if *off + len > buf.len() {
        return Err(KerberosError::Keytab("truncated string".into()));
    }
    let s = String::from_utf8_lossy(&buf[*off..*off + len]).into_owned();
    *off += len;
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_is_error() {
        assert!(Keytab::parse(&[]).is_err());
    }

    #[test]
    fn parse_magic_only() {
        // 仅有 magic、无条目
        let kt = Keytab::parse(&[0x05, 0x02]).unwrap();
        assert!(kt.entries.is_empty());
    }
}
