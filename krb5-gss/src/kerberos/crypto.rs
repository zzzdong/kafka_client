//! Kerberos cryptography core (built on RustCrypto primitives).
//!
//! Implemented and verified by unit tests:
//! - [`nfold`] (RFC 3961 §6.1)
//! - [`string_to_key`] (RFC 3962 / RFC 8009)
//! - AES-CTS (RFC 3962 ciphertext stealing, CS3 variant) encryption/decryption
//! - `EncryptedData` envelope (confounder + CTS + HMAC truncation)
//! - RFC 8009 etypes 19/20 (AES-CTS-HMAC-SHA256/384)
use crate::error::{KerberosError, Result};
use aes::Aes128;
use aes::Aes256;
use aes::cipher::{BlockCipherDecrypt, BlockCipherEncrypt, KeyInit as AesKeyInit};
use cts::{CbcCs3, Decrypt as CtsDecrypt, Encrypt as CtsEncrypt, KeyIvInit as CtsKeyIvInit};
use sha2::{Sha256, Sha384};

/// Kerberos encryption type (etype).
///
/// Supported types:
/// - `Aes128CtsHmacSha196` (17) — RFC 3962
/// - `Aes256CtsHmacSha196` (18) — RFC 3962
/// - `Aes128CtsHmacSha256128` (19) — RFC 8009
/// - `Aes256CtsHmacSha384192` (20) — RFC 8009
///
/// All non-deprecated AES-based etypes are implemented (17-20).
/// Other etypes (DES, DES3, RC4-hmac = 23) are deprecated per RFC 8429
/// and intentionally excluded. They are deliberately left out of the
/// AS-REQ/TGS-REQ etype lists to prevent the KDC from selecting them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Etype {
    Aes128CtsHmacSha196 = 17,
    Aes256CtsHmacSha196 = 18,
    /// AES-128-CTS-HMAC-SHA256-128 (RFC 8009).
    Aes128CtsHmacSha256128 = 19,
    /// AES-256-CTS-HMAC-SHA384-192 (RFC 8009).
    Aes256CtsHmacSha384192 = 20,
}

impl Etype {
    pub fn from_u32(v: u32) -> Option<Self> {
        match v {
            17 => Some(Etype::Aes128CtsHmacSha196),
            18 => Some(Etype::Aes256CtsHmacSha196),
            19 => Some(Etype::Aes128CtsHmacSha256128),
            20 => Some(Etype::Aes256CtsHmacSha384192),
            _ => None,
        }
    }
    pub fn key_len(&self) -> usize {
        match self {
            Etype::Aes128CtsHmacSha196 | Etype::Aes128CtsHmacSha256128 => 16,
            Etype::Aes256CtsHmacSha196 | Etype::Aes256CtsHmacSha384192 => 32,
        }
    }
    /// HMAC truncation length in bytes.
    /// RFC 3962 (etypes 17/18): 96-bit = 12 bytes.
    /// RFC 8009 (etype 19): 128-bit = 16 bytes.
    /// RFC 8009 (etype 20): 192-bit = 24 bytes.
    pub fn mac_len(&self) -> usize {
        match self {
            Etype::Aes128CtsHmacSha196 | Etype::Aes256CtsHmacSha196 => 12,
            Etype::Aes128CtsHmacSha256128 => 16,
            Etype::Aes256CtsHmacSha384192 => 24,
        }
    }
    /// Whether this etype uses the RFC 8009 KDF (HMAC-SHA2 based).
    pub fn is_rfc8009(&self) -> bool {
        matches!(
            self,
            Etype::Aes128CtsHmacSha256128 | Etype::Aes256CtsHmacSha384192
        )
    }
    /// The enctype name string used in RFC 8009 string-to-key salt prefix.
    pub fn rfc8009_name(&self) -> &'static str {
        match self {
            Etype::Aes128CtsHmacSha256128 => "aes128-cts-hmac-sha256-128",
            Etype::Aes256CtsHmacSha384192 => "aes256-cts-hmac-sha384-192",
            _ => unreachable!(),
        }
    }
}

// ------------------------- nfold (RFC 3961 §6.1) -------------------------

fn gcd(a: usize, b: usize) -> usize {
    if b == 0 { a } else { gcd(b, a % b) }
}

fn lcm(a: usize, b: usize) -> usize {
    (a / gcd(a, b)) * b
}

/// 将字节数组右旋转 nbits 位。
fn rotate_right(data: &[u8], nbits: usize) -> Vec<u8> {
    let len = data.len();
    let nbytes = (nbits / 8) % len;
    let remain = nbits % 8;
    let mut result = vec![0u8; len];
    for (i, slot) in result.iter_mut().enumerate() {
        let idx1 = (len + i - nbytes) % len;
        let idx2 = (len + i - nbytes - 1) % len;
        if remain == 0 {
            *slot = data[idx1];
        } else {
            *slot = (data[idx1] >> remain) | (data[idx2] << (8 - remain));
        }
    }
    result
}

/// 1's complement 加法 (end-around carry)。
fn add_ones_complement(a: &[u8], b: &[u8]) -> Vec<u8> {
    let n = a.len();
    let mut v: Vec<u16> = a
        .iter()
        .zip(b.iter())
        .map(|(&x, &y)| x as u16 + y as u16)
        .collect();

    loop {
        let has_carry = v.iter().any(|&x| x > 0xff);
        if !has_carry {
            break;
        }

        let mut new_v = vec![0u16; n];
        for i in 0..n {
            let carry_from_next = v[(i + 1) % n] >> 8;
            new_v[i] = (v[i] & 0xff) + carry_from_next;
        }
        v = new_v;
    }

    v.iter().map(|&x| x as u8).collect()
}

/// 将任意长度输入折叠/扩展为 `size` 比特 (size 必须是 8 的倍数)。
///
/// 实现参考 RFC 3961 §6.1 和 Impacket 的 nfold 实现。
pub fn nfold(data: &[u8], size: usize) -> Vec<u8> {
    assert_eq!(size % 8, 0, "nfold size must be a multiple of 8 bits");
    let out_bytes = size / 8;
    let in_bytes = data.len();

    if in_bytes == 0 {
        return vec![0u8; out_bytes];
    }

    let slen = in_bytes;
    let lcm_val = lcm(out_bytes, slen);

    let mut bigstr = Vec::with_capacity(lcm_val);
    for i in 0..(lcm_val / slen) {
        bigstr.extend_from_slice(&rotate_right(data, 13 * i));
    }

    let mut result = vec![0u8; out_bytes];
    let mut first = true;
    for p in (0..lcm_val).step_by(out_bytes) {
        let slice = &bigstr[p..p + out_bytes];
        if first {
            result.copy_from_slice(slice);
            first = false;
        } else {
            result = add_ones_complement(&result, slice);
        }
    }

    result
}

// ------------------------- string-to-key & HMAC helpers -------------------------

/// HMAC-SHA1 (RFC 2104), implemented via the `hmac` crate.
fn hmac_sha1(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    type HmacSha1 = Hmac<sha1::Sha1>;
    let mut mac = HmacSha1::new_from_slice(key).expect("hmac sha1 key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// HMAC-SHA-256 (RFC 8009 etype 19).
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac sha256 key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// HMAC-SHA-384 (RFC 8009 etype 20).
fn hmac_sha384(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    type HmacSha384 = Hmac<Sha384>;
    let mut mac = HmacSha384::new_from_slice(key).expect("hmac sha384 key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

// ------------------------- KDF-HMAC-SHA2 (RFC 8009 §3) -------------------------

/// KDF-HMAC-SHA2 as defined in RFC 8009 §3.
///
/// ```text
/// K1 = HMAC-SHA-256/384(key, 0x00000001 | label | 0x00 | k)
/// output = k-truncate(K1)
/// ```
///
/// `label` is `usage(4B BE) | suffix(1B)` (same format as RFC 3961).
/// `k` is the desired output length in bits (as 4-byte big-endian).
pub fn kdf_hmac_sha2(etype: Etype, key: &[u8], label: &[u8], out_bits: u32) -> Vec<u8> {
    let mut input = Vec::with_capacity(4 + label.len() + 1 + 4);
    input.extend_from_slice(&1u32.to_be_bytes()); // iteration counter i=1
    input.extend_from_slice(label);
    input.push(0x00); // separator
    input.extend_from_slice(&out_bits.to_be_bytes()); // k
    let k_bytes = (out_bits / 8) as usize;
    let h = match etype {
        Etype::Aes128CtsHmacSha256128 => hmac_sha256(key, &input),
        Etype::Aes256CtsHmacSha384192 => hmac_sha384(key, &input),
        _ => unreachable!(),
    };
    h[..k_bytes].to_vec()
}

/// Derive a per-message key (Kc/Ke/Ki) for RFC 8009 etypes.
///
/// For etype 19: Kc=128, Ke=128, Ki=128 bits.
/// For etype 20: Kc=192, Ke=256, Ki=192 bits.
fn dk_rfc8009(etype: Etype, key: &[u8], constant: &[u8], out_bits: u32) -> Vec<u8> {
    kdf_hmac_sha2(etype, key, constant, out_bits)
}

/// Return the output bit length for the given etype and suffix.
/// - `0x99` (checksum): Kc → 128 (etype 19) or 192 (etype 20)
/// - `0xAA` (encryption): Ke → 128 (etype 19) or 256 (etype 20)
/// - `0x55` (integrity): Ki → 128 (etype 19) or 192 (etype 20)
fn rfc8009_out_bits(etype: Etype, suffix: u8) -> u32 {
    match (etype, suffix) {
        (Etype::Aes128CtsHmacSha256128, 0x99) => 128,
        (Etype::Aes128CtsHmacSha256128, 0xAA) => 128,
        (Etype::Aes128CtsHmacSha256128, 0x55) => 128,
        (Etype::Aes256CtsHmacSha384192, 0x99) => 192,
        (Etype::Aes256CtsHmacSha384192, 0xAA) => 256,
        (Etype::Aes256CtsHmacSha384192, 0x55) => 192,
        _ => unreachable!(),
    }
}

// ------------------------- string-to-key (RFC 3962 / RFC 8009) -------------------------

/// AES string-to-key: dispatches to RFC 3962 or RFC 8009 based on etype.
///
/// For RFC 3962 (etypes 17/18):
///   1. t1 = PBKDF2-HMAC-SHA1(password, salt, iter, key_length)
///   2. K = DK(t1, "kerberos") via AES-based DR
///
/// For RFC 8009 (etypes 19/20):
///   1. saltp = enctype-name | 0x00 | salt
///   2. tkey = PBKDF2-HMAC-SHA256/384(password, saltp, iter, key_length)
///   3. base-key = KDF-HMAC-SHA2(tkey, "kerberos", key_length)
pub fn string_to_key(etype: Etype, password: &[u8], salt: &[u8], iter: u32) -> Result<Vec<u8>> {
    let kl = etype.key_len();
    match etype {
        Etype::Aes128CtsHmacSha196 => {
            let t = pbkdf2_hmac_sha1(password, salt, iter, kl)?;
            Ok(dk_aes_aes128(&t, b"kerberos", kl))
        }
        Etype::Aes256CtsHmacSha196 => {
            let t = pbkdf2_hmac_sha1(password, salt, iter, kl)?;
            Ok(dk_aes_aes(&t, b"kerberos", kl))
        }
        Etype::Aes128CtsHmacSha256128 | Etype::Aes256CtsHmacSha384192 => {
            let hmac_fn: fn(&[u8], &[u8]) -> Vec<u8> = match etype {
                Etype::Aes128CtsHmacSha256128 => hmac_sha256,
                Etype::Aes256CtsHmacSha384192 => hmac_sha384,
                _ => unreachable!(),
            };
            // saltp = enctype-name | 0x00 | salt
            let name = etype.rfc8009_name();
            let mut saltp = Vec::with_capacity(name.len() + 1 + salt.len());
            saltp.extend_from_slice(name.as_bytes());
            saltp.push(0x00);
            saltp.extend_from_slice(salt);
            let tkey = pbkdf2(password, &saltp, iter, kl, hmac_fn)?;
            let kl_bits = (kl as u32) * 8;
            Ok(kdf_hmac_sha2(etype, &tkey, b"kerberos", kl_bits))
        }
    }
}

/// PBKDF2-HMAC-SHA1 (RFC 2898).
fn pbkdf2_hmac_sha1(password: &[u8], salt: &[u8], iter: u32, dk_len: usize) -> Result<Vec<u8>> {
    pbkdf2(password, salt, iter, dk_len, hmac_sha1)
}

/// Generic PBKDF2 implementation.
fn pbkdf2(
    password: &[u8],
    salt: &[u8],
    iter: u32,
    dk_len: usize,
    hmac: impl Fn(&[u8], &[u8]) -> Vec<u8>,
) -> Result<Vec<u8>> {
    let h_len = hmac(password, b"").len();
    let l = dk_len.div_ceil(h_len);
    let mut dk = Vec::with_capacity(l * h_len);
    for i in 1..=l as u32 {
        let mut u = hmac(password, &[salt, &i.to_be_bytes()].concat());
        let mut t = u.clone();
        for _ in 1..iter {
            u = hmac(password, &u);
            for j in 0..h_len {
                t[j] ^= u[j];
            }
        }
        dk.extend_from_slice(&t);
    }
    dk.truncate(dk_len);
    Ok(dk)
}

// ------------------------- AES-CTS (RFC 3962, CS3) -------------------------

fn cts_encrypt<C: BlockCipherEncrypt + BlockCipherDecrypt + AesKeyInit>(
    key: &[u8],
    iv: &[u8],
    data: &[u8],
) -> Result<Vec<u8>> {
    const BS: usize = 16;
    if data.is_empty() {
        return Ok(Vec::new());
    }
    if data.len() <= BS {
        // Single block: AES (ECB mode) per RFC 3962 §5.
        let cipher = <C as AesKeyInit>::new_from_slice(key)
            .map_err(|e| KerberosError::Crypto(format!("invalid key length: {e}")))?;
        let mut block = [0u8; BS];
        block[..data.len()].copy_from_slice(data);
        cipher.encrypt_block(
            (&mut block[..])
                .try_into()
                .map_err(|_| KerberosError::Crypto("internal: block size mismatch".into()))?,
        );
        return Ok(block[..data.len()].to_vec());
    }
    // For exact block-size multiples (> 1 block): CBC + swap last two blocks (RFC 3962 CS3).
    if data.len().is_multiple_of(BS) {
        let cipher = <C as AesKeyInit>::new_from_slice(key)
            .map_err(|e| KerberosError::Crypto(format!("invalid key length: {e}")))?;
        let n_blocks = data.len() / BS;
        let mut out = vec![0u8; data.len()];
        let mut prev = [0u8; BS];
        prev.copy_from_slice(iv);
        for blk in 0..n_blocks {
            let mut buf = [0u8; BS];
            let start = blk * BS;
            buf.copy_from_slice(&data[start..start + BS]);
            for j in 0..BS {
                buf[j] ^= prev[j];
            }
            cipher.encrypt_block(
                (&mut buf[..])
                    .try_into()
                    .map_err(|_| KerberosError::Crypto("internal: block size mismatch".into()))?,
            );
            out[start..start + BS].copy_from_slice(&buf);
            prev = buf;
        }
        // CS3: swap last two blocks
        let last_two_start = out.len() - 2 * BS;
        let mut swapped = out[last_two_start + BS..].to_vec();
        swapped.extend_from_slice(&out[last_two_start..last_two_start + BS]);
        out[last_two_start..].copy_from_slice(&swapped);
        return Ok(out);
    }
    // Non-block-aligned multi-block: use cts crate (CbcCs3, CS3 variant, RFC 3962)
    let cipher = CbcCs3::<C>::new_from_slices(key, iv)
        .map_err(|e| KerberosError::Crypto(format!("invalid key/iv length: {e}")))?;
    let mut out = vec![0u8; data.len()];
    cipher
        .encrypt_b2b(data, &mut out)
        .map_err(|e| KerberosError::Crypto(format!("CTS encrypt failed: {e:?}")))?;
    Ok(out)
}

fn cts_decrypt<C: BlockCipherEncrypt + BlockCipherDecrypt + AesKeyInit>(
    key: &[u8],
    iv: &[u8],
    data: &[u8],
) -> Result<Vec<u8>> {
    const BS: usize = 16;
    if data.is_empty() {
        return Ok(Vec::new());
    }
    if data.len() <= BS {
        // Single block: AES decrypt (ECB mode) per RFC 3962 §5.
        let cipher = <C as AesKeyInit>::new_from_slice(key)
            .map_err(|e| KerberosError::Crypto(format!("invalid key length: {e}")))?;
        let mut block = [0u8; BS];
        block[..data.len()].copy_from_slice(data);
        cipher.decrypt_block(
            (&mut block[..])
                .try_into()
                .map_err(|_| KerberosError::Crypto("internal: block size mismatch".into()))?,
        );
        return Ok(block[..data.len()].to_vec());
    }
    // For exact block-size multiples (> 1 block):
    // undo the CS3 swap first (swap last two blocks back), then CBC decrypt
    if data.len().is_multiple_of(BS) {
        let cipher = <C as AesKeyInit>::new_from_slice(key)
            .map_err(|e| KerberosError::Crypto(format!("invalid key length: {e}")))?;
        let n_blocks = data.len() / BS;
        // Undo CS3 swap: swap last two ciphertext blocks back
        let mut swapped = data.to_vec();
        let last_two_start = swapped.len() - 2 * BS;
        let mut reordered = swapped[last_two_start + BS..].to_vec();
        reordered.extend_from_slice(&swapped[last_two_start..last_two_start + BS]);
        swapped[last_two_start..].copy_from_slice(&reordered);

        // Standard CBC decrypt
        let mut out = vec![0u8; data.len()];
        let mut prev = [0u8; BS];
        prev.copy_from_slice(iv);
        for blk in 0..n_blocks {
            let mut buf = [0u8; BS];
            let start = blk * BS;
            buf.copy_from_slice(&swapped[start..start + BS]);
            cipher.decrypt_block(
                (&mut buf[..])
                    .try_into()
                    .map_err(|_| KerberosError::Crypto("internal: block size mismatch".into()))?,
            );
            for j in 0..BS {
                buf[j] ^= prev[j];
            }
            out[start..start + BS].copy_from_slice(&buf);
            prev.copy_from_slice(&swapped[start..start + BS]);
        }
        return Ok(out);
    }
    // Non-block-aligned multi-block: use cts crate
    let cipher = CbcCs3::<C>::new_from_slices(key, iv)
        .map_err(|e| KerberosError::Crypto(format!("invalid key/iv length: {e}")))?;
    let mut buf = data.to_vec();
    cipher
        .decrypt(&mut buf)
        .map_err(|e| KerberosError::Crypto(format!("CTS decrypt failed: {e:?}")))?;
    Ok(buf)
}

pub fn cts_encrypt_for(etype: Etype, key: &[u8], iv: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    match etype {
        Etype::Aes128CtsHmacSha196 | Etype::Aes128CtsHmacSha256128 => {
            cts_encrypt::<Aes128>(key, iv, data)
        }
        Etype::Aes256CtsHmacSha196 | Etype::Aes256CtsHmacSha384192 => {
            cts_encrypt::<Aes256>(key, iv, data)
        }
    }
}

pub fn cts_decrypt_for(etype: Etype, key: &[u8], iv: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    match etype {
        Etype::Aes128CtsHmacSha196 | Etype::Aes128CtsHmacSha256128 => {
            cts_decrypt::<Aes128>(key, iv, data)
        }
        Etype::Aes256CtsHmacSha196 | Etype::Aes256CtsHmacSha384192 => {
            cts_decrypt::<Aes256>(key, iv, data)
        }
    }
}

// ------------------------- 密钥派生 (RFC 3961 §5.1 + MIT krb5 实现) -------------------------
//
// MIT krb5 的 `k5_derive_random_rfc3961` 使用 AES 分组加密做 DR (不是 HMAC):
//   block = nfold(constant, blocksize)
//   K1 = AES(base_key, block)
//   K2 = AES(base_key, K1)   ← chain 模式: 每次输出作为下次输入
//   ...
//   DR = k-truncate(K1 || K2 || ...)
//
//  (参考: krb5/src/lib/crypto/builtin/kdf.c:141)
//
// MIT krb5 的 Ke/Ki 派生常量为 usage(4B, BE) | suffix(1B):
//   Ke = DK(key, usage | 0xAA)
//   Ki = DK(key, usage | 0x55)
//  (参考: krb5/src/lib/crypto/krb/enc_dk_hmac.c:130-141)

/// AES 加密驱动的 DR 函数 (MIT krb5 兼容).
fn dk_aes_aes(key: &[u8], constant: &[u8], out_len: usize) -> Vec<u8> {
    let bs = 16;
    // nfold constant to block size
    let mut block = nfold(constant, bs * 8);
    let cipher = aes::Aes256::new_from_slice(key).expect("AES-256 key");
    let mut result = Vec::with_capacity(out_len);
    while result.len() < out_len {
        let iv = [0u8; 16];
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&block[..16]);
        for j in 0..16 {
            buf[j] ^= iv[j];
        }
        cipher.encrypt_block((&mut buf[..]).try_into().unwrap());
        let remaining = out_len - result.len();
        let take = remaining.min(16);
        result.extend_from_slice(&buf[..take]);
        block = buf.to_vec();
    }
    result
}

fn dk_aes_aes128(key: &[u8], constant: &[u8], out_len: usize) -> Vec<u8> {
    let bs = 16;
    let mut block = nfold(constant, bs * 8);
    let cipher = aes::Aes128::new_from_slice(key).expect("AES-128 key");
    let mut result = Vec::with_capacity(out_len);
    while result.len() < out_len {
        let iv = [0u8; 16];
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&block[..16]);
        for j in 0..16 {
            buf[j] ^= iv[j];
        }
        cipher.encrypt_block((&mut buf[..]).try_into().unwrap());
        let remaining = out_len - result.len();
        let take = remaining.min(16);
        result.extend_from_slice(&buf[..take]);
        block = buf.to_vec();
    }
    result
}

/// Derive a per-message key: dispatches to RFC 3961 DK or RFC 8009 KDF.
///
/// For RFC 3961 (etypes 17/18): output = key_len bytes (AES-based DR).
/// For RFC 8009 (etypes 19/20): output length determined by suffix byte
/// in the constant (last byte).
pub fn dk_for(etype: Etype, key: &[u8], constant: &[u8]) -> Vec<u8> {
    let kl = etype.key_len();
    match etype {
        Etype::Aes128CtsHmacSha196 => dk_aes_aes128(key, constant, kl),
        Etype::Aes256CtsHmacSha196 => dk_aes_aes(key, constant, kl),
        Etype::Aes128CtsHmacSha256128 | Etype::Aes256CtsHmacSha384192 => {
            // Extract suffix from last byte of constant (usage|suffix format)
            let suffix = *constant.last().unwrap_or(&0);
            let out_bits = rfc8009_out_bits(etype, suffix);
            dk_rfc8009(etype, key, constant, out_bits)
        }
    }
}

/// RFC 3962 DK: n-fold(HMAC(key, constant), key_len * 8)
///
/// Java JDK's Krb5 CipherHelper.dk() uses this approach.
pub fn dk_rfc(etype: Etype, key: &[u8], constant: &[u8]) -> Vec<u8> {
    let out_bits = etype.key_len() * 8;
    let h = hmac_for(etype, key, constant);
    nfold(&h, out_bits)
}

pub fn hmac_for(etype: Etype, key: &[u8], data: &[u8]) -> Vec<u8> {
    match etype {
        Etype::Aes128CtsHmacSha196 | Etype::Aes256CtsHmacSha196 => hmac_sha1(key, data),
        Etype::Aes128CtsHmacSha256128 => hmac_sha256(key, data),
        Etype::Aes256CtsHmacSha384192 => hmac_sha384(key, data),
    }
}

/// Build a DK constant: usage (4-byte big-endian) + suffix (1 byte).
/// Simplified profile (RFC 3961 §5.3):
///   Ke = DK(key, usage | 0xAA)
///   Ki = DK(key, usage | 0x55)
pub fn usage_constant(usage: u32, suffix: u8) -> Vec<u8> {
    let mut v = Vec::with_capacity(5);
    v.extend_from_slice(&usage.to_be_bytes());
    v.push(suffix);
    v
}

// ------------------------- 随机数辅助 -------------------------

/// 生成 16 字节安全随机数 (用于 confounder)。
fn random_16_bytes() -> Result<Vec<u8>> {
    let mut buf = vec![0u8; 16];
    getrandom::fill(&mut buf)
        .map_err(|e| KerberosError::Crypto(format!("failed to generate confounder random: {e}")))?;
    Ok(buf)
}

/// Generate a random key of the appropriate length for the given `etype`.
pub fn random_key(etype: Etype) -> Result<Vec<u8>> {
    let len = etype.key_len();
    let mut buf = vec![0u8; len];
    getrandom::fill(&mut buf)
        .map_err(|e| KerberosError::Crypto(format!("failed to generate random key: {e}")))?;
    Ok(buf)
}

// ------------------------- EncryptedData 封装 (RFC 3962 §5 / RFC 3961 简化配置) -------------------------
//
// 正确顺序 (RFC 3962 §5):
//   1. Ke = DK(key, usage | 0xAA); Ki = DK(key, usage | 0x55)
//   2. ciphertext = AES-CTS(Ke, confounder || plaintext)
//   3. MAC = HMAC(Ki, ciphertext)[:h]
//   4. output = ciphertext || MAC

/// Encrypt — dispatches to RFC 3962 profile (HMAC over plaintext) or
/// RFC 8009 profile (HMAC over ciphertext) based on etype.
///
/// For etypes 17/18 (RFC 3962):
///   1. HMAC = HMAC(Ki, confounder || plaintext)
///   2. C = AES-CTS(Ke, confounder || plaintext)
///   3. Output = C || HMAC[..h]
///
/// For etypes 19/20 (RFC 8009):
///   1. C = AES-CTS(Ke, confounder || plaintext, IV=0)
///   2. H = HMAC(Ki, IV || C)[:h]
///   3. Output = C || H
pub fn encrypt(etype: Etype, key: &[u8], usage: u32, plaintext: &[u8]) -> Result<Vec<u8>> {
    if etype.is_rfc8009() {
        return encrypt_rfc8009(etype, key, usage, plaintext);
    }
    let confounder = random_16_bytes()?;
    let mut basic_plaintext = confounder;
    basic_plaintext.extend_from_slice(plaintext);

    let ke = dk_for(etype, key, &usage_constant(usage, 0xAA));
    let ki = dk_for(etype, key, &usage_constant(usage, 0x55));

    let mac = hmac_for(etype, &ki, &basic_plaintext);

    let iv = vec![0u8; 16];
    let cipher = cts_encrypt_for(etype, &ke, &iv, &basic_plaintext)?;

    let mut out = cipher;
    out.extend_from_slice(&mac[..etype.mac_len()]);
    Ok(out)
}

/// 加密 (RFC 3962 §5): MAC on ciphertext.
/// 用于 GSS-API / Java JRE 互操作场景。
pub fn encrypt_rfc3962(etype: Etype, key: &[u8], usage: u32, plaintext: &[u8]) -> Result<Vec<u8>> {
    let confounder = random_16_bytes()?;
    let mut basic_plaintext = confounder;
    basic_plaintext.extend_from_slice(plaintext);

    let ke = dk_for(etype, key, &usage_constant(usage, 0xAA));
    let ki = dk_for(etype, key, &usage_constant(usage, 0x55));

    // 先加密
    let iv = vec![0u8; 16];
    let cipher = cts_encrypt_for(etype, &ke, &iv, &basic_plaintext)?;

    // 再对密文计算 MAC (RFC 3962 §5: HMAC of ciphertext)
    let mac = hmac_for(etype, &ki, &cipher);

    let mut out = cipher;
    out.extend_from_slice(&mac[..etype.mac_len()]);
    Ok(out)
}

/// 解密 (RFC 3962 §5): 先验 MAC on ciphertext, 再解密。
/// 用于 GSS-API / Java JRE 互操作场景。
pub fn decrypt_rfc3962(etype: Etype, key: &[u8], usage: u32, data: &[u8]) -> Result<Vec<u8>> {
    let mac_len = etype.mac_len();
    if data.len() < mac_len + 16 {
        return Err(KerberosError::Crypto("ciphertext too short".into()));
    }
    let (cipher, mac) = data.split_at(data.len() - mac_len);

    // 先验 HMAC (对密文)
    let ki = dk_for(etype, key, &usage_constant(usage, 0x55));
    let computed = hmac_for(etype, &ki, cipher);
    if &computed[..mac_len] != mac {
        return Err(KerberosError::Crypto("HMAC verification failed".into()));
    }

    // 再解密
    let ke = dk_for(etype, key, &usage_constant(usage, 0xAA));
    let iv = vec![0u8; 16];
    let basic_plaintext = cts_decrypt_for(etype, &ke, &iv, cipher)?;

    if basic_plaintext.len() < 16 {
        return Err(KerberosError::Crypto("decrypt too short".into()));
    }
    Ok(basic_plaintext[16..].to_vec())
}

// ------------------------- RFC 8009 Encrypt/Decrypt (etypes 19/20) -------------------------
//
// RFC 8009 §5:
//   N = random 128-bit confounder
//   C = AES-CBC-CS3(Ke, N || plaintext, IV=0)
//   H = HMAC(Ki, IV || C)[:h]
//   ciphertext = C || H
//
// Decryption:
//   (C, H) = ciphertext
//   verify H == HMAC(Ki, IV || C)[:h]
//   (N, P) = AES-CBC-CS3(Ke, C, IV=0)
//   discard N, return P

/// Encrypt using RFC 8009 profile (HMAC over ciphertext).
pub fn encrypt_rfc8009(etype: Etype, key: &[u8], usage: u32, plaintext: &[u8]) -> Result<Vec<u8>> {
    let confounder = random_16_bytes()?;
    let mut data = confounder;
    data.extend_from_slice(plaintext);

    let ke = dk_for(etype, key, &usage_constant(usage, 0xAA));
    let ki = dk_for(etype, key, &usage_constant(usage, 0x55));

    let iv = vec![0u8; 16];
    let cipher = cts_encrypt_for(etype, &ke, &iv, &data)?;

    // HMAC(Ki, IV || C)[:h]
    let mut mac_input = Vec::with_capacity(16 + cipher.len());
    mac_input.extend_from_slice(&iv);
    mac_input.extend_from_slice(&cipher);
    let mac = hmac_for(etype, &ki, &mac_input);
    let h = etype.mac_len();

    let mut out = cipher;
    out.extend_from_slice(&mac[..h]);
    Ok(out)
}

/// Decrypt using RFC 8009 profile (HMAC verification before decryption).
pub fn decrypt_rfc8009(etype: Etype, key: &[u8], usage: u32, data: &[u8]) -> Result<Vec<u8>> {
    let h = etype.mac_len();
    if data.len() < h + 16 + 1 {
        return Err(KerberosError::Crypto("ciphertext too short".into()));
    }
    let (cipher, mac) = data.split_at(data.len() - h);

    let ki = dk_for(etype, key, &usage_constant(usage, 0x55));
    let iv = vec![0u8; 16];
    let mut mac_input = Vec::with_capacity(16 + cipher.len());
    mac_input.extend_from_slice(&iv);
    mac_input.extend_from_slice(cipher);
    let computed = hmac_for(etype, &ki, &mac_input);
    if &computed[..h] != mac {
        return Err(KerberosError::Crypto(
            "RFC 8009 HMAC verification failed".into(),
        ));
    }

    let ke = dk_for(etype, key, &usage_constant(usage, 0xAA));
    let plain = cts_decrypt_for(etype, &ke, &iv, cipher)?;

    if plain.len() < 16 {
        return Err(KerberosError::Crypto("decrypt too short".into()));
    }
    Ok(plain[16..].to_vec())
}

/// Decrypt — dispatches to RFC 3962 profile (HMAC over plaintext) or
/// RFC 8009 profile (HMAC over ciphertext, verify before decrypt) based on etype.
pub fn decrypt(etype: Etype, key: &[u8], usage: u32, data: &[u8]) -> Result<Vec<u8>> {
    if etype.is_rfc8009() {
        return decrypt_rfc8009(etype, key, usage, data);
    }
    let mac_len = etype.mac_len();
    if data.len() < mac_len + 16 {
        return Err(KerberosError::Crypto("ciphertext too short".into()));
    }
    let (cipher, mac) = data.split_at(data.len() - mac_len);

    let ke = dk_for(etype, key, &usage_constant(usage, 0xAA));
    let iv = vec![0u8; 16];
    let basic_plaintext = cts_decrypt_for(etype, &ke, &iv, cipher)?;

    let ki = dk_for(etype, key, &usage_constant(usage, 0x55));
    let computed = hmac_for(etype, &ki, &basic_plaintext);
    if &computed[..mac_len] != mac {
        return Err(KerberosError::Crypto("HMAC verification failed".into()));
    }

    if basic_plaintext.len() < 16 {
        return Err(KerberosError::Crypto("decrypt too short".into()));
    }
    Ok(basic_plaintext[16..].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== n-fold ====================

    #[test]
    fn nfold_length() {
        assert_eq!(nfold(b"kerberos", 64).len(), 8);
        assert_eq!(nfold(b"kerberos", 128).len(), 16);
        assert_eq!(nfold(b"kerberos", 256).len(), 32);
        assert_eq!(nfold(b"", 64).len(), 8);
    }

    #[test]
    fn nfold_kerberos_128() {
        let result = nfold(b"kerberos", 128);
        let expected: [u8; 16] = [
            0x6b, 0x65, 0x72, 0x62, 0x65, 0x72, 0x6f, 0x73, 0x7b, 0x9b, 0x5b, 0x2b, 0x93, 0x13,
            0x2b, 0x93,
        ];
        assert_eq!(result, expected, "nfold(kerberos, 128) mismatch");
    }

    /// RFC 3961 §A.1 n-fold 测试向量
    #[test]
    fn nfold_rfc3961_vectors() {
        assert_eq!(
            nfold(b"012345", 64),
            &[0xbe, 0x07, 0x26, 0x31, 0x27, 0x6b, 0x19, 0x55][..]
        );
        assert_eq!(
            nfold(b"password", 56),
            &[0x78, 0xa0, 0x7b, 0x6c, 0xaf, 0x85, 0xfa][..]
        );
        assert_eq!(
            nfold(b"Rough Consensus, and Running Code", 64),
            &[0xbb, 0x6e, 0xd3, 0x08, 0x70, 0xb7, 0xf0, 0xe0][..]
        );
        assert_eq!(
            nfold(b"password", 168),
            &[
                0x59, 0xe4, 0xa8, 0xca, 0x7c, 0x03, 0x85, 0xc3, 0xc3, 0x7b, 0x3f, 0x6d, 0x20, 0x00,
                0x24, 0x7c, 0xb6, 0xe6, 0xbd, 0x5b, 0x3e
            ][..]
        );
        assert_eq!(
            nfold(b"MASSACHVSETTS INSTITVTE OF TECHNOLOGY", 192),
            &[
                0xdb, 0x3b, 0x0d, 0x8f, 0x0b, 0x06, 0x1e, 0x60, 0x32, 0x82, 0xb3, 0x08, 0xa5, 0x08,
                0x41, 0x22, 0x9a, 0xd7, 0x98, 0xfa, 0xb9, 0x54, 0x0c, 0x1b
            ][..]
        );
        assert_eq!(
            nfold(b"Q", 168),
            &[
                0x51, 0x8a, 0x54, 0xa2, 0x15, 0xa8, 0x45, 0x2a, 0x51, 0x8a, 0x54, 0xa2, 0x15, 0xa8,
                0x45, 0x2a, 0x51, 0x8a, 0x54, 0xa2, 0x15
            ][..]
        );
        assert_eq!(
            nfold(b"ba", 168),
            &[
                0xfb, 0x25, 0xd5, 0x31, 0xae, 0x89, 0x74, 0x49, 0x9f, 0x52, 0xfd, 0x92, 0xea, 0x98,
                0x57, 0xc4, 0xba, 0x24, 0xcf, 0x29, 0x7e
            ][..]
        );
        assert_eq!(
            nfold(b"kerberos", 256),
            &[
                0x6b, 0x65, 0x72, 0x62, 0x65, 0x72, 0x6f, 0x73, 0x7b, 0x9b, 0x5b, 0x2b, 0x93, 0x13,
                0x2b, 0x93, 0x5c, 0x9b, 0xdc, 0xda, 0xd9, 0x5c, 0x98, 0x99, 0xc4, 0xca, 0xe4, 0xde,
                0xe6, 0xd6, 0xca, 0xe4
            ][..]
        );
    }

    // ==================== AES 驱动 DK 验证 ====================

    #[test]
    fn dk_aes_deterministic() {
        let key = [0x42u8; 32];
        let c = usage_constant(2, 0xAA);
        let ke = dk_aes_aes(&key, &c, 32);
        assert_eq!(ke.len(), 32);
        assert_eq!(dk_aes_aes(&key, &c, 32), ke);
    }

    #[test]
    fn dk_aes_aes128_len() {
        let ke = dk_aes_aes128(&[0x11u8; 16], &usage_constant(1, 0xAA), 16);
        assert_eq!(ke.len(), 16);
    }

    #[test]
    fn dk_aes_aes256_len() {
        let ke = dk_aes_aes(&[0x22u8; 32], &usage_constant(3, 0xAA), 32);
        assert_eq!(ke.len(), 32);
    }

    #[test]
    fn dk_aes_ke_vs_ki() {
        let key = [0x42u8; 32];
        assert_ne!(
            dk_aes_aes(&key, &usage_constant(3, 0xAA), 32),
            dk_aes_aes(&key, &usage_constant(3, 0x55), 32)
        );
    }

    #[test]
    fn dk_aes_different_usage() {
        let key = [0x42u8; 32];
        assert_ne!(
            dk_aes_aes(&key, &usage_constant(1, 0xAA), 32),
            dk_aes_aes(&key, &usage_constant(2, 0xAA), 32)
        );
    }

    // ==================== 加密/解密 roundtrip ====================

    #[test]
    fn aes256_cts_roundtrip() {
        let etype = Etype::Aes256CtsHmacSha196;
        let key = vec![0x42u8; 32];
        let pt = b"hello kerberos gssapi world, this is a longer test plaintext!";
        let ct = encrypt(etype, &key, 2, pt).unwrap();
        assert_eq!(decrypt(etype, &key, 2, &ct).unwrap(), pt);
    }

    #[test]
    fn aes256_cts_roundtrip_partial() {
        let etype = Etype::Aes256CtsHmacSha196;
        let key = vec![0x42u8; 32];
        for pt in [
            &b"short"[..],
            &b"exactly sixteen b"[..],
            &b"twenty bytes exactly!!"[..],
        ] {
            let ct = encrypt(etype, &key, 7, pt).unwrap();
            assert_eq!(
                decrypt(etype, &key, 7, &ct).unwrap(),
                pt,
                "pt len = {}",
                pt.len()
            );
        }
    }

    #[test]
    fn aes128_cts_roundtrip() {
        let etype = Etype::Aes128CtsHmacSha196;
        let key = vec![0x11u8; 16];
        let pt = b"short";
        let ct = encrypt(etype, &key, 2, pt).unwrap();
        assert_eq!(decrypt(etype, &key, 2, &ct).unwrap(), pt);
    }

    /// 验证不同 usage 和不同 etype 的 roundtrip。
    #[test]
    fn encrypt_decrypt_various_usage_etype() {
        let cases: Vec<(Etype, Vec<u8>, u32, &[u8])> = vec![
            (
                Etype::Aes128CtsHmacSha196,
                vec![0x11u8; 16],
                1u32,
                b"test data",
            ),
            (
                Etype::Aes128CtsHmacSha196,
                vec![0x11u8; 16],
                3u32,
                b"longer test payload for aes128",
            ),
            (
                Etype::Aes256CtsHmacSha196,
                vec![0x42u8; 32],
                1u32,
                b"test data",
            ),
            (
                Etype::Aes256CtsHmacSha196,
                vec![0x42u8; 32],
                3u32,
                b"longer test payload for aes256",
            ),
        ];
        for (etype, key, usage, pt) in &cases {
            let ct = encrypt(*etype, key, *usage, pt).unwrap();
            let dec = decrypt(*etype, key, *usage, &ct).unwrap();
            assert_eq!(
                &dec,
                pt,
                "etype={:?} usage={} pt.len={}",
                etype,
                usage,
                pt.len()
            );
        }
    }

    // ==================== 错误检测 ====================

    /// 篡改密文 -> HMAC 校验失败。
    #[test]
    fn hmac_mismatch_detected() {
        let etype = Etype::Aes256CtsHmacSha196;
        let key = vec![0x42u8; 32];
        let mut ct = encrypt(etype, &key, 2, b"payload").unwrap();
        ct[0] ^= 0xff; // 破坏密文
        assert!(decrypt(etype, &key, 2, &ct).is_err());
    }

    /// 篡改 MAC -> HMAC 校验失败。
    #[test]
    fn mac_tampered_detected() {
        let etype = Etype::Aes256CtsHmacSha196;
        let key = vec![0x42u8; 32];
        let mut ct = encrypt(etype, &key, 2, b"payload").unwrap();
        let last = ct.len() - 1;
        ct[last] ^= 0xff; // 破坏 MAC
        assert!(decrypt(etype, &key, 2, &ct).is_err());
    }

    /// 错误的密钥 -> HMAC 校验失败。
    #[test]
    fn wrong_key_detected() {
        let etype = Etype::Aes256CtsHmacSha196;
        let key = vec![0x42u8; 32];
        let wrong_key = vec![0x99u8; 32];
        let ct = encrypt(etype, &key, 2, b"secret").unwrap();
        assert!(decrypt(etype, &wrong_key, 2, &ct).is_err());
    }

    /// 不同 usage 导致不同 MAC -> decrypt 失败。
    #[test]
    fn wrong_usage_detected_now() {
        let etype = Etype::Aes256CtsHmacSha196;
        let key = vec![0x42u8; 32];
        let ct = encrypt(etype, &key, 1, b"secret").unwrap();
        assert!(decrypt(etype, &key, 2, &ct).is_err());
    }

    /// 截断的密文 -> 错误。
    #[test]
    fn truncated_ciphertext_detected() {
        let etype = Etype::Aes256CtsHmacSha196;
        let key = vec![0x42u8; 32];
        let ct = encrypt(etype, &key, 2, b"data").unwrap();
        // 太短（不足 mac_len + 16）
        assert!(decrypt(etype, &key, 2, &ct[..10]).is_err());
    }

    // ==================== string-to-key ====================

    #[test]
    fn string_to_key_deterministic() {
        let k = string_to_key(
            Etype::Aes256CtsHmacSha196,
            b"password",
            b"ATHENA.MIT.EDUraeburn",
            4096,
        )
        .unwrap();
        assert_eq!(k.len(), 32);
        // 相同输入产生相同输出
        let k2 = string_to_key(
            Etype::Aes256CtsHmacSha196,
            b"password",
            b"ATHENA.MIT.EDUraeburn",
            4096,
        )
        .unwrap();
        assert_eq!(k, k2);
    }

    #[test]
    fn string_to_key_aes128_length() {
        let k = string_to_key(
            Etype::Aes128CtsHmacSha196,
            b"password",
            b"ATHENA.MIT.EDUraeburn",
            4096,
        )
        .unwrap();
        assert_eq!(k.len(), 16);
    }

    #[test]
    fn string_to_key_different_salt() {
        let k1 = string_to_key(Etype::Aes256CtsHmacSha196, b"password", b"salt1", 4096).unwrap();
        let k2 = string_to_key(Etype::Aes256CtsHmacSha196, b"password", b"salt2", 4096).unwrap();
        assert_ne!(k1, k2);
    }

    // ==================== 跨协议验证: 密钥派生与加密兼容性 ====================

    /// 验证加密输出按正确格式: [密文][HMAC截断]
    /// 密文长度 = 16(confounder) + 明文长度(CTS 保留长度)
    /// 总长度 = 密文长度 + mac_len
    #[test]
    fn ciphertext_format_check() {
        let etype = Etype::Aes256CtsHmacSha196;
        let key = vec![0x42u8; 32];
        let plaintext = b"hello world";
        let pt_len = plaintext.len();
        let expected_ct_len = pt_len + 16; // confounder 16 bytes, CTS preserves length
        let expected_total = expected_ct_len + etype.mac_len(); // +12 for HMAC

        let ct = encrypt(etype, &key, 2, plaintext).unwrap();
        assert_eq!(
            ct.len(),
            expected_total,
            "ciphertext total length: expected {expected_total}, got {}",
            ct.len()
        );

        let dec = decrypt(etype, &key, 2, &ct).unwrap();
        assert_eq!(dec, plaintext);
    }

    // ==================== HMAC / PBKDF2 known-answer tests (RFC 2202 / RFC 6070) ====================

    /// HMAC-SHA1 known-answer test (RFC 2202, test case 1).
    /// Key: 20 bytes of 0x0b. Data: "Hi There". Expected HMAC-SHA1 = b6173186...
    #[test]
    fn hmac_sha1_rfc2202() {
        let mac = hmac_sha1(
            b"\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b",
            b"Hi There",
        );
        assert_eq!(
            mac,
            vec![
                0xb6, 0x17, 0x31, 0x86, 0x55, 0x05, 0x72, 0x64, 0xe2, 0x8b, 0xc0, 0xb6, 0xfb, 0x37,
                0x8c, 0x8e, 0xf1, 0x46, 0xbe, 0x00
            ]
        );
    }

    /// PBKDF2-HMAC-SHA1 known-answer test (RFC 6070, c=4096).
    #[test]
    fn pbkdf2_hmac_sha1_rfc6070() {
        let k = pbkdf2_hmac_sha1(b"password", b"salt", 4096, 20).unwrap();
        assert_eq!(
            k,
            vec![
                0x4b, 0x00, 0x79, 0x01, 0xb7, 0x65, 0x48, 0x9a, 0xbe, 0xad, 0x49, 0xd9, 0x26, 0xf7,
                0x21, 0xd0, 0x65, 0xa4, 0x29, 0xc1
            ]
        );
    }

    // ==================== Known-answer vectors: string_to_key (RFC 3962 Appendix A) ====================

    /// Known-answer test for `string_to_key` (etype 18) using the RFC 3962 Appendix B
    /// vector for c=1200. Verifies PBKDF2-HMAC-SHA1 + DK("kerberos").
    #[test]
    fn string_to_key_aes256_known_vector() {
        let k = string_to_key(
            Etype::Aes256CtsHmacSha196,
            b"password",
            b"ATHENA.MIT.EDUraeburn",
            1200,
        )
        .unwrap();
        assert_eq!(
            k,
            vec![
                0x55, 0xa6, 0xac, 0x74, 0x0a, 0xd1, 0x7b, 0x48, 0x46, 0x94, 0x10, 0x51, 0xe1, 0xe8,
                0xb0, 0xa7, 0x54, 0x8d, 0x93, 0xb0, 0xab, 0x30, 0xa8, 0xbc, 0x3f, 0xf1, 0x62, 0x80,
                0x38, 0x2b, 0x8c, 0x2a
            ]
        );
    }

    /// Known-answer test for `string_to_key` (etype 17) using the RFC 3962 Appendix B
    /// vector for c=1200. Verifies PBKDF2-HMAC-SHA1 + DK("kerberos").
    #[test]
    fn string_to_key_aes128_known_vector() {
        let k = string_to_key(
            Etype::Aes128CtsHmacSha196,
            b"password",
            b"ATHENA.MIT.EDUraeburn",
            1200,
        )
        .unwrap();
        assert_eq!(
            k,
            vec![
                0x4c, 0x01, 0xcd, 0x46, 0xd6, 0x32, 0xd0, 0x1e, 0x6d, 0xbe, 0x23, 0x0a, 0x01, 0xed,
                0x64, 0x2a
            ]
        );
    }

    // ==================== encrypt_rfc3962 / decrypt_rfc3962 roundtrip ====================

    /// RFC 3962 §5 profile (HMAC over ciphertext) roundtrip for RFC 3962 etypes.
    #[test]
    fn rfc3962_roundtrip_all_etypes() {
        for etype in [Etype::Aes128CtsHmacSha196, Etype::Aes256CtsHmacSha196] {
            let key = match etype {
                Etype::Aes128CtsHmacSha196 => vec![0x11u8; 16],
                Etype::Aes256CtsHmacSha196 => vec![0x42u8; 32],
                _ => unreachable!(),
            };
            for pt in [
                &b"short"[..],
                &b"exactly sixteen b"[..],
                &b"a longer plaintext payload for rfc3962 wrap"[..],
            ] {
                let ct = encrypt_rfc3962(etype, &key, 9, pt).unwrap();
                let dec = decrypt_rfc3962(etype, &key, 9, &ct).unwrap();
                assert_eq!(&dec, pt, "etype={:?} pt.len={}", etype, pt.len());
            }
        }
    }

    // ==================== RFC 8009 Appendix A: KAT Tests ====================

    /// Hex helper for test vectors.
    fn hx(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }
    fn hxe(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}").to_string()).collect()
    }

    /// KDF-HMAC-SHA2: etype 19, Kc derivation (usage=2, suffix=0x99, 128 bits).
    #[test]
    fn rfc8009_kdf_etype19_kc() {
        let key = hx("3705d96080c17728a0e800eab6e0d23c");
        let label = usage_constant(2, 0x99);
        let kc = kdf_hmac_sha2(Etype::Aes128CtsHmacSha256128, &key, &label, 128);
        assert_eq!(hxe(&kc), "b31a018a48f54776f403e9a396325dc3");
    }

    #[test]
    fn rfc8009_kdf_etype19_ke() {
        let key = hx("3705d96080c17728a0e800eab6e0d23c");
        let label = usage_constant(2, 0xAA);
        let ke = kdf_hmac_sha2(Etype::Aes128CtsHmacSha256128, &key, &label, 128);
        assert_eq!(hxe(&ke), "9b197dd1e8c5609d6e67c3e37c62c72e");
    }

    #[test]
    fn rfc8009_kdf_etype19_ki() {
        let key = hx("3705d96080c17728a0e800eab6e0d23c");
        let label = usage_constant(2, 0x55);
        let ki = kdf_hmac_sha2(Etype::Aes128CtsHmacSha256128, &key, &label, 128);
        assert_eq!(hxe(&ki), "9fda0e56ab2d85e1569a688696c26a6c");
    }

    #[test]
    fn rfc8009_kdf_etype20_kc() {
        let key = hx("6d404d37faf79f9df0d33568d320669800eb4836472ea8a026d16b7182460c52");
        let label = usage_constant(2, 0x99);
        let kc = kdf_hmac_sha2(Etype::Aes256CtsHmacSha384192, &key, &label, 192);
        assert_eq!(hxe(&kc), "ef5718be86cc84963d8bbb5031e9f5c4ba41f28faf69e73d");
    }

    #[test]
    fn rfc8009_kdf_etype20_ke() {
        let key = hx("6d404d37faf79f9df0d33568d320669800eb4836472ea8a026d16b7182460c52");
        let label = usage_constant(2, 0xAA);
        let ke = kdf_hmac_sha2(Etype::Aes256CtsHmacSha384192, &key, &label, 256);
        assert_eq!(
            hxe(&ke),
            "56ab22bee63d82d7bc5227f6773f8ea7a5eb1c825160c38312980c442e5c7e49"
        );
    }

    #[test]
    fn rfc8009_kdf_etype20_ki() {
        let key = hx("6d404d37faf79f9df0d33568d320669800eb4836472ea8a026d16b7182460c52");
        let label = usage_constant(2, 0x55);
        let ki = kdf_hmac_sha2(Etype::Aes256CtsHmacSha384192, &key, &label, 192);
        assert_eq!(hxe(&ki), "69b16514e3cd8e56b82010d5c73012b622c4d00ffc23ed1f");
    }

    /// RFC 8009 encrypt/decrypt roundtrip for etypes 19/20.
    #[test]
    fn rfc8009_roundtrip() {
        for etype in [Etype::Aes128CtsHmacSha256128, Etype::Aes256CtsHmacSha384192] {
            let key = match etype {
                Etype::Aes128CtsHmacSha256128 => vec![0x11u8; 16],
                Etype::Aes256CtsHmacSha384192 => vec![0x42u8; 32],
                _ => unreachable!(),
            };
            for pt in [
                &b"short"[..],
                &b"exactly sixteen b"[..],
                &b"a longer plaintext payload"[..],
            ] {
                let ct = encrypt(etype, &key, 3, pt).unwrap();
                let dec = decrypt(etype, &key, 3, &ct).unwrap();
                assert_eq!(
                    &dec,
                    pt,
                    "RFC 8009 roundtrip etype={etype:?} pt.len={}",
                    pt.len()
                );
            }
        }
    }

    /// RFC 8009 tampered ciphertext must fail.
    #[test]
    fn rfc8009_tamper_detected() {
        for etype in [Etype::Aes128CtsHmacSha256128, Etype::Aes256CtsHmacSha384192] {
            let key = match etype {
                Etype::Aes128CtsHmacSha256128 => vec![0x11u8; 16],
                Etype::Aes256CtsHmacSha384192 => vec![0x42u8; 32],
                _ => unreachable!(),
            };
            let mut ct = encrypt(etype, &key, 3, b"secret payload").unwrap();
            ct[0] ^= 0xff;
            assert!(
                decrypt(etype, &key, 3, &ct).is_err(),
                "RFC 8009 tamper should fail {etype:?}"
            );
        }
    }

    /// A tampered ciphertext must fail verification under the RFC 3962 profile.
    #[test]
    fn rfc3962_tamper_detected() {
        let etype = Etype::Aes256CtsHmacSha196;
        let key = vec![0x42u8; 32];
        let mut ct = encrypt_rfc3962(etype, &key, 9, b"secret payload").unwrap();
        ct[0] ^= 0xff;
        assert!(decrypt_rfc3962(etype, &key, 9, &ct).is_err());
    }

    // ==================== RFC 3962 Appendix B: AES-CTS Test Vectors ====================

    /// RFC 3962 Appendix B — AES-128-CTS, key="chicken teriyaki", IV=0
    /// Input: "I would like the " (17 bytes)
    /// Expected output matches RFC 3962 §B test vector 1
    #[test]
    fn rfc3962_aes128_cts_17_bytes() {
        let key = b"chicken teriyaki"; // 16 bytes, AES-128 key
        let iv = vec![0u8; 16];
        // "I would like the " = 17 bytes
        let input = b"I would like the ";
        let output = cts_encrypt::<Aes128>(key, &iv, input).unwrap();
        let expected: Vec<u8> = vec![
            0xc6, 0x35, 0x35, 0x68, 0xf2, 0xbf, 0x8c, 0xb4, 0xd8, 0xa5, 0x80, 0x36, 0x2d, 0xa7,
            0xff, 0x7f, 0x97,
        ];
        assert_eq!(output, expected, "AES-128-CTS 17-byte mismatch");
        // Roundtrip: decrypt should recover input
        let dec = cts_decrypt::<Aes128>(key, &iv, &output).unwrap();
        assert_eq!(dec, input, "AES-128-CTS 17-byte decrypt roundtrip");
    }

    /// RFC 3962 Appendix B — AES-128-CTS, key="chicken teriyaki", IV=0
    /// Input: "I would like the General Gau's " (31 bytes)
    #[test]
    fn rfc3962_aes128_cts_31_bytes() {
        let key = b"chicken teriyaki";
        let iv = vec![0u8; 16];
        // "I would like the General Gau's " = 31 bytes
        let input = b"I would like the General Gau's ";
        let output = cts_encrypt::<Aes128>(key, &iv, input).unwrap();
        let expected: Vec<u8> = vec![
            0xfc, 0x00, 0x78, 0x3e, 0x0e, 0xfd, 0xb2, 0xc1, 0xd4, 0x45, 0xd4, 0xc8, 0xef, 0xf7,
            0xed, 0x22, 0x97, 0x68, 0x72, 0x68, 0xd6, 0xec, 0xcc, 0xc0, 0xc0, 0x7b, 0x25, 0xe2,
            0x5e, 0xcf, 0xe5,
        ];
        assert_eq!(output, expected, "AES-128-CTS 31-byte mismatch");
        let dec = cts_decrypt::<Aes128>(key, &iv, &output).unwrap();
        assert_eq!(dec, input, "AES-128-CTS 31-byte decrypt roundtrip");
    }

    /// RFC 3962 Appendix B — AES-128-CTS, key="chicken teriyaki", IV=0
    /// Input: "I would like the General Gau's C" (32 bytes, exact 2 blocks)
    #[test]
    fn rfc3962_aes128_cts_32_bytes() {
        let key = b"chicken teriyaki";
        let iv = vec![0u8; 16];
        let input = b"I would like the General Gau's C";
        let output = cts_encrypt::<Aes128>(key, &iv, input).unwrap();
        let expected: Vec<u8> = vec![
            0x39, 0x31, 0x25, 0x23, 0xa7, 0x86, 0x62, 0xd5, 0xbe, 0x7f, 0xcb, 0xcc, 0x98, 0xeb,
            0xf5, 0xa8, 0x97, 0x68, 0x72, 0x68, 0xd6, 0xec, 0xcc, 0xc0, 0xc0, 0x7b, 0x25, 0xe2,
            0x5e, 0xcf, 0xe5, 0x84,
        ];
        assert_eq!(
            output, expected,
            "AES-128-CTS 32-byte (exact 2 blocks) mismatch"
        );
        let dec = cts_decrypt::<Aes128>(key, &iv, &output).unwrap();
        assert_eq!(dec, input, "AES-128-CTS 32-byte decrypt roundtrip");
    }

    /// RFC 3962 Appendix B — AES-128-CTS, key="chicken teriyaki", IV=0
    /// 64 bytes input (exact 4 blocks)
    #[test]
    fn rfc3962_aes128_cts_64_bytes() {
        let key = b"chicken teriyaki";
        let iv = vec![0u8; 16];
        let input = b"I would like the General Gau's Chicken, please, and wonton soup.";
        let output = cts_encrypt::<Aes128>(key, &iv, input).unwrap();
        let expected: Vec<u8> = vec![
            0x97, 0x68, 0x72, 0x68, 0xd6, 0xec, 0xcc, 0xc0, 0xc0, 0x7b, 0x25, 0xe2, 0x5e, 0xcf,
            0xe5, 0x84, 0x39, 0x31, 0x25, 0x23, 0xa7, 0x86, 0x62, 0xd5, 0xbe, 0x7f, 0xcb, 0xcc,
            0x98, 0xeb, 0xf5, 0xa8, 0x48, 0x07, 0xef, 0xe8, 0x36, 0xee, 0x89, 0xa5, 0x26, 0x73,
            0x0d, 0xbc, 0x2f, 0x7b, 0xc8, 0x40, 0x9d, 0xad, 0x8b, 0xbb, 0x96, 0xc4, 0xcd, 0xc0,
            0x3b, 0xc1, 0x03, 0xe1, 0xa1, 0x94, 0xbb, 0xd8,
        ];
        assert_eq!(
            output, expected,
            "AES-128-CTS 64-byte (exact 4 blocks) mismatch"
        );
        let dec = cts_decrypt::<Aes128>(key, &iv, &output).unwrap();
        assert_eq!(dec, input, "AES-128-CTS 64-byte decrypt roundtrip");
    }
}
