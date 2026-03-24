// src/encode.rs
use quote::quote;
use proc_macro2::TokenStream;
use crate::field::FieldInfo;
use crate::version_range::VersionRange;

/// 生成 encode 方法
pub fn generate_encode(fields: &[FieldInfo], flexible_version: Option<i16>) -> TokenStream {
    let encode_fields: Vec<TokenStream> = fields
        .iter()
        .map(|field| generate_encode_field(field, flexible_version))
        .collect();
    
    quote! {
        fn encode(&self, buf: &mut ::bytes::BytesMut, version: i16) -> kafka_client_protocol_core::ProtocolResult<()> {
            use kafka_client_protocol_core::codec::*;
            use ::bytes::BufMut;
            
            #(#encode_fields)*
            
            Ok(())
        }
    }
}

/// 判断字段是否应该使用 flexible format
fn should_use_flexible(field: &FieldInfo, flexible_version: Option<i16>) -> bool {
    if field.tagged_versions.is_some() {
        return true;
    }
    match flexible_version {
        Some(v) => field.versions.min_version() >= v,
        None => false,
    }
}

/// 生成单个字段的编码代码
fn generate_encode_field(field: &FieldInfo, flexible_version: Option<i16>) -> TokenStream {
    let field_name = &field.name;
    let condition = field.versions.as_check_expr();
    let use_flexible = should_use_flexible(field, flexible_version);
    
    // 标签字段特殊处理
    if field.tagged_versions.is_some() {
        if let Some(tag) = field.tag {
            return quote! {
                if #condition {
                    // 检查是否需要跳过（所有字段都是默认值）
                    if !self.#field_name.is_default() {
                        encode_unsigned_varint(buf, #tag);
                        let len_pos = buf.len();
                        // 预留最大 varint 长度（5 bytes for u32）
                        buf.extend_from_slice(&[0u8; 5]);
                        let start_len = buf.len();
                        self.#field_name.encode(buf, version)?;
                        let data_len = (buf.len() - start_len) as u32;
                        // 计算实际 varint 长度
                        let len_bytes = varint_len(data_len);
                        // 创建临时 buffer 编码长度
                        let mut len_buf = ::bytes::BytesMut::with_capacity(5);
                        encode_unsigned_varint(&mut len_buf, data_len + 1);
                        // 回填长度
                        buf[len_pos..len_pos + len_bytes].copy_from_slice(&len_buf[..len_bytes]);
                    }
                }
            };
        }
    }
    
    // 普通字段
    let encode_body = generate_encode_body(field, use_flexible);
    
    if field.versions == VersionRange::All {
        encode_body
    } else {
        quote! {
            if #condition {
                #encode_body
            }
        }
    }
}

/// 生成字段编码体
fn generate_encode_body(field: &FieldInfo, use_flexible: bool) -> TokenStream {
    let field_name = &field.name;
    
    // Option 类型特殊处理
    if field.is_option() {
        if let Some(nullable_versions) = &field.nullable_versions {
            let null_condition = nullable_versions.as_check_expr();
            return quote! {
                if #null_condition && self.#field_name.is_none() {
                    if #use_flexible {
                        encode_unsigned_varint(buf, 0);
                    } else {
                        buf.put_i32(-1);
                    }
                } else {
                    self.#field_name.encode(buf, version)?;
                }
            };
        } else {
            return quote! {
                self.#field_name.encode(buf, version)?;
            };
        }
    }
    
    // 根据类型生成编码代码
    if field.is_string() {
        if use_flexible {
            quote! { encode_compact_string(buf, &self.#field_name); }
        } else {
            quote! { encode_string(buf, &self.#field_name); }
        }
    } else if field.is_bytes() {
        if use_flexible {
            quote! { encode_compact_bytes(buf, &self.#field_name); }
        } else {
            quote! { encode_bytes(buf, &self.#field_name); }
        }
    } else if field.is_vec() {
        if use_flexible {
            quote! {
                encode_compact_array(buf, &self.#field_name, |b, item| -> kafka_client_protocol_core::ProtocolResult<()> {
                    item.encode(b, version)?;
                    Ok(())
                }).ok();
            }
        } else {
            quote! {
                encode_array(buf, &self.#field_name, |b, item| -> kafka_client_protocol_core::ProtocolResult<()> {
                    item.encode(b, version)?;
                    Ok(())
                }).ok();
            }
        }
    } else if field.is_bool() {
        quote! { buf.put_i8(self.#field_name as i8); }
    } else if field.is_integer() {
        let ty_str = field.type_str();
        let put_method = format!("put_{}", ty_str);
        let put_ident = syn::Ident::new(&put_method, proc_macro2::Span::call_site());
        quote! { buf.#put_ident(self.#field_name); }
    } else if field.is_float() {
        quote! { buf.put_f64(self.#field_name); }
    } else if field.is_uuid() {
        quote! { self.#field_name.encode(buf, version)?; }
    } else {
        // 默认：使用 Message trait 的 encode
        quote! { self.#field_name.encode(buf, version)?; }
    }
}