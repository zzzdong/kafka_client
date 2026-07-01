// src/decode.rs
use crate::field::FieldInfo;
use crate::version_range::VersionRange;
use proc_macro2::TokenStream;
use quote::quote;

/// 生成 decode 和 flexible_decode 方法
pub fn generate_decode(fields: &[FieldInfo], flexible_version: Option<i16>) -> TokenStream {
    let decode_method = generate_decode_single(fields, flexible_version, false);
    let flexible_decode_method = generate_decode_single(fields, flexible_version, true);

    quote! {
        #decode_method
        #flexible_decode_method
    }
}

/// 生成单个解码方法（decode 或 flexible_decode）
fn generate_decode_single(
    fields: &[FieldInfo],
    _flexible_version: Option<i16>,
    flexible: bool,
) -> TokenStream {
    let method_name = if flexible {
        syn::Ident::new("flexible_decode", proc_macro2::Span::call_site())
    } else {
        syn::Ident::new("decode", proc_macro2::Span::call_site())
    };

    let decode_fields: Vec<TokenStream> = fields
        .iter()
        .map(|field| generate_decode_field(field, flexible))
        .collect();

    let field_names: Vec<_> = fields.iter().map(|f| &f.name).collect();

    let tagged_decode = if flexible {
        quote! {
            let _ = ::kafka_client_protocol_core::header::decode_tagged_fields(buf)?;
        }
    } else {
        TokenStream::new()
    };

    quote! {
        fn #method_name(buf: &mut ::bytes::Bytes, version: i16) -> kafka_client_protocol_core::ProtocolResult<Self> {
            use kafka_client_protocol_core::codec::*;
            use ::bytes::Buf;

            #(#decode_fields)*

            // 灵活版本：跳过 tagged fields
            #tagged_decode

            Ok(Self {
                #(#field_names,)*
            })
        }
    }
}

/// 生成默认值表达式
fn generate_default_value(field: &FieldInfo) -> TokenStream {
    if let Some(default) = &field.default {
        if default == "null" || default == "None" {
            return quote! { None };
        }
        if let Ok(v) = default.parse::<i64>() {
            return quote! { #v };
        }
        if default == "true" || default == "false" {
            let b = default == "true";
            return quote! { #b };
        }
        return quote! { #default.to_string() };
    }

    // 根据类型生成默认值
    if field.is_option() {
        quote! { None }
    } else if field.is_string() {
        quote! { String::new() }
    } else if field.is_bytes() {
        quote! { ::bytes::Bytes::new() }
    } else if field.is_vec() {
        quote! { Vec::new() }
    } else if field.is_bool() {
        quote! { false }
    } else if field.is_integer() {
        quote! { 0 }
    } else if field.is_float() {
        quote! { 0.0 }
    } else {
        quote! { Default::default() }
    }
}

/// 生成字段解码体
fn generate_decode_field(field: &FieldInfo, flexible: bool) -> TokenStream {
    let field_name = &field.name;
    let condition = field.versions.as_check_expr();
    let default_expr = generate_default_value(field);

    // Tagged fields are NOT decoded inline — they appear in the tagged section
    // at the end of the struct in compact format. Use default value for inline decode.
    // In non-flexible mode, tagged fields ARE inlined with their default value.
    if field.tagged_versions.is_some() {
        if flexible {
            return quote! {
                let #field_name = #default_expr;
            };
        } else {
            return quote! {
                let #field_name = if #condition {
                    #default_expr
                } else {
                    #default_expr
                };
            };
        }
    }

    let decode_body = generate_decode_body(field, flexible);

    if field.versions == VersionRange::All {
        quote! {
            let #field_name = #decode_body;
        }
    } else {
        quote! {
            let #field_name = if #condition {
                #decode_body
            } else {
                #default_expr
            };
        }
    }
}

/// 生成字段解码体
fn generate_decode_body(field: &FieldInfo, flexible: bool) -> TokenStream {
    // encoded_as_bytes 字段：线上是 bytes（需读取长度前缀），但映射为结构化类型
    if field.encoded_as_bytes
        && let Some(nullable_versions) = &field.nullable_versions
    {
        let null_condition = nullable_versions.as_check_expr();
        let inner_ty = FieldInfo::extract_option_inner(&field.ty)
            .expect("encoded_as_bytes field must be Option<T>");
        let decode_method = if flexible {
            "flexible_decode"
        } else {
            "decode"
        };
        let decode_ident = syn::Ident::new(decode_method, proc_macro2::Span::call_site());
        let batch_decode = quote! { <#inner_ty as ::kafka_client_protocol_core::Message>::#decode_ident(&mut batch_buf, version)? };
        if flexible {
            // Fetch v12+: COMPACT_NULLABLE_RECORDS → uvint N+1
            return quote! {
                if #null_condition {
                    let len = decode_unsigned_varint(buf);
                    if len <= 1 {
                        None
                    } else {
                        let data_len = (len - 1) as usize;
                        if buf.remaining() < data_len {
                            return Err(::kafka_client_protocol_core::ProtocolError::invalid_data("encoded_as_bytes data exceeds remaining buffer"));
                        }
                        let mut batch_buf = buf.split_to(data_len);
                        Some(#batch_decode)
                    }
                } else {
                    let len = decode_unsigned_varint(buf);
                    if len <= 1 {
                        None
                    } else {
                        let data_len = (len - 1) as usize;
                        if buf.remaining() < data_len {
                            return Err(::kafka_client_protocol_core::ProtocolError::invalid_data("encoded_as_bytes data exceeds remaining buffer"));
                        }
                        let mut batch_buf = buf.split_to(data_len);
                        Some(#batch_decode)
                    }
                }
            };
        } else {
            // Fetch v0-11: NULLABLE_RECORDS → INT32 len
            return quote! {
                if #null_condition {
                    if buf.remaining() < 4 {
                        return Err(::kafka_client_protocol_core::ProtocolError::insufficient_data(4, buf.remaining()));
                    }
                    let len = buf.get_i32();
                    if len <= 0 {
                        None
                    } else {
                        let data_len = len as usize;
                        if buf.remaining() < data_len {
                            return Err(::kafka_client_protocol_core::ProtocolError::invalid_data("encoded_as_bytes data exceeds remaining buffer"));
                        }
                        let mut batch_buf = buf.split_to(data_len);
                        Some(#batch_decode)
                    }
                } else {
                    let len = buf.get_i32();
                    if len < 0 {
                        return Err(::kafka_client_protocol_core::ProtocolError::invalid_data("unexpected negative length for non-nullable records"));
                    }
                    let data_len = len as usize;
                    if buf.remaining() < data_len {
                        return Err(::kafka_client_protocol_core::ProtocolError::invalid_data("encoded_as_bytes data exceeds remaining buffer"));
                    }
                    let mut batch_buf = buf.split_to(data_len);
                    Some(#batch_decode)
                }
            };
        }
    }

    // Option 类型特殊处理
    if field.is_option() {
        let inner_decode = generate_decode_body_inner(field, flexible);

        if let Some(nullable_versions) = &field.nullable_versions {
            let null_condition = nullable_versions.as_check_expr();
            if flexible {
                return quote! {
                    if #null_condition {
                        // Compact nullable: varint(0) = null (0x00 byte).
                        // Peek first byte: 0x00 → null; otherwise pass through.
                        if buf.is_empty() {
                            return Err(
                                ::kafka_client_protocol_core::ProtocolError::insufficient_data(1, 0)
                            );
                        }
                        if buf[0] == 0 {
                            buf.advance(1);
                            None
                        } else {
                            Some(#inner_decode)
                        }
                    } else {
                        Some(#inner_decode)
                    }
                };
            } else {
                return quote! {
                    if #null_condition {
                        let peek = if buf.remaining() >= 4 {
                            i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]])
                        } else {
                            0
                        };
                        if peek == -1 {
                            buf.advance(4);
                            None
                        } else {
                            Some(#inner_decode)
                        }
                    } else {
                        Some(#inner_decode)
                    }
                };
            }
        } else {
            return quote! { Some(#inner_decode) };
        }
    }

    generate_decode_body_inner(field, flexible)
}

/// 生成内部类型的解码体
fn generate_decode_body_inner(field: &FieldInfo, flexible: bool) -> TokenStream {
    let ty = &field.ty;

    // 移除可能的 Option 包装
    let inner_ty = if let Some(inner) = FieldInfo::extract_option_inner(ty) {
        inner
    } else {
        ty.clone()
    };

    // 检查是否是 Vec 类型
    if let Some(vec_inner_ty) = FieldInfo::extract_vec_inner(&inner_ty) {
        let decode_fn = if flexible {
            "decode_compact_array"
        } else {
            "decode_array"
        };
        let decode_ident = syn::Ident::new(decode_fn, proc_macro2::Span::call_site());
        let item_decode_method = if flexible {
            syn::Ident::new("flexible_decode", proc_macro2::Span::call_site())
        } else {
            syn::Ident::new("decode", proc_macro2::Span::call_site())
        };
        quote! {
            #decode_ident(buf, |b| <#vec_inner_ty as ::kafka_client_protocol_core::Message>::#item_decode_method(b, version))?
        }
    } else {
        // 对于非 Vec 类型，使用字符串比较
        let inner_ty_str = quote! { #inner_ty }.to_string();

        // 根据类型生成解码代码
        if inner_ty_str == "String" {
            let decode_fn = if flexible {
                "decode_compact_string"
            } else {
                "decode_string"
            };
            let fn_ident = syn::Ident::new(decode_fn, proc_macro2::Span::call_site());
            quote! { #fn_ident(buf)? }
        } else if inner_ty_str == "Bytes" {
            let decode_fn = if flexible {
                "decode_compact_bytes"
            } else {
                "decode_bytes"
            };
            let fn_ident = syn::Ident::new(decode_fn, proc_macro2::Span::call_site());
            quote! { #fn_ident(buf)? }
        } else if inner_ty_str == "bool" {
            quote! { buf.get_i8() != 0 }
        } else if inner_ty_str == "i8" {
            quote! { buf.get_i8() }
        } else if inner_ty_str == "i16" {
            quote! { buf.get_i16() }
        } else if inner_ty_str == "i32" {
            quote! { buf.get_i32() }
        } else if inner_ty_str == "i64" {
            quote! { buf.get_i64() }
        } else if inner_ty_str == "u8" {
            quote! { buf.get_u8() }
        } else if inner_ty_str == "u16" {
            quote! { buf.get_u16() }
        } else if inner_ty_str == "u32" {
            quote! { buf.get_u32() }
        } else if inner_ty_str == "u64" {
            quote! { buf.get_u64() }
        } else if inner_ty_str == "f64" {
            quote! { buf.get_f64() }
        } else if inner_ty_str == "Uuid" {
            let decode_method = if flexible {
                syn::Ident::new("flexible_decode", proc_macro2::Span::call_site())
            } else {
                syn::Ident::new("decode", proc_macro2::Span::call_site())
            };
            quote! { <uuid::Uuid as ::kafka_client_protocol_core::Message>::#decode_method(buf, version)? }
        } else {
            // 将字符串转换为 Ident
            let type_ident = syn::Ident::new(&inner_ty_str, proc_macro2::Span::call_site());
            let decode_method = if flexible {
                syn::Ident::new("flexible_decode", proc_macro2::Span::call_site())
            } else {
                syn::Ident::new("decode", proc_macro2::Span::call_site())
            };
            quote! { <#type_ident as ::kafka_client_protocol_core::Message>::#decode_method(buf, version)? }
        }
    }
}
