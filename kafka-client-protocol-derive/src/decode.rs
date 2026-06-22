// src/decode.rs
use crate::field::FieldInfo;
use crate::version_range::VersionRange;
use proc_macro2::TokenStream;
use quote::quote;

/// 生成 decode 方法
pub fn generate_decode(fields: &[FieldInfo], flexible_version: Option<i16>) -> TokenStream {
    let decode_fields: Vec<TokenStream> = fields
        .iter()
        .map(|field| generate_decode_field(field, flexible_version))
        .collect();

    let field_names: Vec<_> = fields.iter().map(|f| &f.name).collect();

    let tag_buffer = match flexible_version {
        Some(v) => quote! {
            if version >= #v {
                let _ = ::kafka_client_protocol_core::header::decode_tagged_fields(buf)?;
            }
        },
        None => quote! {},
    };

    quote! {
        fn decode(buf: &mut ::bytes::Bytes, version: i16) -> kafka_client_protocol_core::ProtocolResult<Self> {
            use kafka_client_protocol_core::codec::*;
            use ::bytes::Buf;

            #(#decode_fields)*

            #tag_buffer

            Ok(Self {
                #(#field_names,)*
            })
        }
    }
}

/// 生成运行时版本是否使用 flexible 格式的检查表达式
fn flexible_check(flexible_version: Option<i16>) -> TokenStream {
    match flexible_version {
        Some(v) => quote! { version >= #v },
        None => quote! { false },
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
fn generate_decode_field(field: &FieldInfo, flexible_version: Option<i16>) -> TokenStream {
    let field_name = &field.name;
    let condition = field.versions.as_check_expr();
    let default_expr = generate_default_value(field);
    let is_flex = flexible_check(flexible_version);

    let decode_body = generate_decode_body(field, flexible_version);

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

/// 检查类型是否是 RecordBatch
fn is_record_batch_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.first() {
            return segment.ident == "RecordBatch";
        }
    }
    false
}

/// 生成字段解码体
fn generate_decode_body(field: &FieldInfo, flexible_version: Option<i16>) -> TokenStream {
    let is_flex = flexible_check(flexible_version);

    // Option<RecordBatch> 特殊处理：按 nullable bytes 读取长度后再解码
    if field.is_option() {
        if let Some(inner) = FieldInfo::extract_option_inner(&field.ty) {
            if is_record_batch_type(&inner) {
                if let Some(nullable_versions) = &field.nullable_versions {
                    let null_condition = nullable_versions.as_check_expr();
                    return quote! {
                        if #null_condition {
                            if #is_flex {
                                let len = decode_unsigned_varint(buf);
                                if len == 0 {
                                    None
                                } else {
                                    let mut batch_buf = buf.split_to((len - 1) as usize);
                                    Some(<::kafka_client_protocol_core::RecordBatch as ::kafka_client_protocol_core::Message>::decode(&mut batch_buf, version)?)
                                }
                            } else {
                                if buf.remaining() < 4 {
                                    return Err(::kafka_client_protocol_core::ProtocolError::insufficient_data(4, buf.remaining()));
                                }
                                let len = buf.get_i32();
                                if len <= 0 {
                                    None
                                } else {
                                    let mut batch_buf = buf.split_to(len as usize);
                                    Some(<::kafka_client_protocol_core::RecordBatch as ::kafka_client_protocol_core::Message>::decode(&mut batch_buf, version)?)
                                }
                            }
                        } else {
                            let mut batch_buf = if #is_flex {
                                let len = decode_unsigned_varint(buf);
                                buf.split_to((len - 1) as usize)
                            } else {
                                let len = buf.get_i32();
                                if len < 0 {
                                    return Err(::kafka_client_protocol_core::ProtocolError::invalid_data("unexpected negative length for non-nullable records"));
                                }
                                buf.split_to(len as usize)
                            };
                            Some(<::kafka_client_protocol_core::RecordBatch as ::kafka_client_protocol_core::Message>::decode(&mut batch_buf, version)?)
                        }
                    };
                }
            }
        }
    }

    // Option 类型特殊处理
    if field.is_option() {
        let inner_decode = generate_decode_body_inner(field, flexible_version);

        if let Some(nullable_versions) = &field.nullable_versions {
            let null_condition = nullable_versions.as_check_expr();
            return quote! {
                if #null_condition {
                    if #is_flex {
                        let len = decode_unsigned_varint(buf);
                        if len == 0 {
                            None
                        } else {
                            Some(#inner_decode)
                        }
                    } else {
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
                    }
                } else {
                    Some(#inner_decode)
                }
            };
        } else {
            return quote! { Some(#inner_decode) };
        }
    }

    generate_decode_body_inner(field, flexible_version)
}

/// 生成内部类型的解码体
fn generate_decode_body_inner(field: &FieldInfo, flexible_version: Option<i16>) -> TokenStream {
    let ty = &field.ty;
    let is_flex = flexible_check(flexible_version);

    // 移除可能的 Option 包装
    let inner_ty = if let Some(inner) = FieldInfo::extract_option_inner(ty) {
        inner
    } else {
        ty.clone()
    };

    // 检查是否是 Vec 类型
    if let Some(vec_inner_ty) = FieldInfo::extract_vec_inner(&inner_ty) {
        quote! {
            if #is_flex {
                decode_compact_array(buf, |b| <#vec_inner_ty as ::kafka_client_protocol_core::Message>::decode(b, version))?
            } else {
                decode_array(buf, |b| <#vec_inner_ty as ::kafka_client_protocol_core::Message>::decode(b, version))?
            }
        }
    } else {
        // 对于非 Vec 类型，使用字符串比较
        let inner_ty_str = quote! { #inner_ty }.to_string();

        // 根据类型生成解码代码
        if inner_ty_str == "String" {
            quote! {
                if #is_flex {
                    decode_compact_string(buf)?
                } else {
                    decode_string(buf)?
                }
            }
        } else if inner_ty_str == "Bytes" {
            quote! {
                if #is_flex {
                    decode_compact_bytes(buf)?
                } else {
                    decode_bytes(buf)?
                }
            }
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
            quote! { <uuid::Uuid as ::kafka_client_protocol_core::Message>::decode(buf, version)? }
        } else {
            // 将字符串转换为 Ident
            let type_ident = syn::Ident::new(&inner_ty_str, proc_macro2::Span::call_site());
            quote! { <#type_ident as ::kafka_client_protocol_core::Message>::decode(buf, version)? }
        }
    }
}
