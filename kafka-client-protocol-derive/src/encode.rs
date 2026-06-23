// src/encode.rs
use crate::field::FieldInfo;
use crate::version_range::VersionRange;
use proc_macro2::TokenStream;
use quote::quote;

/// 生成 encode 方法
pub fn generate_encode(fields: &[FieldInfo], flexible_version: Option<i16>) -> TokenStream {
    let encode_fields: Vec<TokenStream> = fields
        .iter()
        .map(|field| generate_encode_field(field, flexible_version))
        .collect();

    let tag_buffer = match flexible_version {
        Some(v) => quote! {
            if version >= #v {
                encode_unsigned_varint(buf, 0);
            }
        },
        None => quote! {},
    };

    quote! {
        fn encode(&self, buf: &mut ::bytes::BytesMut, version: i16) -> kafka_client_protocol_core::ProtocolResult<()> {
            use kafka_client_protocol_core::codec::*;
            use ::bytes::BufMut;

            #(#encode_fields)*

            #tag_buffer

            Ok(())
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

/// 生成单个字段的编码代码
fn generate_encode_field(field: &FieldInfo, flexible_version: Option<i16>) -> TokenStream {
    let field_name = &field.name;
    let condition = field.versions.as_check_expr();
    #[allow(unused_variables)]
    let is_flex = flexible_check(flexible_version);

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
    let encode_body = generate_encode_body(field, flexible_version);

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
fn generate_encode_body(field: &FieldInfo, flexible_version: Option<i16>) -> TokenStream {
    let field_name = &field.name;
    let is_flex = flexible_check(flexible_version);

    // Option 类型特殊处理
    if field.is_option() {
        if let Some(nullable_versions) = &field.nullable_versions {
            let null_condition = nullable_versions.as_check_expr();

            // 提取 Option 内部类型，检测是否是 Vec 或 String
            let inner_ty = FieldInfo::extract_option_inner(&field.ty);
            let default_encode = inner_ty.as_ref().map(|inner| {
                if FieldInfo::is_vec_type(inner) {
                    // Option<Vec<T>>: None at non-nullable version → encode as empty array
                    quote! {
                        if #is_flex {
                            encode_unsigned_varint(buf, 1);
                        } else {
                            buf.put_i32(0);
                        }
                    }
                } else if is_string_type(inner) {
                    // Option<String>: None at non-nullable version → encode as empty string
                    quote! {
                        if #is_flex {
                            encode_unsigned_varint(buf, 1);
                        } else {
                            buf.put_i16(0);
                        }
                    }
                } else if is_bytes_type(inner)
                    || FieldInfo::is_vec_type(inner)
                    || is_record_batch_type(inner)
                {
                    // Option<Bytes/RecordBatch>: None at non-nullable version → encode as empty bytes
                    quote! {
                        if #is_flex {
                            encode_unsigned_varint(buf, 1);
                        } else {
                            buf.put_i32(0);
                        }
                    }
                } else {
                    // Option<OtherStruct>: use default value
                    quote! {
                        let empty: #inner = ::core::default::Default::default();
                        ::kafka_client_protocol_core::Message::encode(&empty, buf, version)?;
                    }
                }
            });

            let is_inner_string = inner_ty.as_ref().map(is_string_type).unwrap_or(false);
            #[allow(unused_variables)]
            let is_inner_bytes = inner_ty.as_ref().map(is_bytes_type).unwrap_or(false);
            #[allow(unused_variables)]
            let is_inner_record_batch =
                inner_ty.as_ref().map(is_record_batch_type).unwrap_or(false);

            let some_encode = inner_ty.as_ref().map(|inner| {
                if FieldInfo::is_vec_type(inner) {
                    quote! {
                        if #is_flex {
                            encode_compact_array(buf, self.#field_name.as_ref().unwrap(), |b, item| -> kafka_client_protocol_core::ProtocolResult<()> {
                                item.encode(b, version)?;
                                Ok(())
                            }).ok();
                        } else {
                            encode_array(buf, self.#field_name.as_ref().unwrap(), |b, item| -> kafka_client_protocol_core::ProtocolResult<()> {
                                item.encode(b, version)?;
                                Ok(())
                            }).ok();
                        }
                    }
                } else if is_string_type(inner) {
                    quote! {
                        if #is_flex {
                            encode_compact_nullable_string(buf, &self.#field_name);
                        } else {
                            encode_nullable_string(buf, &self.#field_name);
                        }
                    }
                } else if is_bytes_type(inner) {
                    quote! {
                        if #is_flex {
                            encode_compact_bytes(buf, self.#field_name.as_ref().unwrap());
                        } else {
                            encode_bytes(buf, self.#field_name.as_ref().unwrap());
                        }
                    }
                } else if is_record_batch_type(inner) {
                    // Option<RecordBatch>: encode as nullable bytes (records field)
                    quote! {
                        let batch = self.#field_name.as_ref().unwrap();
                        let mut batch_buf = ::bytes::BytesMut::new();
                        ::kafka_client_protocol_core::Message::encode(batch, &mut batch_buf, version)?;
                        if #is_flex {
                            encode_unsigned_varint(buf, batch_buf.len() as u32 + 1);
                            buf.extend_from_slice(&batch_buf);
                        } else {
                            buf.put_i32(batch_buf.len() as i32);
                            buf.extend_from_slice(&batch_buf);
                        }
                    }
                } else {
                    quote! {
                        self.#field_name.encode(buf, version)?;
                    }
                }
            });

            return quote! {
                if #null_condition && self.#field_name.is_none() {
                    if #is_flex {
                        encode_unsigned_varint(buf, 0);
                    } else if #is_inner_string {
                        buf.put_i16(-1);
                    } else {
                        buf.put_i32(-1);
                    }
                } else if self.#field_name.is_none() {
                    // Not nullable at this version: encode as default/empty
                    #default_encode
                } else {
                    #some_encode
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
        quote! {
            if #is_flex {
                encode_compact_string(buf, &self.#field_name);
            } else {
                encode_string(buf, &self.#field_name);
            }
        }
    } else if field.is_bytes() {
        quote! {
            if #is_flex {
                encode_compact_bytes(buf, &self.#field_name);
            } else {
                encode_bytes(buf, &self.#field_name);
            }
        }
    } else if field.is_vec() {
        quote! {
            if #is_flex {
                encode_compact_array(buf, &self.#field_name, |b, item| -> kafka_client_protocol_core::ProtocolResult<()> {
                    item.encode(b, version)?;
                    Ok(())
                }).ok();
            } else {
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

/// 检查类型是否是 String
fn is_string_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.first() {
            return segment.ident == "String";
        }
    }
    false
}

/// 检查类型是否是 Bytes
fn is_bytes_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.first() {
            return segment.ident == "Bytes";
        }
    }
    false
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
