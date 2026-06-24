// src/encode.rs
use crate::field::FieldInfo;
use crate::version_range::VersionRange;
use proc_macro2::TokenStream;
use quote::quote;

/// 生成 encode 方法
pub fn generate_encode(fields: &[FieldInfo], flexible_version: Option<i16>) -> TokenStream {
    let inline_fields: Vec<TokenStream> = fields
        .iter()
        .filter(|f| f.tagged_versions.is_none())
        .map(|field| generate_encode_field_inline(field, flexible_version))
        .collect();

    let tagged_check: Vec<TokenStream> = fields
        .iter()
        .filter(|f| f.tagged_versions.is_some())
        .map(|field| {
            let field_name = &field.name;
            let condition = field.versions.as_check_expr();
            quote! {
                if #condition && !self.#field_name.is_default() {
                    tagged_count += 1;
                }
            }
        })
        .collect();

    let tagged_encode: Vec<TokenStream> = fields
        .iter()
        .filter(|f| f.tagged_versions.is_some())
        .map(|field| {
            let field_name = &field.name;
            let condition = field.versions.as_check_expr();
            let tag = field.tag.expect("tagged field must have a tag");

            quote! {
                if #condition && !self.#field_name.is_default() {
                    encode_unsigned_varint(&mut tagged_buf, #tag);
                    let len_pos = tagged_buf.len();
                    tagged_buf.extend_from_slice(&[0u8; 5]);
                    let start_len = tagged_buf.len();
                    self.#field_name.encode(&mut tagged_buf, version, is_flexible)?;
                    let data_len = (tagged_buf.len() - start_len) as u32;
                    let len_bytes = varint_len(data_len);
                    let mut len_buf = ::bytes::BytesMut::with_capacity(5);
                    encode_unsigned_varint(&mut len_buf, data_len + 1);
                    tagged_buf[len_pos..len_pos + len_bytes].copy_from_slice(&len_buf[..len_bytes]);
                }
            }
        })
        .collect();

    quote! {
        fn encode(&self, buf: &mut ::bytes::BytesMut, version: i16, is_flexible: bool) -> kafka_client_protocol_core::ProtocolResult<()> {
            use kafka_client_protocol_core::codec::*;
            use ::bytes::BufMut;

            #(#inline_fields)*

            // 灵活版本：编码 tagged fields
            if is_flexible {
                let mut tagged_count = 0u32;
                #(#tagged_check)*
                encode_unsigned_varint(buf, tagged_count);
                if tagged_count > 0 {
                    let mut tagged_buf = ::bytes::BytesMut::new();
                    #(#tagged_encode)*
                    buf.extend_from_slice(&tagged_buf);
                }
            }

            Ok(())
        }
    }
}

/// 生成内联字段（非 tagged）的编码代码
fn generate_encode_field_inline(field: &FieldInfo, flexible_version: Option<i16>) -> TokenStream {
    let condition = field.versions.as_check_expr();
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
fn generate_encode_body(field: &FieldInfo, _flexible_version: Option<i16>) -> TokenStream {
    let field_name = &field.name;
    // is_flexible 是生成函数的参数名，由外层传入
    let is_flex = quote! { is_flexible };

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
                        ::kafka_client_protocol_core::Message::encode(&empty, buf, version, is_flexible)?;
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
                                item.encode(b, version, is_flexible)?;
                                Ok(())
                            }).ok();
                        } else {
                            encode_array(buf, self.#field_name.as_ref().unwrap(), |b, item| -> kafka_client_protocol_core::ProtocolResult<()> {
                                item.encode(b, version, is_flexible)?;
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
                        ::kafka_client_protocol_core::Message::encode(batch, &mut batch_buf, version, is_flexible)?;
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
                        self.#field_name.encode(buf, version, is_flexible)?;
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
                self.#field_name.encode(buf, version, is_flexible)?;
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
                    item.encode(b, version, is_flexible)?;
                    Ok(())
                }).ok();
            } else {
                encode_array(buf, &self.#field_name, |b, item| -> kafka_client_protocol_core::ProtocolResult<()> {
                    item.encode(b, version, is_flexible)?;
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
        quote! { self.#field_name.encode(buf, version, is_flexible)?; }
    } else {
        // 默认：使用 Message trait 的 encode
        quote! { self.#field_name.encode(buf, version, is_flexible)?; }
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
