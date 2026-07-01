// src/encode.rs
use crate::field::FieldInfo;
use crate::version_range::VersionRange;
use proc_macro2::TokenStream;
use quote::quote;

/// 生成 encode 和 flexible_encode 方法
pub fn generate_encode(fields: &[FieldInfo], flexible_version: Option<i16>) -> TokenStream {
    let encode_method = generate_encode_single(fields, flexible_version, false);
    let flexible_encode_method = generate_encode_single(fields, flexible_version, true);

    quote! {
        #encode_method
        #flexible_encode_method
    }
}

/// 生成单个编码方法（encode 或 flexible_encode）
fn generate_encode_single(
    fields: &[FieldInfo],
    _flexible_version: Option<i16>,
    flexible: bool,
) -> TokenStream {
    let method_name = if flexible {
        syn::Ident::new("flexible_encode", proc_macro2::Span::call_site())
    } else {
        syn::Ident::new("encode", proc_macro2::Span::call_site())
    };

    let inline_fields: Vec<TokenStream> = fields
        .iter()
        .filter(|f| f.tagged_versions.is_none())
        .map(|field| generate_encode_field_inline(field, flexible))
        .collect();

    // Tagged fields are only emitted in flexible mode
    let tagged_check: Vec<TokenStream> = if flexible {
        fields
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
            .collect()
    } else {
        Vec::new()
    };

    let tagged_encode: Vec<TokenStream> = if flexible {
        fields
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
                        self.#field_name.flexible_encode(&mut tagged_buf, version)?;
                        let data_len = (tagged_buf.len() - start_len) as u32;
                        let len_bytes = varint_len(data_len);
                        let mut len_buf = ::bytes::BytesMut::with_capacity(5);
                        encode_unsigned_varint(&mut len_buf, data_len + 1);
                        tagged_buf[len_pos..len_pos + len_bytes].copy_from_slice(&len_buf[..len_bytes]);
                    }
                }
            })
            .collect()
    } else {
        Vec::new()
    };

    // 灵活版本 tagged 字段处理
    let tagged_section = if flexible {
        quote! {
            let mut tagged_count = 0u32;
            #(#tagged_check)*
            encode_unsigned_varint(buf, tagged_count);
            if tagged_count > 0 {
                let mut tagged_buf = ::bytes::BytesMut::new();
                #(#tagged_encode)*
                buf.extend_from_slice(&tagged_buf);
            }
        }
    } else {
        TokenStream::new()
    };

    quote! {
        fn #method_name(&self, buf: &mut ::bytes::BytesMut, version: i16) -> kafka_client_protocol_core::ProtocolResult<()> {
            use kafka_client_protocol_core::codec::*;
            use ::bytes::BufMut;

            #(#inline_fields)*

            #tagged_section

            Ok(())
        }
    }
}

/// 生成内联字段（非 tagged）的编码代码
fn generate_encode_field_inline(field: &FieldInfo, flexible: bool) -> TokenStream {
    let condition = field.versions.as_check_expr();
    let encode_body = generate_encode_body(field, flexible);

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
fn generate_encode_body(field: &FieldInfo, flexible: bool) -> TokenStream {
    let field_name = &field.name;

    // Option 类型特殊处理
    if field.is_option() {
        if let Some(nullable_versions) = &field.nullable_versions {
            let null_condition = nullable_versions.as_check_expr();

            // 提取 Option 内部类型
            let inner_ty = FieldInfo::extract_option_inner(&field.ty);

            // None 编码
            let none_encode = if flexible {
                quote! {
                    encode_unsigned_varint(buf, 0);
                }
            } else {
                let inner_str = inner_ty.as_ref().map(is_string_type).unwrap_or(false);
                if inner_str {
                    quote! { buf.put_i16(-1); }
                } else {
                    quote! { buf.put_i32(-1); }
                }
            };

            // 非 nullable 版本的默认编码（None 时）
            let default_encode = inner_ty.as_ref().map(|inner| {
                if FieldInfo::is_vec_type(inner)
                    || is_string_type(inner)
                    || is_bytes_type(inner)
                    || field.encoded_as_bytes
                {
                    if flexible {
                        quote! {
                            encode_unsigned_varint(buf, 1);
                        }
                    } else if is_string_type(inner) {
                        quote! {
                            buf.put_i16(0);
                        }
                    } else {
                        quote! {
                            buf.put_i32(0);
                        }
                    }
                } else {
                    let encode_call = if flexible {
                        quote! { ::kafka_client_protocol_core::Message::flexible_encode }
                    } else {
                        quote! { ::kafka_client_protocol_core::Message::encode }
                    };
                    quote! {
                        let empty: #inner = ::core::default::Default::default();
                        #encode_call(&empty, buf, version)?;
                    }
                }
            });

            // Some 编码
            let some_encode = inner_ty.as_ref().map(|inner| {
                if FieldInfo::is_vec_type(inner) {
                    let item_encode = if flexible {
                        quote! { item.flexible_encode(b, version)?; }
                    } else {
                        quote! { item.encode(b, version)?; }
                    };
                    let array_fn = if flexible { "encode_compact_array" } else { "encode_array" };
                    let array_ident = syn::Ident::new(array_fn, proc_macro2::Span::call_site());
                    quote! {
                        #array_ident(buf, self.#field_name.as_ref().unwrap(), |b, item| -> kafka_client_protocol_core::ProtocolResult<()> {
                            #item_encode
                            Ok(())
                        }).ok();
                    }
                } else if is_string_type(inner) {
                    let encode_fn = if flexible { "encode_compact_nullable_string" } else { "encode_nullable_string" };
                    let fn_ident = syn::Ident::new(encode_fn, proc_macro2::Span::call_site());
                    quote! {
                        #fn_ident(buf, &self.#field_name);
                    }
                } else if is_bytes_type(inner) {
                    let encode_fn = if flexible { "encode_compact_bytes" } else { "encode_bytes" };
                    let fn_ident = syn::Ident::new(encode_fn, proc_macro2::Span::call_site());
                    quote! {
                        #fn_ident(buf, self.#field_name.as_ref().unwrap());
                    }
                } else if field.encoded_as_bytes {
                    let batch_encode = if flexible {
                        quote! { ::kafka_client_protocol_core::Message::flexible_encode }
                    } else {
                        quote! { ::kafka_client_protocol_core::Message::encode }
                    };
                    let length_prefix = if flexible {
                        quote! { encode_unsigned_varint(buf, batch_buf.len() as u32 + 1); }
                    } else {
                        quote! { buf.put_i32(batch_buf.len() as i32); }
                    };
                    quote! {
                        let batch = self.#field_name.as_ref().unwrap();
                        let mut batch_buf = ::bytes::BytesMut::new();
                        #batch_encode(batch, &mut batch_buf, version)?;
                        #length_prefix
                        buf.extend_from_slice(&batch_buf);
                    }
                } else {
                    let encode_call = if flexible {
                        quote! { self.#field_name.flexible_encode(buf, version)?; }
                    } else {
                        quote! { self.#field_name.encode(buf, version)?; }
                    };
                    quote! { #encode_call }
                }
            });

            return quote! {
                if #null_condition && self.#field_name.is_none() {
                    #none_encode
                } else if self.#field_name.is_none() {
                    // Not nullable at this version: encode as default/empty
                    #default_encode
                } else {
                    #some_encode
                }
            };
        } else {
            let encode_call = if flexible {
                quote! { self.#field_name.flexible_encode(buf, version)?; }
            } else {
                quote! { self.#field_name.encode(buf, version)?; }
            };
            return quote! {
                #encode_call
            };
        }
    }

    // 根据类型生成编码代码
    if field.is_string() {
        let encode_fn = if flexible {
            "encode_compact_string"
        } else {
            "encode_string"
        };
        let fn_ident = syn::Ident::new(encode_fn, proc_macro2::Span::call_site());
        quote! {
            #fn_ident(buf, &self.#field_name);
        }
    } else if field.is_bytes() {
        let encode_fn = if flexible {
            "encode_compact_bytes"
        } else {
            "encode_bytes"
        };
        let fn_ident = syn::Ident::new(encode_fn, proc_macro2::Span::call_site());
        quote! {
            #fn_ident(buf, &self.#field_name);
        }
    } else if field.is_vec() {
        let array_fn = if flexible {
            "encode_compact_array"
        } else {
            "encode_array"
        };
        let array_ident = syn::Ident::new(array_fn, proc_macro2::Span::call_site());
        let item_encode = if flexible {
            quote! { item.flexible_encode(b, version)?; }
        } else {
            quote! { item.encode(b, version)?; }
        };
        quote! {
            #array_ident(buf, &self.#field_name, |b, item| -> kafka_client_protocol_core::ProtocolResult<()> {
                #item_encode
                Ok(())
            }).ok();
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
        let encode_call = if flexible {
            quote! { self.#field_name.flexible_encode(buf, version)?; }
        } else {
            quote! { self.#field_name.encode(buf, version)?; }
        };
        quote! { #encode_call }
    } else {
        // 默认：使用 Message trait 的 encode
        let encode_call = if flexible {
            quote! { self.#field_name.flexible_encode(buf, version)?; }
        } else {
            quote! { self.#field_name.encode(buf, version)?; }
        };
        quote! { #encode_call }
    }
}

/// 检查类型是否是 String
fn is_string_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.first()
    {
        return segment.ident == "String";
    }
    false
}

/// 检查类型是否是 Bytes
fn is_bytes_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.first()
    {
        return segment.ident == "Bytes";
    }
    false
}
