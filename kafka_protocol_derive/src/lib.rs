//! Kafka Protocol Derive Macros
//!
//! 模仿 Prost 的派生宏设计，提供自定义 derive 宏来简化 Message trait 的实现

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, Fields, Type, PathArguments, GenericArgument};

/// 版本范围
#[derive(Debug, Clone, PartialEq)]
enum VersionRange {
    Exact(i16),
    Range(i16, i16),  // inclusive
    From(i16),        // 版本+ 格式
    All,
}

impl VersionRange {
    pub fn parse(s: &str) -> Self {
        let s = s.trim();

        if s == "0+" || s == "none" || s == "all" || s.is_empty() {
            return VersionRange::All;
        }

        if s.contains('-') {
            let parts: Vec<&str> = s.split('-').collect();
            if parts.len() == 2 {
                if let (Ok(start), Ok(end)) = (parts[0].parse::<i16>(), parts[1].parse::<i16>()) {
                    return VersionRange::Range(start, end);
                }
            }
        }

        if s.ends_with('+') {
            if let Ok(start) = s.trim_end_matches('+').parse::<i16>() {
                return VersionRange::From(start);
            }
        }

        if let Ok(v) = s.parse::<i16>() {
            VersionRange::Exact(v)
        } else {
            VersionRange::All
        }
    }

    pub fn contains(&self, version: i16) -> bool {
        match self {
            VersionRange::Exact(v) => version == *v,
            VersionRange::Range(start, end) => version >= *start && version <= *end,
            VersionRange::From(start) => version >= *start,
            VersionRange::All => true,
        }
    }

    pub fn min_version(&self) -> i16 {
        match self {
            VersionRange::Exact(v) => *v,
            VersionRange::Range(start, _) => *start,
            VersionRange::From(start) => *start,
            VersionRange::All => 0,
        }
    }

    pub fn as_check_expr(&self) -> proc_macro2::TokenStream {
        match self {
            VersionRange::Exact(v) => quote::quote! { version == #v },
            VersionRange::Range(start, end) => quote::quote! { version >= #start && version <= #end },
            VersionRange::From(start) => quote::quote! { version >= #start },
            VersionRange::All => quote::quote! { true },
        }
    }
}

/// 解析后的字段信息
struct FieldInfo {
    pub name: syn::Ident,
    pub ty: Type,
    pub versions: VersionRange,
    pub nullable: bool,
    pub flexible: bool,
    pub default: Option<String>,
    pub about: Option<String>,
}

/// 解析字段
fn parse_field(field: &syn::Field) -> FieldInfo {
    let name = field.ident.clone().unwrap();
    let ty = field.ty.clone();

    let mut versions = VersionRange::All;
    let mut nullable = false;
    let mut flexible = false;
    let mut default = None;
    let mut about = None;

    for attr in &field.attrs {
        if attr.path().is_ident("kafka") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("versions") {
                    let value: syn::LitStr = meta.value()?.parse()?;
                    versions = VersionRange::parse(&value.value());
                } else if meta.path.is_ident("nullable") {
                    nullable = true;
                } else if meta.path.is_ident("flexible") {
                    flexible = true;
                } else if meta.path.is_ident("default") {
                    let value: syn::LitStr = meta.value()?.parse()?;
                    default = Some(value.value());
                } else if meta.path.is_ident("about") {
                    let value: syn::LitStr = meta.value()?.parse()?;
                    about = Some(value.value());
                }
                Ok(())
            }).ok();
        }
    }

    // 如果类型是 Option<T>，自动设置为 nullable
    if is_option_type(&ty) {
        nullable = true;
    }

    FieldInfo {
        name,
        ty,
        versions,
        nullable,
        flexible,
        default,
        about,
    }
}

/// 检查类型是否是 Option<T>
fn is_option_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.first() {
            return segment.ident == "Option";
        }
    }
    false
}

/// 为结构体自动实现 Message trait
///
/// # 示例
/// ```rust
/// #[derive(KafkaMessage)]
/// #[kafka(api_key = 0, valid_versions = "0-12")]
/// struct ProduceRequest {
///     #[kafka(versions = "3+", nullable)]
///     transactional_id: Option<String>,
///     acks: i16,
/// }
/// ```
#[proc_macro_derive(KafkaMessage, attributes(kafka))]
pub fn derive_kafka_message(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // 解析结构体级别的属性
    let mut api_key = None;
    let mut valid_versions = VersionRange::All;
    let mut flexible_versions = None;

    for attr in &input.attrs {
        if attr.path().is_ident("kafka") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("api_key") {
                    let value: syn::LitInt = meta.value()?.parse()?;
                    api_key = Some(value.base10_parse::<i16>()?);
                } else if meta.path.is_ident("valid_versions") {
                    let value: syn::LitStr = meta.value()?.parse()?;
                    valid_versions = VersionRange::parse(&value.value());
                } else if meta.path.is_ident("flexible_versions") {
                    let value: syn::LitStr = meta.value()?.parse()?;
                    flexible_versions = Some(VersionRange::parse(&value.value()));
                }
                Ok(())
            }).ok();
        }
    }

    let api_key_impl = match api_key {
        Some(key) => quote! {
            fn api_key(&self) -> Option<i16> {
                Some(#key)
            }
        },
        None => quote! {
            fn api_key(&self) -> Option<i16> {
                None
            }
        },
    };

    let min_version = valid_versions.min_version();

    // 解析字段并生成 encode/decode
    let (encode_impl, decode_impl) = match &input.data {
        Data::Struct(data) => {
            match &data.fields {
                Fields::Named(fields) => {
                    let field_infos: Vec<_> = fields.named.iter()
                        .map(|f| parse_field(f))
                        .collect();

                    let encode_fields: Vec<_> = field_infos.iter()
                        .map(|f| generate_encode_field(f, &flexible_versions))
                        .collect();

                    let decode_fields: Vec<_> = field_infos.iter()
                        .map(|f| generate_decode_field(f, &flexible_versions))
                        .collect();

                    let field_names: Vec<_> = field_infos.iter()
                        .map(|f| &f.name)
                        .collect();

                    let encode_body = quote! {
                        #( #encode_fields )*
                        Ok(())
                    };

                    let decode_body = quote! {
                        Ok(Self {
                            #( #field_names: #decode_fields, )*
                        })
                    };

                    (encode_body, decode_body)
                }
                _ => (quote!(Ok(())), quote!(Ok(Self {}))),
            }
        }
        _ => (quote!(Ok(())), quote!(Ok(Self {}))),
    };

    let type_name = name.to_string();

    // 生成 encoded_len 方法
    let encoded_len_impl = quote! {
        fn encoded_len(&self, version: i16) -> usize {
            let mut buf = BytesMut::new();
            self.encode(&mut buf, version).unwrap();
            buf.len()
        }
    };

    let expanded = quote! {
        impl Message for #name {
            fn type_name() -> &'static str {
                #type_name
            }

            #api_key_impl

            fn default_version() -> i16 {
                #min_version
            }

            fn encode(&self, buf: &mut BytesMut, version: i16) -> ProtocolResult<()> {
                use crate::protocol::*;
                use bytes::BufMut;
                #encode_impl
            }

            fn decode(buf: &mut Bytes, version: i16) -> ProtocolResult<Self> {
                use crate::protocol::*;
                use bytes::Buf;
                #decode_impl
            }

            #encoded_len_impl
        }
    };

    TokenStream::from(expanded)
}

/// 为请求结构体自动实现 RequestMessage trait
#[proc_macro_derive(KafkaRequest, attributes(kafka))]
pub fn derive_kafka_request(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // 解析 api_key
    let mut api_key = 0i16;
    for attr in &input.attrs {
        if attr.path().is_ident("kafka") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("api_key") {
                    let value: syn::LitInt = meta.value()?.parse()?;
                    api_key = value.base10_parse::<i16>()?;
                }
                Ok(())
            }).ok();
        }
    }

    let expanded = quote! {
        impl RequestMessage for #name {
            fn request_header(&self, version: i16, correlation_id: i32, client_id: &str) -> RequestHeader {
                RequestHeader::new_v1(
                    #api_key,
                    version,
                    correlation_id,
                    if client_id.is_empty() { None } else { Some(client_id.to_string()) },
                )
            }
        }
    };

    TokenStream::from(expanded)
}

/// 为响应结构体自动实现 ResponseMessage trait
#[proc_macro_derive(KafkaResponse)]
pub fn derive_kafka_response(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let expanded = quote! {
        impl ResponseMessage for #name {}
    };

    TokenStream::from(expanded)
}

fn generate_encode_field(
    field: &FieldInfo,
    flexible_versions: &Option<VersionRange>,
) -> proc_macro2::TokenStream {
    let _field_name = &field.name;
    let condition = field.versions.as_check_expr();
    let is_flexible = flexible_versions.as_ref().map(|v| v.contains(0)).unwrap_or(false);

    let encode_body = generate_encode_body(field, is_flexible);

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

fn generate_encode_body(field: &FieldInfo, is_flexible: bool) -> proc_macro2::TokenStream {
    let field_name = &field.name;
    let ty = &field.ty;

    // 检查是否是 Option<T>
    if let Some(inner_ty) = extract_option_inner_type(ty) {
        let inner_encode = generate_encode_for_inner_type(&inner_ty, quote!(v), is_flexible);
        return quote! {
            if let Some(ref v) = self.#field_name {
                #inner_encode
            }
        };
    }

    // 类型匹配
    if let Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            let type_name = seg.ident.to_string();

            match type_name.as_str() {
                "String" => {
                    return if is_flexible {
                        quote! { encode_compact_string(buf, &self.#field_name); }
                    } else {
                        quote! { encode_string(buf, &self.#field_name); }
                    };
                }
                "Vec" => {
                    // 提取 Vec 的内部类型
                    if let Some(inner_ty) = extract_vec_inner_type(ty) {
                        // 检查是否是 Vec<u8> (bytes)
                        if is_u8_type(&inner_ty) {
                            return if is_flexible {
                                quote! { encode_compact_bytes(buf, &self.#field_name); }
                            } else {
                                // 非灵活版本使用普通数组编码
                                quote! {
                                    encode_array(buf, &self.#field_name, |b, item| {
                                        b.put_u8(*item);
                                    });
                                }
                            };
                        }
                        
                        // 检查是否是 Vec<String>
                        if is_string_type(&inner_ty) {
                            return if is_flexible {
                                quote! {
                                    encode_compact_array(buf, &self.#field_name, |b, item| {
                                        encode_compact_string(b, item);
                                    });
                                }
                            } else {
                                quote! {
                                    encode_array(buf, &self.#field_name, |b, item| {
                                        encode_string(b, item);
                                    });
                                }
                            };
                        }
                        
                        if is_primitive_type(&inner_ty) {
                            // 基本类型数组，使用 put_xxx 系列方法
                            let encode_item = generate_primitive_encode(quote!(item), &inner_ty);
                            
                            return if is_flexible {
                                quote! {
                                    encode_compact_array(buf, &self.#field_name, |b, item| {
                                        #encode_item
                                    });
                                }
                            } else {
                                quote! {
                                    encode_array(buf, &self.#field_name, |b, item| {
                                        #encode_item
                                    });
                                }
                            };
                        }
                    }
                    // 复杂类型数组，使用 Message trait
                    return if is_flexible {
                        quote! {
                            encode_compact_array(buf, &self.#field_name, |b, item| {
                                item.encode(b, version).unwrap();
                            });
                        }
                    } else {
                        quote! {
                            encode_array(buf, &self.#field_name, |b, item| {
                                item.encode(b, version).unwrap();
                            });
                        }
                    };
                }
                "bool" => {
                    return quote! { buf.put_i8(self.#field_name as i8); };
                }
                "i8" => {
                    return quote! { buf.put_i8(self.#field_name); };
                }
                "i16" => {
                    return quote! { buf.put_i16(self.#field_name); };
                }
                "i32" => {
                    return quote! { buf.put_i32(self.#field_name); };
                }
                "i64" => {
                    return quote! { buf.put_i64(self.#field_name); };
                }
                "Uuid" => {
                    return quote! { self.#field_name.encode(buf); };
                }
                _ => {}
            }
        }
    }

    // 默认：使用 Message trait 的 encode
    quote! {
        self.#field_name.encode(buf, version)?;
    }
}

fn generate_encode_for_inner_type(
    ty: &Type,
    value_expr: proc_macro2::TokenStream,
    is_flexible: bool,
) -> proc_macro2::TokenStream {
    if let Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            let type_name = seg.ident.to_string();

            match type_name.as_str() {
                "String" => {
                    return if is_flexible {
                        quote! { encode_compact_nullable_string(buf, &#value_expr); }
                    } else {
                        quote! { encode_nullable_string(buf, &#value_expr); }
                    };
                }
                "Vec" => {
                    return if is_flexible {
                        quote! {
                            encode_compact_array(buf, &#value_expr, |b, item| {
                                item.encode(b, version).unwrap();
                            });
                        }
                    } else {
                        quote! {
                            encode_array(buf, &#value_expr, |b, item| {
                                item.encode(b, version).unwrap();
                            });
                        }
                    };
                }
                "i8" => return quote! { buf.put_i8(#value_expr); },
                "i16" => return quote! { buf.put_i16(#value_expr); },
                "i32" => return quote! { buf.put_i32(#value_expr); },
                "i64" => return quote! { buf.put_i64(#value_expr) },
                "bool" => return quote! { buf.put_i8(#value_expr as i8) },
                "Uuid" => return quote! { #value_expr.encode(buf) },
                _ => {}
            }
        }
    }

    quote! { #value_expr.encode(buf, version)?; }
}

fn generate_decode_field(
    field: &FieldInfo,
    flexible_versions: &Option<VersionRange>,
) -> proc_macro2::TokenStream {
    let condition = field.versions.as_check_expr();
    let is_flexible = flexible_versions.as_ref().map(|v| v.contains(0)).unwrap_or(false);

    let decode_body = generate_decode_body(field, is_flexible);

    let default_val = if let Some(ref default) = field.default {
        if default == "null" || default == "None" {
            quote!(None)
        } else if default.parse::<i64>().is_ok() {
            let num: i64 = default.parse().unwrap();
            quote!(#num)
        } else {
            quote!(#default.to_string())
        }
    } else {
        quote!(Default::default())
    };

    if field.versions == VersionRange::All {
        decode_body
    } else {
        quote! {
            if #condition {
                #decode_body
            } else {
                #default_val
            }
        }
    }
}

fn generate_decode_body(field: &FieldInfo, is_flexible: bool) -> proc_macro2::TokenStream {
    let ty = &field.ty;

    // 检查是否是 Option<T>
    if let Some(inner_ty) = extract_option_inner_type(ty) {
        let inner_decode = generate_decode_for_inner_type(&inner_ty, is_flexible);
        return quote! { Some(#inner_decode) };
    }

    // 类型匹配
    if let Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            let type_name = seg.ident.to_string();

            match type_name.as_str() {
                "String" => {
                    return if is_flexible {
                        quote! { decode_compact_string(buf)? }
                    } else {
                        quote! { decode_string(buf)? }
                    };
                }
                "Vec" => {
                    // 提取 Vec 的内部类型
                    if let Some(inner_ty) = extract_vec_inner_type(ty) {
                        // 检查是否是 Vec<u8> (bytes)
                        if is_u8_type(&inner_ty) {
                            return if is_flexible {
                                quote! { decode_compact_bytes(buf)?.to_vec() }
                            } else {
                                // 非灵活版本使用普通数组解码
                                quote! { decode_array(buf, |b| Ok::<_, ProtocolError>(b.get_u8()))? }
                            };
                        }
                        
                        // 检查是否是 Vec<String>
                        if is_string_type(&inner_ty) {
                            return if is_flexible {
                                quote! { decode_compact_array(buf, |b| decode_compact_string(b))? }
                            } else {
                                quote! { decode_array(buf, |b| decode_string(b))? }
                            };
                        }
                        
                        if is_primitive_type(&inner_ty) {
                            let decode_item = generate_primitive_decode(&inner_ty);
                            return if is_flexible {
                                quote! { decode_compact_array(buf, |b| #decode_item)? }
                            } else {
                                quote! { decode_array(buf, |b| #decode_item)? }
                            };
                        }
                    }
                    // 复杂类型数组，使用 Message trait
                    return if is_flexible {
                        quote! { decode_compact_array(buf, |b| Message::decode(b, version))? }
                    } else {
                        quote! { decode_array(buf, |b| Message::decode(b, version))? }
                    };
                }
                "bool" => {
                    return quote! { buf.get_i8() != 0 };
                }
                "i8" => return quote! { buf.get_i8() },
                "i16" => return quote! { buf.get_i16() },
                "i32" => return quote! { buf.get_i32() },
                "i64" => return quote! { buf.get_i64() },
                "Uuid" => return quote! { Uuid::decode(buf)? },
                _ => {}
            }
        }
    }

    // 默认：使用 Message trait 的 decode
    quote! { <#ty as Message>::decode(buf, version)? }
}

fn generate_decode_for_inner_type(ty: &Type, is_flexible: bool) -> proc_macro2::TokenStream {
    if let Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            let type_name = seg.ident.to_string();

            match type_name.as_str() {
                "String" => {
                    return if is_flexible {
                        quote! { decode_compact_nullable_string(buf)? }
                    } else {
                        quote! { decode_nullable_string(buf)? }
                    };
                }
                "Vec" => {
                    // 提取 Vec 的内部类型
                    if let Some(inner_ty) = extract_vec_inner_type(ty) {
                        if is_primitive_type(&inner_ty) {
                            let decode_item = generate_primitive_decode(&inner_ty);
                            return if is_flexible {
                                quote! { decode_compact_array(buf, |b| #decode_item)? }
                            } else {
                                quote! { decode_array(buf, |b| #decode_item)? }
                            };
                        }
                    }
                    // 复杂类型数组，使用 Message trait
                    return if is_flexible {
                        quote! { decode_compact_array(buf, |b| Message::decode(b, version))? }
                    } else {
                        quote! { decode_array(buf, |b| Message::decode(b, version))? }
                    };
                }
                "i8" => return quote! { buf.get_i8() },
                "i16" => return quote! { buf.get_i16() },
                "i32" => return quote! { buf.get_i32() },
                "i64" => return quote! { buf.get_i64() },
                "bool" => return quote! { buf.get_i8() != 0 },
                "Uuid" => return quote! { Uuid::decode(buf)? },
                _ => {}
            }
        }
    }

    quote! { <#ty as Message>::decode(buf, version)? }
}

/// 从 Option<T> 中提取内部类型 T
fn extract_option_inner_type(ty: &Type) -> Option<Type> {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.first() {
            if segment.ident == "Option" {
                if let PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(GenericArgument::Type(inner_ty)) = args.args.first() {
                        return Some(inner_ty.clone());
                    }
                }
            }
        }
    }
    None
}

/// 从 Vec<T> 中提取内部类型 T
fn extract_vec_inner_type(ty: &Type) -> Option<Type> {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.first() {
            if segment.ident == "Vec" {
                if let PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(GenericArgument::Type(inner_ty)) = args.args.first() {
                        return Some(inner_ty.clone());
                    }
                }
            }
        }
    }
    None
}

/// 检查类型是否是 u8
fn is_u8_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            return seg.ident == "u8";
        }
    }
    false
}

/// 检查类型是否是 String
fn is_string_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            return seg.ident == "String";
        }
    }
    false
}

/// 检查类型是否是基本类型（i8, i16, i32, i64, bool, String, Uuid）
fn is_primitive_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            let type_name = seg.ident.to_string();
            return matches!(
                type_name.as_str(),
                "i8" | "i16" | "i32" | "i64" | "bool" | "String" | "Uuid"
            );
        }
    }
    false
}

/// 为基本类型生成编码代码
fn generate_primitive_encode(expr: proc_macro2::TokenStream, ty: &Type) -> proc_macro2::TokenStream {
    if let Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            let type_name = seg.ident.to_string();
            return match type_name.as_str() {
                "i8" => quote! { buf.put_i8(*#expr); },
                "i16" => quote! { buf.put_i16(*#expr); },
                "i32" => quote! { buf.put_i32(*#expr); },
                "i64" => quote! { buf.put_i64(*#expr); },
                "bool" => quote! { buf.put_i8(*#expr as i8); },
                "String" => quote! { encode_string(buf, #expr); },
                "Uuid" => quote! { #expr.encode(buf); },
                _ => quote! { #expr.encode(buf, version)?; },
            };
        }
    }
    quote! { #expr.encode(buf, version)?; }
}

/// 为基本类型生成解码代码（用于数组内部）
fn generate_primitive_decode(ty: &Type) -> proc_macro2::TokenStream {
    if let Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            let type_name = seg.ident.to_string();
            return match type_name.as_str() {
                "i8" => quote! { Ok::<_, ProtocolError>(buf.get_i8()) },
                "i16" => quote! { Ok::<_, ProtocolError>(buf.get_i16()) },
                "i32" => quote! { Ok::<_, ProtocolError>(buf.get_i32()) },
                "i64" => quote! { Ok::<_, ProtocolError>(buf.get_i64()) },
                "bool" => quote! { Ok::<_, ProtocolError>(buf.get_i8() != 0) },
                "String" => quote! { decode_string(buf) },
                "Uuid" => quote! { Uuid::decode(buf) },
                _ => quote! { <#ty as Message>::decode(buf, version) },
            };
        }
    }
    quote! { <#ty as Message>::decode(buf, version) }
}
