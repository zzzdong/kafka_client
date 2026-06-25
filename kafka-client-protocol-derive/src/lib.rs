// src/lib.rs
use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

mod decode;
mod encode;
mod field;
mod size;
mod version_range;

use field::FieldInfo;
use version_range::VersionRange;

/// 派生 KafkaMessage trait
///
/// # 示例
/// ```ignore
/// use kafka_client_protocol_derive::KafkaMessage;
///
/// // 请求消息
/// #[derive(KafkaMessage, Debug, Clone, Default)]
/// #[kafka(api_key = 3, msg_type = "request", valid_versions = "0-12", flexible_versions = "9+")]
/// pub struct MetadataRequest {
///     #[kafka(versions = "0+", nullable_versions = "0+")]
///     pub topics: Option<Vec<String>>,
///     #[kafka(versions = "4+", default = "true")]
///     pub allow_auto_topic_creation: bool,
/// }
///
/// // 响应消息
/// #[derive(KafkaMessage, Debug, Clone, Default)]
/// #[kafka(api_key = 3, msg_type = "response", valid_versions = "0-12", flexible_versions = "9+")]
/// pub struct MetadataResponse {
///     #[kafka(versions = "0+")]
///     pub brokers: Vec<Broker>,
///     #[kafka(versions = "2+", nullable_versions = "2+")]
///     pub cluster_id: Option<String>,
/// }
///
/// // 普通结构体
/// #[derive(KafkaMessage, Debug, Clone, Default)]
/// #[kafka(valid_versions = "0+")]
/// pub struct Broker {
///     #[kafka(versions = "0+")]
///     pub node_id: i32,
/// }
/// ```
#[proc_macro_derive(KafkaMessage, attributes(kafka))]
pub fn kafka_message_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // 解析结构体属性
    let mut api_key = None;
    let mut msg_type = None;
    let mut valid_versions = VersionRange::All;
    let mut flexible_versions = None;
    let mut flexible_versions_raw = String::new();

    for attr in &input.attrs {
        if attr.path().is_ident("kafka") {
            parse_struct_attr(
                attr,
                &mut api_key,
                &mut msg_type,
                &mut valid_versions,
                &mut flexible_versions,
                &mut flexible_versions_raw,
            );
        }
    }

    // 解析字段
    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => fields
                .named
                .iter()
                .map(FieldInfo::from_field)
                .collect::<Vec<_>>(),
            _ => {
                return syn::Error::new_spanned(&input, "Only named fields are supported")
                    .to_compile_error()
                    .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(&input, "Only structs are supported")
                .to_compile_error()
                .into();
        }
    };

    let flexible_start = flexible_versions.as_ref().map(|v| v.min_version());

    let encode_impl = encode::generate_encode(&fields, flexible_start);
    let decode_impl = decode::generate_decode(&fields, flexible_start);
    let size_impl = size::generate_size(&fields, flexible_start);

    let min_version = valid_versions.min_version();
    let max_version = valid_versions.max_version().unwrap_or(i16::MAX);
    // "none" 表示不支持 flexible，即使 parse 后 min_version=0
    let flexible_version_expr = if flexible_versions_raw == "none" {
        quote! { None }
    } else if let Some(ref fv) = flexible_versions {
        let v = fv.min_version();
        quote! { Some(#v) }
    } else {
        quote! { None }
    };

    let type_name = name.to_string();

    // 构建 Message trait 实现
    let message_impl = quote! {
        impl ::kafka_client_protocol_core::Message for #name {
            fn type_name() -> &'static str {
                #type_name
            }

            fn min_version() -> i16 {
                #min_version
            }

            fn max_version() -> i16 {
                #max_version
            }

            fn flexible_version() -> Option<i16> {
                #flexible_version_expr
            }

            #encode_impl
            #decode_impl
            #size_impl
        }
    };

    // 根据是否有 api_key 和 msg_type 生成额外的 trait 实现
    let extra_impls = if let (Some(key), Some(ty)) = (api_key, msg_type) {
        match ty.as_str() {
            "request" => {
                quote! {
                    impl ::kafka_client_protocol_core::Request for #name {
                        fn api_key(&self) -> i16 {
                            #key
                        }
                    }
                }
            }
            "response" => {
                quote! {
                    impl ::kafka_client_protocol_core::Response for #name {
                        fn api_key(&self) -> i16 {
                            #key
                        }
                    }
                }
            }
            _ => quote! {},
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #message_impl
        #extra_impls
    };

    TokenStream::from(expanded)
}

/// 解析结构体属性
fn parse_struct_attr(
    attr: &syn::Attribute,
    api_key: &mut Option<i16>,
    msg_type: &mut Option<String>,
    valid_versions: &mut VersionRange,
    flexible_versions: &mut Option<VersionRange>,
    flexible_versions_raw: &mut String,
) {
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("api_key") {
            let value: syn::LitInt = meta.value()?.parse()?;
            *api_key = Some(value.base10_parse()?);
        } else if meta.path.is_ident("msg_type") {
            let value: syn::LitStr = meta.value()?.parse()?;
            *msg_type = Some(value.value());
        } else if meta.path.is_ident("valid_versions") {
            let value: syn::LitStr = meta.value()?.parse()?;
            *valid_versions = VersionRange::parse(&value.value());
        } else if meta.path.is_ident("flexible_versions") {
            let value: syn::LitStr = meta.value()?.parse()?;
            let raw = value.value();
            *flexible_versions_raw = raw.clone();
            *flexible_versions = Some(VersionRange::parse(&raw));
        }
        Ok(())
    })
    .ok();
}
