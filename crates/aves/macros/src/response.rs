use proc_macro2::TokenStream as TokenStream2;
use quote::ToTokens;

use super::request::{normalize_type, parse_type_attr, public_projection_type};

pub(super) struct ResponseEnum {
    name: String,
    variants: Vec<ResponseVariant>,
}

struct ResponseVariant {
    name: String,
    fields: ResponseFields,
    skip_generation: bool,
}

enum ResponseFields {
    Unit,
    Named(Vec<ResponseField>),
    Tuple(Vec<String>),
}

struct ResponseField {
    name: String,
    ty: String,
    attrs: String,
}

const OUTPUT_STRUCT_HEADER: &str = "#[doc = \"Generated operation-specific output projection for this response variant.\"]\n#[derive(Clone, Debug, ::serde::Deserialize, ::serde::Serialize, ::specta::Type, ::schemars::JsonSchema)]\n#[serde(rename_all = \"camelCase\")]\n";

impl ResponseField {
    fn public_ty(&self) -> String {
        public_projection_type(&self.ty)
    }

    fn set_canonical_expression(&self) -> String {
        if self.public_ty() == self.ty {
            self.name.clone()
        } else {
            format!("{}.into()", self.name)
        }
    }
}

impl ResponseEnum {
    pub(super) fn parse(attr: TokenStream2, item: TokenStream2) -> Result<Self, String> {
        let parsed =
            syn::parse2::<syn::ItemEnum>(item).map_err(|_| "bus_responses must be applied to an enum".to_string())?;
        if parse_type_attr(&attr, "root_error")?.is_some() {
            return Err("bus_responses does not accept root_error; response projection is domain-neutral".to_string());
        }
        let name = parsed.ident.to_string();
        let variants = parse_variants(parsed.variants.into_iter());

        Ok(Self { name, variants })
    }

    pub(super) fn render(&self) -> String {
        let mut rendered = String::new();

        for variant in &self.variants {
            if variant.skip_generation {
                rendered.push_str(&variant.render_internal_projection());
                rendered.push_str(&variant.render_try_from_impl(&self.name, true));
            } else {
                rendered.push_str(&variant.render_output_struct());
                rendered.push_str(&variant.render_try_from_impl(&self.name, false));
            }
        }

        rendered
    }
}

impl ResponseVariant {
    fn output_name(&self) -> String {
        format!("{}Output", self.name)
    }

    fn render_output_struct(&self) -> String {
        let output_name = self.output_name();
        let declaration = match &self.fields {
            ResponseFields::Unit => format!("pub struct {output_name};\n"),
            ResponseFields::Named(fields) => format!(
                "pub struct {output_name} {{\n{}\n}}\n",
                fields
                    .iter()
                    .map(|field| {
                        let attrs = if field.attrs.is_empty() {
                            String::new()
                        } else {
                            format!("{}\n", field.attrs)
                        };
                        let public_ty = field.public_ty();
                        format!("{attrs}pub {}: {public_ty},", field.name)
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            ),
            ResponseFields::Tuple(fields) => format!(
                "pub struct {output_name}({});\n",
                fields
                    .iter()
                    .map(|field| format!("pub {}", public_projection_type(field)))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        };
        format!("{OUTPUT_STRUCT_HEADER}{declaration}")
    }

    fn render_internal_projection(&self) -> String {
        let output_name = self.output_name();
        match &self.fields {
            ResponseFields::Unit => format!("#[doc(hidden)]\npub struct {output_name};\n"),
            ResponseFields::Named(fields) => {
                let declarations = fields
                    .iter()
                    .map(|field| format!("pub {}: {},", field.name, field.ty))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("#[doc(hidden)]\npub struct {output_name} {{\n{declarations}\n}}\n")
            }
            ResponseFields::Tuple(fields) => {
                let declarations = fields.join(", ");
                format!("#[doc(hidden)]\npub struct {output_name}({declarations});\n")
            }
        }
    }

    fn render_try_from_impl(&self, enum_name: &str, internal: bool) -> String {
        let output_name = self.output_name();
        let variant_name = &self.name;
        let expected = format!("{enum_name}::{variant_name}");

        let success = self.render_success_arm(enum_name, internal);

        format!(
            "impl ::core::convert::TryFrom<{enum_name}> for {output_name} {{\n    type Error = ::aves::ResponseProjectionError;\n\n    fn try_from(response: {enum_name}) -> ::core::result::Result<Self, Self::Error> {{\n        match response {{\n            {success},\n            unexpected => Err(::aves::ResponseProjectionError {{ expected: \"{expected}\".to_string(), actual: format!(\"{{:?}}\", &unexpected) }}),\n        }}\n    }}\n}}\n"
        )
    }

    fn render_success_arm(&self, enum_name: &str, internal: bool) -> String {
        let variant = &self.name;
        match &self.fields {
            ResponseFields::Unit => format!("{enum_name}::{variant} => Ok(Self)"),
            ResponseFields::Named(fields) => {
                let names = fields
                    .iter()
                    .map(|field| field.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let values = fields
                    .iter()
                    .map(|field| {
                        let value = if internal {
                            field.name.clone()
                        } else {
                            field.set_canonical_expression()
                        };
                        format!("{}: {value}", field.name)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{enum_name}::{variant} {{ {names} }} => Ok(Self {{ {values} }})")
            }
            ResponseFields::Tuple(fields) => {
                let names = (0..fields.len())
                    .map(|index| format!("field_{index}"))
                    .collect::<Vec<_>>();
                let values = fields
                    .iter()
                    .enumerate()
                    .map(|(index, ty)| {
                        let name = &names[index];
                        if internal || public_projection_type(ty) == *ty {
                            name.clone()
                        } else {
                            format!("{name}.into()")
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{enum_name}::{variant}({}) => Ok(Self({values}))", names.join(", "))
            }
        }
    }
}

fn parse_variants(variants: impl Iterator<Item = syn::Variant>) -> Vec<ResponseVariant> {
    variants.map(parse_variant).collect()
}

fn parse_variant(variant: syn::Variant) -> ResponseVariant {
    let name = variant.ident.to_string();
    let skip_generation = has_serde_skip(&variant.attrs);
    let fields = match variant.fields {
        syn::Fields::Unit => ResponseFields::Unit,
        syn::Fields::Named(fields) => ResponseFields::Named(
            fields
                .named
                .into_iter()
                .map(|field| ResponseField {
                    name: field
                        .ident
                        .expect("named fields have identifiers")
                        .to_string(),
                    ty: normalize_type(&field.ty.to_token_stream().to_string()),
                    attrs: field
                        .attrs
                        .iter()
                        .map(ToTokens::to_token_stream)
                        .map(|attr| attr.to_string())
                        .collect::<Vec<_>>()
                        .join("\n"),
                })
                .collect(),
        ),
        syn::Fields::Unnamed(fields) => ResponseFields::Tuple(
            fields
                .unnamed
                .into_iter()
                .map(|field| normalize_type(&field.ty.to_token_stream().to_string()))
                .collect(),
        ),
    };

    ResponseVariant {
        name,
        fields,
        skip_generation,
    }
}

fn has_serde_skip(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        let Ok(meta) = attr.meta.require_list() else {
            return false;
        };

        meta.path.is_ident("serde")
            && attr
                .parse_args::<syn::Path>()
                .is_ok_and(|path| path.is_ident("skip"))
    })
}
