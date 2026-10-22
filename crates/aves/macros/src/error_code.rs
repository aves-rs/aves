use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Fields, ItemEnum, LitStr, parse2};

pub(super) fn render_error_codes(item: TokenStream2) -> Result<TokenStream2, String> {
    let parsed = parse2::<ItemEnum>(item).map_err(|_| "ErrorCode must derive on an enum".to_string())?;
    if parsed.variants.is_empty() {
        return Err("ErrorCode enum has no variants".into());
    }

    let arms = parsed
        .variants
        .iter()
        .map(|variant| {
            let name = &variant.ident;
            let pattern = match &variant.fields {
                Fields::Unit => quote! {},
                Fields::Named(_) => quote! { { .. } },
                Fields::Unnamed(_) => quote! { (..) },
            };
            let code = variant
                .attrs
                .iter()
                .find(|attribute| attribute.path().is_ident("error_code"))
                .map(|attribute| {
                    let meta = attribute
                        .meta
                        .require_list()
                        .map_err(|_| "error_code requires a string literal")?;
                    parse2::<LitStr>(meta.tokens.clone())
                        .map_err(|_| "error_code requires a string literal".to_string())
                })
                .transpose()?
                .unwrap_or_else(|| LitStr::new(&name.to_string(), name.span()));
            let cfg = variant
                .attrs
                .iter()
                .filter(|attribute| attribute.path().is_ident("cfg"));
            Ok(quote! { #(#cfg)* Self::#name #pattern => #code, })
        })
        .collect::<Result<Vec<_>, String>>()?;

    let name = &parsed.ident;
    Ok(quote! {
        impl #name {
            #[doc = "Stable RPC error code; independent of Display text."]
            pub fn rpc_code(&self) -> &'static str {
                match self { #(#arms)* }
            }
        }
    })
}
