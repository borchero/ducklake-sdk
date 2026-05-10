use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{ItemFn, Token, Visibility, parse_macro_input};

/// Arguments for the `visibility_if` macro.
///
/// Parses: `<cfg_expr>, <visibility>`
struct VisibilityIfArgs {
    cfg_expr: TokenStream2,
    visibility: Visibility,
}

impl Parse for VisibilityIfArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        // Parse everything up to the last comma as the cfg expression
        let mut cfg_tokens = Vec::new();

        loop {
            if input.peek(Token![,]) {
                // Check if this is the last comma by peeking ahead
                let fork = input.fork();
                fork.parse::<Token![,]>()?;

                // Try to parse visibility - if it succeeds with no remaining input,
                // this is the separator comma
                if fork.peek(Token![pub]) || fork.peek(Token![crate]) || fork.is_empty() {
                    break;
                }
            }

            if input.is_empty() {
                return Err(syn::Error::new(
                    input.span(),
                    "expected `, <visibility>` after cfg expression",
                ));
            }

            // Track parenthesis depth
            if input.peek(syn::token::Paren) {
                let content;
                let paren = syn::parenthesized!(content in input);
                let inner: TokenStream2 = content.parse()?;
                cfg_tokens.push(quote::quote_spanned!(paren.span.join()=> (#inner)));
            } else {
                let token: proc_macro2::TokenTree = input.parse()?;
                cfg_tokens.push(token.into());
            }
        }

        // Parse the comma separator
        input.parse::<Token![,]>()?;

        // Parse the visibility
        let visibility: Visibility = input.parse()?;

        let cfg_expr = cfg_tokens.into_iter().collect();

        Ok(VisibilityIfArgs {
            cfg_expr,
            visibility,
        })
    }
}

pub fn visibility_if_impl(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as VisibilityIfArgs);
    let func = parse_macro_input!(input as ItemFn);

    let cfg_expr = &args.cfg_expr;
    let feature_visibility = &args.visibility;
    let original_visibility = &func.vis;

    // Store the original function components
    let attrs = &func.attrs;
    let sig = &func.sig;
    let block = &func.block;

    // Generate code that uses different visibility based on the cfg expression
    let output = quote! {
        #[cfg(#cfg_expr)]
        #(#attrs)*
        #feature_visibility #sig #block

        #[cfg(not(#cfg_expr))]
        #(#attrs)*
        #original_visibility #sig #block
    };

    output.into()
}
