use proc_macro::TokenStream;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Attribute, DataStruct, DeriveInput, Field, Fields, Ident, Type,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

struct SerializeAs {
    ty: Type,
}

impl Parse for SerializeAs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        _ = input.lookahead1();
        //if input.peek(syn::Ident) {
        //    let id: syn::Ident = input.parse()?;
        //    if id != "as" {
        //        return Err(input.error("expected `as`"));
        //    }
        //}

        //input.parse::<syn::Token![=]>()?;

        let ty: syn::Type = input.parse()?;
        let syn::Type::Path(p) = &ty else {
            return Err(input.error("Only integer types allowed"));
        };

        let int_types = [
            "i8", "i16", "i32", "i64", "i128", "isize", "u8", "u16", "u32", "u64", "u128", "usize",
        ];

        if !int_types.iter().any(|e| p.path.is_ident(e)) {
            return Err(input.error("Only integer types allowed"));
        }

        Ok(SerializeAs { ty })
    }
}

fn enum_serialization(input: DeriveInput) -> TokenStream {
    let mut ty: Option<Type> = None;
    for attr in input.attrs {
        let attr: Attribute = attr;
        if !attr.path().is_ident("repr") {
            continue;
        }

        match attr.parse_args::<SerializeAs>() {
            Ok(ser) => {
                ty = Some(ser.ty);
                break;
            }
            Err(e) => panic!("{}", e.to_string()),
        }
    }

    if ty.is_none() {
        panic!("No serialize attribute found");
    };

    let ty = ty.expect("Just checked if it is none");
    let id = input.ident;

    return quote! {
        impl PacketSerialize for #id {
            fn serialize(self) -> Vec<u8> {
                (self as #ty).serialize()
            }
        }
    }
    .into();
}

struct NamedFields {
    id: Vec<Ident>,
    ty: Vec<Type>,
}

struct UnnamedField {
    ty: Type,
}

fn struct_serialization(input: DeriveInput) -> TokenStream {
    let ident = input.ident;
    if let syn::Data::Struct(struct_data) = input.data {
        match struct_data.fields {
            syn::Fields::Named(fields) => {
                let mut id: Vec<Ident> = vec![];
                let mut ty: Vec<Type> = vec![];
                for field in fields.named {
                    id.push(field.ident.unwrap());
                    ty.push(field.ty);
                }

                let size_expr = ty
                    .iter()
                    .map(|t| {
                        quote! { #t.sized() }
                    })
                    .reduce(|acc, expr| {
                        quote! {#acc + #expr}
                    })
                    .unwrap();

                let serialize_fields = id.iter().map(|i| {
                    quote! { tmp.extend(self.#i.serialize()); }
                });

                quote! {
                    impl PacketSerialize for #ident {
                        fn serialize(self) -> Vec<u8> {
                            let mut tmp = Vec::with_capacity(
                                    #size_expr
                                );
                            #(#serialize_fields)*
                            tmp
                        }

                        fn sized(&self) -> usize {
                            #size_expr
                        }
                    }

                    impl PacketDeserialize for #ident {
                        fn deserialize(bytes: &[u8]) -> Option<Self> {
                            // TODO: finish this
                        }
                    }
                }
            }
            syn::Fields::Unnamed(fields) => {
                let mut v: Vec<UnnamedField> = vec![];
                for field in fields.unnamed {
                    v.push(UnnamedField { ty: field.ty });
                }

                quote! {}.into()
            }
            syn::Fields::Unit => panic!("Unit structs not supported"),
        };

        return quote! {}.into();
    }

    panic!("Somehow a non struct made it to the struct serialize function");
    quote! {}.into()
}

#[proc_macro_derive(PacketSerialize)]
pub fn serialize_derive_macro(item: TokenStream) -> TokenStream {
    match syn::parse::<DeriveInput>(item) {
        Ok(input) => {
            let input: DeriveInput = input;
            match input.data {
                syn::Data::Enum(_) => enum_serialization(input),
                syn::Data::Struct(_) => struct_serialization(input),
                syn::Data::Union(_) => panic!("Unions not supported"),
            }
        }
        Err(e) => panic!("{}", e.to_string()),
    }
}
