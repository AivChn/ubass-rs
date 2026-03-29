use core::panic;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    DataStruct, DeriveInput, Expr, Item, Type,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

struct SerializeAs {
    ty: syn::Ident,
}

impl SerializeAs {
    fn find(attrs: Vec<syn::Attribute>) -> Option<Self> {
        attrs
            .iter()
            .filter(|attr| attr.path().is_ident("repr"))
            .filter_map(|attr| attr.parse_args::<SerializeAs>().ok())
            .next()
    }
}

impl Parse for SerializeAs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut idents = vec![];
        while !input.is_empty() {
            idents.push(input.parse::<syn::Ident>());
        }

        let int_types = [
            "i8", "i16", "i32", "i64", "i128", "isize", "u8", "u16", "u32", "u64", "u128", "usize",
        ];

        let mut ty = None;
        for res in idents {
            let Ok(id) = res else {
                continue;
            };

            if int_types.iter().any(|i| i == &id.to_string().as_str()) {
                ty = Some(id);
            }
        }

        let ty = ty.unwrap();
        Ok(SerializeAs { ty })
    }
}

struct EnumRep {
    ident: syn::Ident,
    type_representation: syn::Ident,
    variants: Vec<(syn::Ident, Expr)>,
}

impl EnumRep {
    fn new(data: syn::DeriveInput) -> Option<Self> {
        let ident = data.ident;
        let SerializeAs {
            ty: type_representation,
        } = SerializeAs::find(data.attrs).expect("Failed to find repr type attribute");
        let syn::Data::Enum(enum_data) = data.data else {
            return None;
        };
        let variants = enum_variants(enum_data)?;

        Some(EnumRep {
            ident,
            type_representation,
            variants,
        })
    }
}

fn enum_variants(data: syn::DataEnum) -> Option<Vec<(syn::Ident, Expr)>> {
    let mut variants = vec![];

    let mut counter = 0;
    let mut last_expr = syn::parse2::<Expr>(quote! { 0 }).ok()?;
    for variant in data.variants {
        let ident = variant.ident;
        let value = match variant.discriminant {
            Some((_, expr)) => {
                counter = 1;
                last_expr = expr.clone();
                expr
            }
            None => syn::parse2(quote! { #last_expr + #counter }).ok()?,
        };

        variants.push((ident, value));
    }

    Some(variants)
}

#[derive(Clone)]
struct StructRep {
    ident: syn::Ident,
    fields: StructFields,
}

impl StructRep {
    fn new(data: syn::DeriveInput) -> Option<Self> {
        let ident = data.ident;
        let syn::Data::Struct(struct_data) = data.data else {
            return None;
        };
        let fields = StructFields::new(struct_data)?;

        Some(Self { ident, fields })
    }
}

#[derive(Clone)]
enum StructFields {
    Named {
        idents: Vec<syn::Ident>,
        types: Vec<syn::Type>,
    },
    UnNamed {
        types: Vec<Type>,
    },
}

impl StructFields {
    fn new(data: DataStruct) -> Option<Self> {
        match data.fields {
            syn::Fields::Named(fields) => {
                let mut idents = vec![];
                let mut types = vec![];

                for field in fields.named {
                    idents.push(field.ident?);
                    types.push(field.ty);
                }

                Some(Self::Named { idents, types })
            }
            syn::Fields::Unnamed(fields) => {
                let mut types = vec![];

                for field in fields.unnamed {
                    types.push(field.ty);
                }

                Some(Self::UnNamed { types })
            }
            syn::Fields::Unit => None,
        }
    }
}

fn enum_serialization(input: DeriveInput) -> TokenStream {
    let EnumRep {
        ident,
        type_representation,
        variants,
    } = EnumRep::new(input).unwrap();

    let if_statements: Vec<_> = variants
        .iter()
        .map(|(i, e)| quote! { if tmp == (#e as #type_representation) { Ok(Self::#i) } else })
        .collect();

    quote! {
        impl Serialize for #ident {
            fn serialize(&self, buf: &mut [u8]) -> EmptyResult {
                if buf.len() < self.sized() {
                    Err(())
                } else {
                    (*self as #type_representation).serialize(buf)
                }
            }

            fn deserialize(bytes: &[u8]) -> core::result::Result<Self, ()> {
                let tmp = <#type_representation>::deserialize(bytes)?;

                #(#if_statements)* {
                    Err(())
                }
            }

            #[inline]
            fn sized(&self) -> usize {
                std::mem::size_of::<#type_representation>()
            }
        }
    }
    .into()
}

fn struct_serialization(input: DeriveInput) -> TokenStream {
    let StructRep { ident, fields } = StructRep::new(input).expect("Could not parse Struct");

    let (struct_size, serialize_logic, deserialize_logic) = match fields.clone() {
        StructFields::Named { idents, types } => {
            let mut struct_size = quote! { 0 };
            let mut serialize_logic = vec![];
            let mut deserialize_logic = vec![];
            let mut fields = vec![];

            for (i, t) in idents.iter().zip(types) {
                fields.push(quote! {#i,});
                serialize_logic.push(quote! { self.#i.serialize(&mut buf[(#struct_size)..]); });
                deserialize_logic.push(
                    quote! { let #i =  <#t>::deserialize(&bytes[offset..])?; offset += #i.sized(); },
                );
                struct_size = quote! { #struct_size + self.#i.sized() };
            }

            let deserialize_logic = quote! {
                #(#deserialize_logic)*
                Ok(Self {
                    #(#fields)*
                })
            };

            (struct_size, serialize_logic, deserialize_logic)
        }
        StructFields::UnNamed { types } => {
            let mut struct_size = quote! { 0 };
            let mut serialize_logic = vec![];
            let mut deserialize_logic = vec![];
            let mut fields = vec![];

            for (i, t) in types.iter().enumerate() {
                let field = format_ident!("f{i}");
                let i = syn::Index::from(i);
                fields.push(quote! {#field, });
                serialize_logic.push(quote! { self.#i.serialize(&mut buf[(#struct_size)..])?; });
                deserialize_logic.push(
                    quote! { let #field = <#t>::deserialize(&bytes[offset..])?; offset += #field.sized(); },
                );
                struct_size = quote! { #struct_size + self.#i.sized() };
            }

            let deserialize_logic = quote! {
                #(#deserialize_logic)*
                Ok(Self(
                    #(#fields)*
                    ))
            };

            (struct_size, serialize_logic, deserialize_logic)
        }
    };

    quote! {
        impl Serialize for #ident {
            fn serialize(&self, buf: &mut [u8]) -> EmptyResult {
                if buf.len() < self.sized() {
                    Err(())
                } else {
                    #(#serialize_logic)*
                    Ok(())
                }
            }

            fn deserialize(bytes: &[u8]) -> core::result::Result<Self, ()> {
                let mut offset: usize = 0;
                #deserialize_logic
            }

            #[inline]
            fn sized(&self) -> usize {
                #struct_size
            }
        }
    }
    .into()
}

#[proc_macro_derive(Serialize)]
pub fn serialize_derive_macro(item: TokenStream) -> TokenStream {
    match syn::parse::<DeriveInput>(item) {
        Ok(input) => match input.data {
            syn::Data::Enum(_) => enum_serialization(input),
            syn::Data::Struct(_) => struct_serialization(input),
            syn::Data::Union(_) => panic!("Unions not supported"),
        },
        Err(e) => panic!("{}", e.to_string()),
    }
}

fn get_headers_impl(input: DeriveInput) -> TokenStream {
    let StructRep { ident, fields } = StructRep::new(input).expect("Failed to parse struct");
    let StructFields::Named { idents, types: _ } = fields else {
        panic!("This trait is meant for named structs only");
    };

    let mut struct_size = quote! { 0 };
    let mut serialize_logic = vec![];

    for i in idents {
        if i.to_string().as_str() == "payload" {
            continue;
        }
        serialize_logic.push(quote! { self.#i.serialize(&mut buf[(#struct_size)..]); });
        struct_size = quote! { #struct_size + self.#i.sized() };
    }

    quote! {
        impl Headers for #ident {
            fn headers(&self) -> Vec<u8> {
                let mut buf = vec![0u8; #struct_size];
                #(#serialize_logic)*

                buf
            }
        }
    }
    .into()
}

#[proc_macro_derive(Headers)]
pub fn headers_derive_macro(item: TokenStream) -> TokenStream {
    let input = syn::parse::<DeriveInput>(item)
        .map_err(|e| panic!("{}", e.to_string()))
        .unwrap();
    get_headers_impl(input)
}

#[proc_macro_attribute]
pub fn variants_array(_attrs: TokenStream, item: TokenStream) -> TokenStream {
    let mut item_copy = item.clone();
    let enum_item = parse_macro_input!(item as syn::Item);
    let Item::Enum(rep) = enum_item else {
        panic!("This attribute is for enums only");
    };

    let ident = rep.ident;
    let variants_size = rep.variants.len();
    let variants = rep.variants.into_iter().map(|e| {
        let e = e.ident;
        quote! {#ident::#e,}
    });

    item_copy.extend(TokenStream::from(quote! {
        impl #ident {
            pub const VARIANTS: [Self; #variants_size] = [#(#variants)*];
        }
    }));

    item_copy
}
