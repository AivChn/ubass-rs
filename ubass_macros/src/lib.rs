use core::panic;

use proc_macro::TokenStream;
use quote::quote;
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

struct ReprC(bool);

impl Parse for ReprC {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut v = vec![];
        while !input.is_empty() {
            v.push(input.parse::<syn::Ident>()?);
            _ = input.parse::<syn::Token![,]>();
        }

        Ok(Self(v.iter().any(|e| "C" == e.to_string().as_str())))
    }
}

fn is_repr_c_found(attrs: &Vec<syn::Attribute>) -> bool {
    attrs.iter().any(|e| {
        if e.path().is_ident("repr") {
            e.parse_args::<ReprC>().expect("Failed to find idents").0
        } else {
            false
        }
    })
}

fn enum_serialization(input: DeriveInput) -> TokenStream {
    let EnumRep {
        ident,
        type_representation,
        variants: _,
    } = EnumRep::new(input).unwrap();

    quote! {
        impl PacketSerialize for #ident {
            fn serialize(&self, buf: &mut [u8]) -> bool {
                if buf.len() < self.sized() {
                    false
                } else {
                    (*self as #type_representation).serialize(buf)
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
    let attrs = &input.attrs.clone();
    let StructRep { ident, fields } = StructRep::new(input).expect("Could not parse Struct");

    let (struct_size, serialize_logic) = match fields {
        StructFields::Named { idents, types: _ } => {
            let mut struct_size = quote! { 0 };
            let mut serialize_logic = vec![];

            if !is_repr_c_found(attrs) {
                panic!("Struct must be repr(C) to be serialized");
            }

            for i in idents {
                serialize_logic.push(quote! { self.#i.serialize(&mut buf[(#struct_size)..]); });
                struct_size = quote! { #struct_size + self.#i.sized() };
            }

            (struct_size, serialize_logic)
        }
        StructFields::UnNamed { types } => {
            let mut struct_size = quote! { 0 };
            let mut serialize_logic = vec![];

            for i in 0..types.len() {
                let index = syn::Index::from(i);
                serialize_logic.push(quote! { self.#index.serialize(&mut buf[(#struct_size)..]); });
                struct_size = quote! { #struct_size + self.#index.sized() };
            }

            (struct_size, serialize_logic)
        }
    };

    quote! {
        impl PacketSerialize for #ident {
            fn serialize(&self, buf: &mut [u8]) -> bool {
                if buf.len() < self.sized() {
                    false
                } else {
                    #(#serialize_logic)*
                    true
                }
            }

            #[inline]
            fn sized(&self) -> usize {
                #struct_size
            }
        }
    }
    .into()
}

#[proc_macro_derive(PacketSerialize)]
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

fn enum_deserialization(input: DeriveInput) -> TokenStream {
    let EnumRep {
        ident,
        type_representation,
        variants,
    } = EnumRep::new(input).expect("Could not parse Enum");

    let if_statements: Vec<_> = variants
        .iter()
        .map(|(i, e)| quote! { if tmp == (#e as #type_representation) { Some(Self::#i) } else })
        .collect();

    quote! {
        impl PacketDeserialize for #ident {
            fn deserialize(bytes: &[u8]) -> Option<Self> {
                let tmp = <#type_representation>::deserialize(bytes)?;

                #(#if_statements)* {
                    None
                }
            }
        }
    }
    .into()
}

fn struct_derserialization(input: DeriveInput) -> TokenStream {
    let StructRep { ident, fields } = StructRep::new(input).expect("Could not parse Struct");

    // #ident: #value,

    let return_stmt = match fields {
        StructFields::Named { idents, types } => {
            let mut size_acc = quote! { 0 };
            let mut res = vec![];

            for (i, t) in std::iter::zip(idents, types) {
                size_acc = quote! { #size_acc + std::mem::size_of::<#t>() };
                res.push(quote! { #i: <#t>::deserialize(&bytes[(#size_acc)..])?, });
            }

            quote! {
                Self {
                    #(#res)*
                }
            }
        }
        StructFields::UnNamed { types } => {
            let mut size_acc = quote! { 0 };
            let mut res = vec![];

            for t in types {
                size_acc = quote! { #size_acc + std::mem::size_of::<#t>() };
                res.push(quote! { <#t>::deserialize(&bytes[(#size_acc)..])?, });
            }

            quote! {
                Self(
                    #(#res)*
                )
            }
        }
    };

    quote! {
        impl PacketDeserialize for #ident {
            fn deserialize(bytes: &[u8]) -> Option<Self> {
                if bytes.len() < std::mem::size_of::<Self>() {
                    None
                } else {
                    Some(#return_stmt)
                }
            }
        }
    }
    .into()
}

#[proc_macro_derive(PacketDeserialize)]
pub fn deserialize_derive_macro(item: TokenStream) -> TokenStream {
    match syn::parse::<DeriveInput>(item) {
        Ok(input) => match input.data {
            syn::Data::Struct(_) => struct_derserialization(input),
            syn::Data::Enum(_) => enum_deserialization(input),
            syn::Data::Union(_) => panic!("unions are not supported"),
        },
        Err(e) => panic!("{}", e.to_string()),
    }
}

fn get_fingerprint_impl(input: DeriveInput) -> TokenStream {
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
        impl Fingerprint for #ident {
            fn fingerprint(&self) -> [u8; 16] {
                let mut buf = vec![0u8; #struct_size];
                #(#serialize_logic)*

                xxhash_rust::xxh3::xxh3_128(&buf).to_be_bytes()
            }
        }
    }
    .into()
}

#[proc_macro_derive(Fingerprint)]
pub fn fingerprint_derive_macro(item: TokenStream) -> TokenStream {
    let input = syn::parse::<DeriveInput>(item)
        .map_err(|e| panic!("{}", e.to_string()))
        .unwrap();
    get_fingerprint_impl(input)
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
