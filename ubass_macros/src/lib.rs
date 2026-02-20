use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    DataStruct, DeriveInput, Expr, Type, Variant,
    parse::{Parse, ParseStream},
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
            _ = input.parse::<syn::Ident>();
            _ = input.parse::<syn::Expr>();
            _ = input.parse::<syn::Token![,]>()?;
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
    variants: EnumVars,
}

impl EnumRep {
    fn new(data: syn::DeriveInput) -> Option<Self> {
        let ident = data.ident;
        let SerializeAs {
            ty: type_representation,
        } = SerializeAs::find(data.attrs)?;
        let syn::Data::Enum(enum_data) = data.data else {
            return None;
        };
        let variants: EnumVars = EnumVars::new(enum_data)?;

        Some(EnumRep {
            ident,
            type_representation,
            variants,
        })
    }
}

struct EnumVars {
    idents: Vec<syn::Ident>,
    values: Vec<Expr>,
}

impl EnumVars {
    fn new(data: syn::DataEnum) -> Option<Self> {
        let mut idents = vec![];
        let mut values = vec![];

        let mut counter = 0;
        let mut last_expr = syn::parse2::<Expr>(quote! { 0 }).ok()?;
        for variant in data.variants {
            idents.push(variant.ident);
            values.push(match variant.discriminant {
                Some((_, expr)) => {
                    counter = 1;
                    last_expr = expr.clone();
                    expr
                }
                None => syn::parse2(quote! { #last_expr + #counter }).ok()?,
            });
        }

        Some(EnumVars { idents, values })
    }
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
        types: Vec<Type>,
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

fn enum_ser(input: DeriveInput) -> TokenStream {
    let EnumRep {
        ident,
        type_representation,
        variants
    } = EnumRep::new(input).expect("Could not parse Enum");

    let serialize_impl = quote! {
        impl PacketSerialize for #ident {
            fn serialize(self) -> Vec<u8> {
                vec![self as #type_representation]
            }
        }
    }

    quote! {}.into()
}

fn struct_ser(input: DeriveInput) -> TokenStream {
    let data = StructRep::new(input).expect("Could not parse Struct");

    quote! {}.into()
}

fn enum_serialization(input: DeriveInput) -> TokenStream {
    let mut ty: Option<syn::Ident> = None;
    for attr in input.attrs {
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

    let variants: Vec<(syn::Ident, Expr)> = match input.data {
        syn::Data::Enum(data) => {
            let mut counter = 1;
            let mut last_expr: Expr = syn::parse2::<Expr>(quote! { 0 }).unwrap();
            let mut v = vec![];
            for variant in data.variants {
                let ident = variant.ident;
                let disc = match variant.discriminant {
                    Some((_, expr)) => {
                        counter = 1;
                        last_expr = expr.clone();
                        expr
                    }
                    None => {
                        let x = counter;
                        counter += 1;
                        syn::parse2::<syn::Expr>(quote! {#last_expr + #x}).unwrap()
                    }
                };
                v.push((ident, disc));
            }
            v
        }
        _ => unreachable!(),
    };

    let ty = ty.expect("Just checked if it is none");
    let id = input.ident;

    return quote! {
        impl PacketSerialize for #id {
            fn serialize(self) -> Vec<u8> {
                (self as #ty).serialize()
            }

            fn sized(&self) -> usize {
                std::mem::size_of::<#ty>()
            }
        }

        impl PacketDeserialize for #id {
            fn deserialize(bytes: &[u8]) -> Option<Self> {

            }
        }
    }
    .into();
}

fn struct_serialization(input: DeriveInput) -> TokenStream {
    let ident = input.ident;
    if let syn::Data::Struct(struct_data) = input.data {
        match struct_data.fields {
            syn::Fields::Named(fields) => {
                let mut id: Vec<syn::Ident> = vec![];
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
                let mut v = vec![];
                for field in fields.unnamed {
                    v.push(field);
                }

                quote! {}.into()
            }
            syn::Fields::Unit => panic!("Unit structs not supported"),
        };
        TokenStream::new();
    }

    panic!("Somehow a non struct made it to the struct serialize function");
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
