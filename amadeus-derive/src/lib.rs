// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![doc(html_root_url = "https://docs.rs/amadeus-derive/0.1.2")]
#![recursion_limit = "400"]
#![allow(clippy::useless_let_if_seq)]

extern crate proc_macro;
extern crate proc_macro2;
#[macro_use]
extern crate syn;
#[macro_use]
extern crate quote;

use proc_macro2::{Span, TokenStream};
use quote::ToTokens;
use syn::{
	punctuated::Punctuated, spanned::Spanned, Attribute, Data, DataEnum, DeriveInput, Error, Field, Fields, Ident, Lit, LitStr, Meta, NestedMeta, Path, TypeParam, WhereClause
};

/// This is a procedural macro to derive the [`Data`](amadeus::record::Data) trait on
/// structs and enums.
///
/// ## Example
///
/// ```text
/// use amadeus::record::Data;
///
/// #[derive(Data, Debug)]
/// struct MyRow {
///     id: u64,
///     time: Timestamp,
///     event: String,
/// }
/// ```
///
/// If the Rust field name and the Parquet field name differ, say if the latter is not an
/// idiomatic or valid identifier in Rust, then an automatic rename can be made like so:
///
/// ```text
/// #[derive(Data, Debug)]
/// struct MyRow {
///     #[amadeus(rename = "ID")]
///     id: u64,
///     time: Timestamp,
///     event: String,
/// }
/// ```
///
/// ## Implementation
///
/// This macro works by creating two new structs: StructSchema and StructReader
/// (where "Struct" is the name of the user's struct). These structs implement the
/// [`Schema`](amadeus::record::Schema) and [`Reader`](amadeus::record::Reader) traits
/// respectively. [`Data`](amadeus::record::Data) can then be implemented on the
/// user's struct.
#[proc_macro_derive(Data, attributes(amadeus))]
pub fn amadeus_data(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	syn::parse::<DeriveInput>(input)
		.and_then(|ast| match ast.data {
			Data::Struct(ref s) => match s.fields {
				Fields::Named(ref fields) => impl_struct(&ast, &fields.named),
				Fields::Unit => impl_struct(&ast, &Punctuated::new()),
				Fields::Unnamed(ref fields) => impl_tuple_struct(&ast, &fields.unnamed),
			},
			Data::Enum(ref e) => impl_enum(&ast, e),
			Data::Union(_) => Err(Error::new_spanned(
				ast,
				"#[derive(Data)] doesn't work with unions",
			)),
		})
		.unwrap_or_else(|err| err.to_compile_error())
		.into()
}

/// Implement on regular named or unit structs.
#[allow(clippy::cognitive_complexity)]
fn impl_struct(
	ast: &DeriveInput, fields: &Punctuated<Field, Token![,]>,
) -> Result<TokenStream, Error> {
	let name = &ast.ident;
	let visibility = &ast.vis;
	let serde_name = Ident::new(&format!("{}Serde", name), Span::call_site());
	let schema_name = Ident::new(&format!("{}Schema", name), Span::call_site());
	let reader_name = Ident::new(&format!("{}Reader", name), Span::call_site());

	let mut amadeus_path = None;

	for meta_items in ast.attrs.iter().filter_map(get_amadeus_meta_items) {
		for meta_item in meta_items {
			match meta_item {
				// Parse `#[amadeus(crate = "self")]`
				NestedMeta::Meta(Meta::NameValue(ref m)) if m.path.is_ident("crate") => {
					let crate_ = m.path.get_ident().unwrap();
					let s: Path = get_lit_str(crate_, crate_, &m.lit)?.parse()?;
					if amadeus_path.is_some() {
						return Err(Error::new_spanned(
							crate_,
							"duplicate amadeus attribute `crate`",
						));
					}
					amadeus_path = Some(s.clone());
				}
				NestedMeta::Meta(ref meta_item) => {
					let path = meta_item
						.path()
						.into_token_stream()
						.to_string()
						.replace(' ', "");
					return Err(Error::new_spanned(
						meta_item.path(),
						format!("unknown amadeus field attribute `{}`", path),
					));
				}
				NestedMeta::Lit(ref lit) => {
					return Err(Error::new_spanned(
						lit,
						"unexpected literal in amadeus field attribute",
					));
				}
			}
		}
	}

	let amadeus_path = amadeus_path.unwrap_or_else(|| syn::parse2(quote! { ::amadeus }).unwrap());

	let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

	let where_clause = where_clause
		.map(Clone::clone)
		.unwrap_or_else(|| WhereClause {
			where_token: <Token![where]>::default(),
			predicates: Punctuated::new(),
		});
	let mut where_clause_with_data = where_clause.clone();
	for TypeParam { ident, .. } in ast.generics.type_params() {
		where_clause_with_data
			.predicates
			.push(syn::parse2(quote! { #ident: __::Data }).unwrap());
	}
	let mut where_clause_with_parquet_data = where_clause.clone();
	for TypeParam { ident, .. } in ast.generics.type_params() {
		where_clause_with_parquet_data
			.predicates
			.push(syn::parse2(quote! { #ident: __::ParquetData }).unwrap());
	}
	let mut where_clause_with_postgres_data = where_clause.clone();
	for TypeParam { ident, .. } in ast.generics.type_params() {
		where_clause_with_postgres_data
			.predicates
			.push(syn::parse2(quote! { #ident: __::PostgresData }).unwrap());
	}
	let mut where_clause_with_serde_data = where_clause.clone();
	for TypeParam { ident, .. } in ast.generics.type_params() {
		where_clause_with_serde_data
			.predicates
			.push(syn::parse2(quote! { #ident: __::SerdeData }).unwrap());
	}
	let mut where_clause_with_parquet_data_debug = where_clause_with_parquet_data.clone();
	for TypeParam { ident, .. } in ast.generics.type_params() {
		where_clause_with_parquet_data_debug
			.predicates
			.push(syn::parse2(quote! { <#ident as __::ParquetData>::Schema: __::Debug }).unwrap());
	}
	let mut where_clause_with_data_default = where_clause_with_data.clone();
	for TypeParam { ident, .. } in ast.generics.type_params() {
		where_clause_with_data_default
			.predicates
			.push(syn::parse2(quote! { <#ident as __::Data>::Schema: __::Default }).unwrap());
	}

	// The struct field names
	let field_names = fields
		.iter()
		.map(|field| field.ident.as_ref().unwrap())
		.collect::<Vec<_>>();
	let field_names1 = &field_names;
	let field_names2 = &field_names;

	let num_fields = field_names.len();

	// The field names specified via `#[amadeus(rename = "foo")]`, falling back to struct
	// field names
	let field_renames = fields
		.iter()
		.map(|field| {
			let mut rename = None;
			for meta_items in field.attrs.iter().filter_map(get_amadeus_meta_items) {
				for meta_item in meta_items {
					match meta_item {
						// Parse `#[amadeus(name = "foo")]`
						NestedMeta::Meta(Meta::NameValue(ref m)) if m.path.is_ident("name") => {
							let name = m.path.get_ident().unwrap();
							let s = get_lit_str(name, name, &m.lit)?;
							if rename.is_some() {
								return Err(Error::new_spanned(
									name,
									"duplicate amadeus attribute `name`",
								));
							}
							rename = Some(s.clone());
						}
						NestedMeta::Meta(ref meta_item) => {
							let path = meta_item
								.path()
								.into_token_stream()
								.to_string()
								.replace(' ', "");
							return Err(Error::new_spanned(
								meta_item.path(),
								format!("unknown amadeus field attribute `{}`", path),
							));
						}
						NestedMeta::Lit(ref lit) => {
							return Err(Error::new_spanned(
								lit,
								"unexpected literal in amadeus field attribute",
							));
						}
					}
				}
			}
			Ok(rename.unwrap_or_else(|| {
				LitStr::new(&field.ident.as_ref().unwrap().to_string(), field.span())
			}))
		})
		.collect::<Result<Vec<_>, _>>()?;
	let field_renames1 = &field_renames;
	let field_renames2 = &field_renames;

	// The struct field types
	let field_types = fields.iter().map(|field| &field.ty).collect::<Vec<_>>();
	let field_types1 = &field_types;

	let name_str = LitStr::new(&name.to_string(), name.span());

	let mut parquet_includes = None;
	let mut parquet_derives = None;
	if cfg!(feature = "parquet") {
		parquet_includes = Some(quote! {
			pub use ::amadeus_parquet::derive::{
				ParquetData, Repetition, ColumnReader, ParquetError, ParquetResult, ParquetSchema, Reader, DisplaySchemaGroup, ColumnPath, Type
			};
		});

		parquet_derives = Some(quote! {
			#visibility struct #schema_name #impl_generics #where_clause_with_parquet_data {
				#(#field_names1: <#field_types1 as __::ParquetData>::Schema,)*
			}
			// #[automatically_derived]
			// impl #impl_generics __::Default for #schema_name #ty_generics #where_clause_with_data_default {
			// 	fn default() -> Self {
			// 		Self {
			// 			#(#field_names1: __::Default::default(),)*
			// 		}
			// 	}
			// }
			#[automatically_derived]
			impl #impl_generics __::Debug for #schema_name #ty_generics #where_clause_with_parquet_data_debug {
				fn fmt(&self, f: &mut __::fmt::Formatter) -> __::fmt::Result {
					f.debug_struct(stringify!(#schema_name))
						#(.field(stringify!(#field_names1), &self.#field_names2))*
						.finish()
				}
			}
			#[automatically_derived]
			impl #impl_generics __::ParquetSchema for #schema_name #ty_generics #where_clause_with_parquet_data {
				fn fmt(self_: __::Option<&Self>, r: __::Option<__::Repetition>, name: __::Option<&str>, f: &mut __::fmt::Formatter) -> __::fmt::Result {
					__::DisplaySchemaGroup::new(r, name, None, f)
					#(
						.field(__::Some(#field_renames1), self_.map(|self_|&self_.#field_names1))
					)*
						.finish()
				}
			}
			#visibility struct #reader_name #impl_generics #where_clause_with_parquet_data {
				#(#field_names1: <#field_types1 as __::ParquetData>::Reader,)*
			}
			#[automatically_derived]
			impl #impl_generics __::Reader for #reader_name #ty_generics #where_clause_with_parquet_data {
				type Item = #name #ty_generics;

				#[allow(unused_variables, non_snake_case)]
				fn read(&mut self, def_level: i16, rep_level: i16) -> __::ParquetResult<Self::Item> {
					#(
						let #field_names1 = self.#field_names2.read(def_level, rep_level);
					)*
					if #(#field_names1.is_err() ||)* false { // TODO: unlikely
						#(#field_names1?;)*
						unreachable!()
					}
					__::Ok(#name {
						#(#field_names1: #field_names2.unwrap(),)*
					})
				}
				fn advance_columns(&mut self) -> __::ParquetResult<()> {
					#[allow(unused_mut)]
					let mut res = __::Ok(());
					#(
						res = res.and(self.#field_names1.advance_columns());
					)*
					res
				}
				#[inline]
				fn has_next(&self) -> bool {
					#(if true { self.#field_names1.has_next() } else)*
					{
						true
					}
				}
				#[inline]
				fn current_def_level(&self) -> i16 {
					#(if true { self.#field_names1.current_def_level() } else)*
					{
						panic!("Current definition level: empty group reader")
					}
				}
				#[inline]
				fn current_rep_level(&self) -> i16 {
					#(if true { self.#field_names1.current_rep_level() } else)*
					{
						panic!("Current repetition level: empty group reader")
					}
				}
			}

			#[automatically_derived]
			impl #impl_generics __::ParquetData for #name #ty_generics #where_clause_with_parquet_data {
				type Schema = #schema_name #ty_generics;
				type Reader = #reader_name #ty_generics;

				fn parse(schema: &__::Type, repetition: __::Option<__::Repetition>) -> __::ParquetResult<(__::String, Self::Schema)> {
					if schema.is_group() && repetition == __::Some(__::Repetition::Required) {
						let fields = schema.get_fields().iter().map(|field|(field.name(),field)).collect::<__::HashMap<_,_>>();
						let name = stringify!(#name);
						let schema_ = #schema_name{
							#(#field_names1: fields.get(#field_renames1).ok_or_else(|| __::ParquetError::General(format!("Struct \"{}\" has field \"{}\" not in the schema", name, #field_renames2))).and_then(|x|<#field_types1 as __::ParquetData>::parse(&**x, __::Some(x.get_basic_info().repetition())))?.1,)*
						};
						return __::Ok((schema.name().to_owned(), schema_))
					}
					__::Err(__::ParquetError::General(format!("Struct \"{}\" is not in the schema", stringify!(#name))))
				}
				fn reader(schema: &Self::Schema, mut path: &mut __::Vec<__::String>, def_level: i16, rep_level: i16, paths: &mut __::HashMap<__::ColumnPath, __::ColumnReader>, batch_size: usize) -> Self::Reader {
					#(
						path.push(#field_renames1.to_owned());
						let #field_names1 = <#field_types1 as __::ParquetData>::reader(&schema.#field_names2, path, def_level, rep_level, paths, batch_size);
						path.pop().unwrap();
					)*
					#reader_name { #(#field_names1,)* }
				}
			}
		});
	}

	let mut postgres_includes = None;
	let mut postgres_derives = None;
	if cfg!(feature = "postgres") {
		postgres_includes = Some(quote! {
			pub use ::amadeus_postgres::{Names,read_be_i32,read_value,_internal as postgres,PostgresData};
		});
		postgres_derives = Some(quote! {
			#[automatically_derived]
			impl #impl_generics __::PostgresData for #name #ty_generics #where_clause_with_postgres_data {
				fn query(f: &mut __::fmt::Formatter, name: __::Option<&__::Names<'_>>) -> __::fmt::Result {
					if let __::Some(name) = name {
						__::Write::write_str(f, "CASE WHEN ")?;
						__::fmt::Display::fmt(name, f)?;
						__::Write::write_str(f, " IS NOT NULL THEN ROW(")?;
					} else {
						__::Write::write_str(f, "ROW(")?;
					}
					let mut comma = false;
					#(
						if comma { __::Write::write_str(f, ",")? } comma = true;
						<#field_types1 as __::PostgresData>::query(f, __::Some(&__::Names(name, #field_renames1)))?;
					)*
					if let __::Some(_name) = name {
						__::Write::write_str(f, ") ELSE NULL END")
					} else {
						__::Write::write_str(f, ")")
					}
				}
				fn decode(type_: &__::postgres::types::Type, buf: Option<&[u8]>) -> __::Result<Self, __::Box<__::Error + __::Sync + __::Send>> {
					let buf = buf.unwrap();
					assert_eq!(type_, &__::postgres::types::RECORD);

					let mut buf = buf;
					let num_fields = __::read_be_i32(&mut buf)?;
					if num_fields as usize != #num_fields {
						return __::Err(__::Into::into(format!("invalid field count: {} vs {}", num_fields, #num_fields)));
					}

					__::Ok(Self {
						#(
							#field_names1: {
								let oid = __::read_be_i32(&mut buf)? as u32;
								__::read_value(&__::postgres::types::Type::from_oid(oid).unwrap_or(__::postgres::types::OPAQUE), &mut buf)?
							},
						)*
					})
				}
			}
		});
	}

	let mut serde_includes = None;
	let mut serde_derives = None;
	if cfg!(feature = "serde") {
		serde_includes = Some(quote! {
			pub use ::amadeus_serde::{SerdeData,_internal::{Serialize, Deserialize, Serializer, Deserializer}};
			pub use #amadeus_path::data::serde_data;
		});
		serde_derives = Some(quote! {
			#[derive(__::Serialize, __::Deserialize)]
			#[serde(remote = #name_str)]
			#[serde(bound = "")]
			#visibility struct #serde_name #impl_generics #where_clause_with_serde_data {
				#(
					#[serde(with = "__::serde_data", rename = #field_renames1)]
					#field_names1: #field_types1,
				)*
			}

			#[automatically_derived]
			impl #impl_generics __::SerdeData for #name #ty_generics #where_clause_with_serde_data {
				fn serialize<__S>(&self, serializer: __S) -> __::Result<__S::Ok, __S::Error>
				where
					__S: __::Serializer {
					<#serde_name #ty_generics>::serialize(self, serializer)
				}
				fn deserialize<'de, __D>(deserializer: __D, schema: __::Option<__::SchemaIncomplete>) -> __::Result<Self, __D::Error>
				where
					__D: __::Deserializer<'de> {
					<#serde_name #ty_generics>::deserialize(deserializer)
				}
			}
		});
	}

	let gen = quote! {
		mod __ {
			#parquet_includes
			#postgres_includes
			#serde_includes
			pub use ::amadeus_types::{DowncastImpl, Downcast, DowncastError, Value, Group, SchemaIncomplete};
			pub use #amadeus_path::data::Data;
			pub use ::std::{boxed::Box, clone::Clone, collections::HashMap, convert::{From, Into}, cmp::PartialEq, default::Default, error::Error, fmt::{self, Debug, Write}, marker::{Send, Sync}, result::Result::{self, Ok, Err}, string::String, vec, vec::Vec, option::Option::{self, Some, None}, iter::Iterator};
		}

		#parquet_derives
		#postgres_derives
		#serde_derives

		#[automatically_derived]
		impl #impl_generics __::Data for #name #ty_generics #where_clause_with_data {}

		impl #impl_generics __::DowncastImpl<__::Value> for #name #ty_generics #where_clause_with_data {
			fn downcast_impl(t: __::Value) -> __::Result<Self, __::DowncastError> {
				let group = t.into_group()?;
				let field_names = group.field_names().map(__::Clone::clone);
				let mut fields = group.into_fields().into_iter();
				let err = __::DowncastError{from:"group",to:stringify!(#name)};
				__::Ok(if let Some(field_names) = field_names {
					let mut fields = fields.map(__::Some).collect::<__::Vec<_>>();
					#name {
						#(#field_names1: __::Downcast::downcast(fields[*field_names.get(#field_renames1).ok_or(err)?].take().ok_or(err)?)?,)*
					}
				} else {
					if fields.len() != #num_fields {
						return Err(err);
					}
					#name {
						#(#field_names1: __::Downcast::downcast(fields.next().unwrap())?,)*
					}
				})
			}
		}

		impl #impl_generics __::From<#name #ty_generics> for __::Value where #where_clause_with_data {
			fn from(value: #name #ty_generics) -> Self {
				__::Value::Group(__::Group::new(__::vec![
					#(__::Into::into(value.#field_names1),)*
				], __::None))
			}
		}
	};

	let gen = quote! {
		#[allow(non_upper_case_globals, unused_attributes, unused_qualifications, clippy::type_complexity, unknown_lints,clippy::useless_attribute,rust_2018_idioms)]
		const _: () = {
			#gen
		};
	};

	Ok(gen)
}

/// Implement on tuple structs.
fn impl_tuple_struct(
	ast: &DeriveInput, fields: &Punctuated<Field, Token![,]>,
) -> Result<TokenStream, Error> {
	let _name = &ast.ident;
	let _schema_name = Ident::new(&format!("{}Schema", _name), Span::call_site());
	let _reader_name = Ident::new(&format!("{}Reader", _name), Span::call_site());

	let (_impl_generics, _ty_generics, _where_clause) = ast.generics.split_for_impl();

	for field in fields.iter() {
		for meta_items in field.attrs.iter().filter_map(get_amadeus_meta_items) {
			#[allow(clippy::never_loop)]
			for meta_item in meta_items {
				match meta_item {
					NestedMeta::Meta(ref meta_item) => {
						let path = meta_item
							.path()
							.into_token_stream()
							.to_string()
							.replace(' ', "");
						return Err(Error::new_spanned(
							meta_item.path(),
							format!("unknown amadeus field attribute `{}`", path),
						));
					}
					NestedMeta::Lit(ref lit) => {
						return Err(Error::new_spanned(
							lit,
							"unexpected literal in amadeus field attribute",
						));
					}
				}
			}
		}
	}

	unimplemented!("#[derive(Data)] on tuple structs not yet implemented")
}

/// Implement on unit variant enums.
fn impl_enum(ast: &DeriveInput, data: &DataEnum) -> Result<TokenStream, Error> {
	if data.variants.is_empty() {
		return Err(Error::new_spanned(
			ast,
			"#[derive(Data)] cannot be implemented for enums with zero variants",
		));
	}
	for v in data.variants.iter() {
		if v.fields.iter().len() == 0 {
			return Err(Error::new_spanned(
				v,
				"#[derive(Data)] cannot be implemented for enums with non-unit variants",
			));
		}
	}

	unimplemented!("#[derive(Data)] on enums not yet implemented")
}

// The below code adapted from https://github.com/serde-rs/serde/tree/c8e39594357bdecb9dfee889dbdfced735033469/serde_derive/src

fn get_amadeus_meta_items(attr: &Attribute) -> Option<Vec<NestedMeta>> {
	if attr.path.is_ident("amadeus") {
		match attr.parse_meta() {
			Ok(Meta::List(ref meta)) => Some(meta.nested.iter().cloned().collect()),
			_ => {
				// TODO: produce an error
				None
			}
		}
	} else {
		None
	}
}

fn get_lit_str<'a>(
	attr_name: &Ident, meta_item_name: &Ident, lit: &'a Lit,
) -> Result<&'a LitStr, Error> {
	if let Lit::Str(ref lit) = *lit {
		Ok(lit)
	} else {
		Err(Error::new_spanned(
			lit,
			format!(
				"expected amadeus {} attribute to be a string: `{} = \"...\"`",
				attr_name, meta_item_name
			),
		))
	}
}
