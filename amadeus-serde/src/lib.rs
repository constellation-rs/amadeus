//! Harmonious distributed data processing & analysis in Rust.
//!
//! <p style="font-family: 'Fira Sans',sans-serif;padding:0.3em 0"><strong>
//! <a href="https://crates.io/crates/amadeus">📦&nbsp;&nbsp;Crates.io</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://github.com/constellation-rs/amadeus">📑&nbsp;&nbsp;GitHub</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://constellation.zulipchat.com/#narrow/stream/213231-amadeus">💬&nbsp;&nbsp;Chat</a>
//! </strong></p>
//!
//! This is a support crate of [Amadeus](https://github.com/constellation-rs/amadeus) and is not intended to be used directly. These types are re-exposed in [`amadeus::source`](https://docs.rs/amadeus/0.3/amadeus/source/index.html).

#![doc(html_root_url = "https://docs.rs/amadeus-serde/0.3.7")]
#![cfg_attr(nightly, feature(type_alias_impl_trait))]
#![warn(
	// missing_copy_implementations,
	// missing_debug_implementations,
	// missing_docs,
	trivial_numeric_casts,
	unused_import_braces,
	unused_qualifications,
	unused_results,
	unreachable_pub,
	clippy::pedantic,
)]
#![allow(
	clippy::module_name_repetitions,
	clippy::similar_names,
	clippy::if_not_else,
	clippy::must_use_candidate,
	clippy::missing_errors_doc,
	clippy::needless_pass_by_value,
	clippy::default_trait_access
)]
#![deny(unsafe_code)]

mod csv;
mod impls;
mod json;

#[doc(hidden)]
pub use serde as _internal;

use amadeus_types::SchemaIncomplete;
use serde::{Deserializer, Serializer};
use std::fmt::Debug;

pub use self::{
	csv::*, json::{Json, JsonError}
};

pub trait SerdeData
where
	Self: Clone + PartialEq + Debug + 'static,
{
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer;
	fn deserialize<'de, D>(
		deserializer: D, schema: Option<SchemaIncomplete>,
	) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>;
}

impl<T> SerdeData for Box<T>
where
	T: SerdeData,
{
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		(**self).serialize(serializer)
	}
	fn deserialize<'de, D>(
		deserializer: D, schema: Option<SchemaIncomplete>,
	) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		T::deserialize(deserializer, schema).map(Box::new)
	}
}

#[repr(transparent)]
pub struct SerdeSerialize<'a, T: SerdeData>(pub &'a T);
impl<'a, T> serde::Serialize for SerdeSerialize<'a, T>
where
	T: SerdeData,
{
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		self.0.serialize(serializer)
	}
}

#[repr(transparent)]
pub struct SerdeDeserialize<T: SerdeData>(pub T);
impl<'de, T> serde::Deserialize<'de> for SerdeDeserialize<T>
where
	T: SerdeData,
{
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		T::deserialize(deserializer, None).map(Self)
	}
}
#[repr(transparent)]
pub struct SerdeDeserializeGroup<T: SerdeData>(pub T);
impl<'de, T> serde::Deserialize<'de> for SerdeDeserializeGroup<T>
where
	T: SerdeData,
{
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		T::deserialize(deserializer, Some(SchemaIncomplete::Group(None))).map(Self)
	}
}
