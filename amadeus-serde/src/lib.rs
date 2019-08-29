#![feature(specialization)]

mod csv;
mod impls;
mod json;

pub use serde as _internal;

use amadeus_types::SchemaIncomplete;
use serde::{Deserializer, Serializer};
use std::fmt::Debug;

pub use self::{csv::*, json::*};

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
	default fn deserialize<'de, D>(
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
