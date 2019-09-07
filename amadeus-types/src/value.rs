//! Implement [`Record`] for [`Value`] – an enum representing any valid Parquet value.

#![allow(clippy::type_complexity)]

use fxhash::FxBuildHasher;
use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};
use std::{
	cmp::Ordering, collections::HashMap, convert::TryInto, hash::{Hash, Hasher}, sync::Arc
};

use super::{
	Bson, Date, Decimal, Downcast, DowncastError, DowncastImpl, Enum, Group, Json, List, Map, Time, Timestamp, ValueRequired
};

#[derive(Clone, PartialEq, Debug)]
pub enum SchemaIncomplete {
	Bool,
	U8,
	I8,
	U16,
	I16,
	U32,
	I32,
	U64,
	I64,
	F32,
	F64,
	Date,
	Time,
	Timestamp,
	Decimal,
	ByteArray,
	Bson,
	String,
	Json,
	Enum,
	List(Box<SchemaIncomplete>),
	Map(Box<(SchemaIncomplete, SchemaIncomplete)>),
	Group(
		Option<(
			Vec<SchemaIncomplete>,
			Option<Arc<LinkedHashMap<String, usize, FxBuildHasher>>>,
		)>,
	),
	Option(Box<SchemaIncomplete>),
}

#[derive(Clone, PartialEq, Debug)]
pub enum Schema {
	Bool,
	U8,
	I8,
	U16,
	I16,
	U32,
	I32,
	U64,
	I64,
	F32,
	F64,
	Date,
	Time,
	Timestamp,
	Decimal,
	ByteArray,
	Bson,
	String,
	Json,
	Enum,
	List(Box<Schema>),
	Map(Box<(Schema, Schema)>),
	Group(
		Vec<Schema>,
		Option<Arc<LinkedHashMap<String, usize, FxBuildHasher>>>,
	),
	Option(Box<Schema>),
}

/// Represents any valid Parquet value.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub enum Value {
	// Primitive types
	/// Boolean value (`true`, `false`).
	Bool(bool),
	/// Signed integer INT_8.
	U8(u8),
	/// Signed integer INT_16.
	I8(i8),
	/// Signed integer INT_32.
	U16(u16),
	/// Signed integer INT_64.
	I16(i16),
	/// Unsigned integer UINT_8.
	U32(u32),
	/// Unsigned integer UINT_16.
	I32(i32),
	/// Unsigned integer UINT_32.
	U64(u64),
	/// Unsigned integer UINT_64.
	I64(i64),
	/// IEEE 32-bit floating point value.
	F32(f32),
	/// IEEE 64-bit floating point value.
	F64(f64),
	/// Date without a time of day, stores the number of days from the Unix epoch, 1
	/// January 1970.
	Date(Date),
	/// Time of day, stores the number of microseconds from midnight.
	Time(Time),
	/// Milliseconds from the Unix epoch, 1 January 1970.
	Timestamp(Timestamp),
	/// Decimal value.
	Decimal(Decimal),
	/// General binary value.
	ByteArray(Vec<u8>),
	/// BSON binary value.
	Bson(Bson),
	/// UTF-8 encoded character string.
	String(String),
	/// JSON string.
	Json(Json),
	/// Enum string.
	Enum(Enum),

	// Complex types
	/// List of elements.
	List(List<Value>),
	/// Map of key-value pairs.
	Map(Map<Value, Value>),
	/// Struct, child elements are tuples of field-value pairs.
	Group(Group),
	/// Optional element.
	Option(#[serde(with = "optional_value")] Option<ValueRequired>),
}

mod optional_value {
	use super::{Value, ValueRequired};
	use serde::{Deserialize, Deserializer, Serializer};

	pub fn serialize<S>(t: &Option<ValueRequired>, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		match t {
			Some(value) => match value {
				ValueRequired::Bool(value) => serializer.serialize_some(&value),
				ValueRequired::U8(value) => serializer.serialize_some(&value),
				ValueRequired::I8(value) => serializer.serialize_some(&value),
				ValueRequired::U16(value) => serializer.serialize_some(&value),
				ValueRequired::I16(value) => serializer.serialize_some(&value),
				ValueRequired::U32(value) => serializer.serialize_some(&value),
				ValueRequired::I32(value) => serializer.serialize_some(&value),
				ValueRequired::U64(value) => serializer.serialize_some(&value),
				ValueRequired::I64(value) => serializer.serialize_some(&value),
				ValueRequired::F32(value) => serializer.serialize_some(&value),
				ValueRequired::F64(value) => serializer.serialize_some(&value),
				ValueRequired::Date(value) => serializer.serialize_some(&value),
				ValueRequired::Time(value) => serializer.serialize_some(&value),
				ValueRequired::Timestamp(value) => serializer.serialize_some(&value),
				ValueRequired::Decimal(value) => serializer.serialize_some(&value),
				ValueRequired::ByteArray(value) => serializer.serialize_some(&value),
				ValueRequired::Bson(value) => serializer.serialize_some(&value),
				ValueRequired::String(value) => serializer.serialize_some(&value),
				ValueRequired::Json(value) => serializer.serialize_some(&value),
				ValueRequired::Enum(value) => serializer.serialize_some(&value),
				ValueRequired::List(value) => serializer.serialize_some(&value),
				ValueRequired::Map(value) => serializer.serialize_some(&value),
				ValueRequired::Group(value) => serializer.serialize_some(&value),
			},
			None => serializer.serialize_none(),
		}
	}
	pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<ValueRequired>, D::Error>
	where
		D: Deserializer<'de>,
	{
		Option::<Value>::deserialize(deserializer).map(|x| x.map(Into::into).unwrap())
	}
}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for Value {
	fn hash<H: Hasher>(&self, state: &mut H) {
		match self {
			Self::Bool(value) => {
				0u8.hash(state);
				value.hash(state);
			}
			Self::U8(value) => {
				1u8.hash(state);
				value.hash(state);
			}
			Self::I8(value) => {
				2u8.hash(state);
				value.hash(state);
			}
			Self::U16(value) => {
				3u8.hash(state);
				value.hash(state);
			}
			Self::I16(value) => {
				4u8.hash(state);
				value.hash(state);
			}
			Self::U32(value) => {
				5u8.hash(state);
				value.hash(state);
			}
			Self::I32(value) => {
				6u8.hash(state);
				value.hash(state);
			}
			Self::U64(value) => {
				7u8.hash(state);
				value.hash(state);
			}
			Self::I64(value) => {
				8u8.hash(state);
				value.hash(state);
			}
			Self::F32(_value) => {
				9u8.hash(state);
			}
			Self::F64(_value) => {
				10u8.hash(state);
			}
			Self::Date(value) => {
				11u8.hash(state);
				value.hash(state);
			}
			Self::Time(value) => {
				12u8.hash(state);
				value.hash(state);
			}
			Self::Timestamp(value) => {
				13u8.hash(state);
				value.hash(state);
			}
			Self::Decimal(_value) => {
				14u8.hash(state);
			}
			Self::ByteArray(value) => {
				15u8.hash(state);
				value.hash(state);
			}
			Self::Bson(value) => {
				16u8.hash(state);
				value.hash(state);
			}
			Self::String(value) => {
				17u8.hash(state);
				value.hash(state);
			}
			Self::Json(value) => {
				18u8.hash(state);
				value.hash(state);
			}
			Self::Enum(value) => {
				19u8.hash(state);
				value.hash(state);
			}
			Self::List(value) => {
				20u8.hash(state);
				value.hash(state);
			}
			Self::Map(_value) => {
				21u8.hash(state);
			}
			Self::Group(_value) => {
				22u8.hash(state);
			}
			Self::Option(value) => {
				23u8.hash(state);
				value.hash(state);
			}
		}
	}
}
impl Eq for Value {}
impl PartialOrd for Value {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		match (self, other) {
			(Self::Bool(a), Self::Bool(b)) => a.partial_cmp(b),
			(Self::U8(a), Self::U8(b)) => a.partial_cmp(b),
			(Self::I8(a), Self::I8(b)) => a.partial_cmp(b),
			(Self::U16(a), Self::U16(b)) => a.partial_cmp(b),
			(Self::I16(a), Self::I16(b)) => a.partial_cmp(b),
			(Self::U32(a), Self::U32(b)) => a.partial_cmp(b),
			(Self::I32(a), Self::I32(b)) => a.partial_cmp(b),
			(Self::U64(a), Self::U64(b)) => a.partial_cmp(b),
			(Self::I64(a), Self::I64(b)) => a.partial_cmp(b),
			(Self::F32(a), Self::F32(b)) => a.partial_cmp(b),
			(Self::F64(a), Self::F64(b)) => a.partial_cmp(b),
			(Self::Date(a), Self::Date(b)) => a.partial_cmp(b),
			(Self::Time(a), Self::Time(b)) => a.partial_cmp(b),
			(Self::Timestamp(a), Self::Timestamp(b)) => a.partial_cmp(b),
			(Self::Decimal(a), Self::Decimal(b)) => a.partial_cmp(b),
			(Self::ByteArray(a), Self::ByteArray(b)) => a.partial_cmp(b),
			(Self::Bson(a), Self::Bson(b)) => a.partial_cmp(b),
			(Self::String(a), Self::String(b)) => a.partial_cmp(b),
			(Self::Json(a), Self::Json(b)) => a.partial_cmp(b),
			(Self::Enum(a), Self::Enum(b)) => a.partial_cmp(b),
			(Self::List(a), Self::List(b)) => a.partial_cmp(b),
			(Self::Map(_a), Self::Map(_b)) => None, // TODO?
			(Self::Group(a), Self::Group(b)) => a.partial_cmp(b),
			(Self::Option(a), Self::Option(b)) => a.partial_cmp(b),
			_ => None,
		}
	}
}

impl Value {
	fn type_name(&self) -> &'static str {
		match self {
			Self::Bool(_value) => "bool",
			Self::U8(_value) => "u8",
			Self::I8(_value) => "i8",
			Self::U16(_value) => "u16",
			Self::I16(_value) => "i16",
			Self::U32(_value) => "u32",
			Self::I32(_value) => "i32",
			Self::U64(_value) => "u64",
			Self::I64(_value) => "i64",
			Self::F32(_value) => "f32",
			Self::F64(_value) => "f64",
			Self::Date(_value) => "date",
			Self::Time(_value) => "time",
			Self::Timestamp(_value) => "timestamp",
			Self::Decimal(_value) => "decimal",
			Self::ByteArray(_value) => "byte_array",
			Self::Bson(_value) => "bson",
			Self::String(_value) => "string",
			Self::Json(_value) => "json",
			Self::Enum(_value) => "enum",
			Self::List(_value) => "list",
			Self::Map(_value) => "map",
			Self::Group(_value) => "group",
			Self::Option(_value) => "option",
		}
	}

	/// Returns true if the `Value` is an Bool. Returns false otherwise.
	pub fn is_bool(&self) -> bool {
		if let Self::Bool(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Bool, return a reference to it. Returns Err otherwise.
	pub fn as_bool(&self) -> Result<bool, DowncastError> {
		if let Self::Bool(ret) = self {
			Ok(*ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "bool",
			})
		}
	}

	/// If the `Value` is an Bool, return it. Returns Err otherwise.
	pub fn into_bool(self) -> Result<bool, DowncastError> {
		if let Self::Bool(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "bool",
			})
		}
	}

	/// Returns true if the `Value` is an U8. Returns false otherwise.
	pub fn is_u8(&self) -> bool {
		if let Self::U8(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an U8, return a reference to it. Returns Err otherwise.
	pub fn as_u8(&self) -> Result<u8, DowncastError> {
		if let Self::U8(ret) = self {
			Ok(*ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "u8",
			})
		}
	}

	/// If the `Value` is an U8, return it. Returns Err otherwise.
	pub fn into_u8(self) -> Result<u8, DowncastError> {
		if let Self::U8(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "u8",
			})
		}
	}

	/// Returns true if the `Value` is an I8. Returns false otherwise.
	pub fn is_i8(&self) -> bool {
		if let Self::I8(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an I8, return a reference to it. Returns Err otherwise.
	pub fn as_i8(&self) -> Result<i8, DowncastError> {
		if let Self::I8(ret) = self {
			Ok(*ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "i8",
			})
		}
	}

	/// If the `Value` is an I8, return it. Returns Err otherwise.
	pub fn into_i8(self) -> Result<i8, DowncastError> {
		if let Self::I8(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "i8",
			})
		}
	}

	/// Returns true if the `Value` is an U16. Returns false otherwise.
	pub fn is_u16(&self) -> bool {
		if let Self::U16(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an U16, return a reference to it. Returns Err otherwise.
	pub fn as_u16(&self) -> Result<u16, DowncastError> {
		if let Self::U16(ret) = self {
			Ok(*ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "u16",
			})
		}
	}

	/// If the `Value` is an U16, return it. Returns Err otherwise.
	pub fn into_u16(self) -> Result<u16, DowncastError> {
		if let Self::U16(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "u16",
			})
		}
	}

	/// Returns true if the `Value` is an I16. Returns false otherwise.
	pub fn is_i16(&self) -> bool {
		if let Self::I16(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an I16, return a reference to it. Returns Err otherwise.
	pub fn as_i16(&self) -> Result<i16, DowncastError> {
		if let Self::I16(ret) = self {
			Ok(*ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "i16",
			})
		}
	}

	/// If the `Value` is an I16, return it. Returns Err otherwise.
	pub fn into_i16(self) -> Result<i16, DowncastError> {
		if let Self::I16(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "i16",
			})
		}
	}

	/// Returns true if the `Value` is an U32. Returns false otherwise.
	pub fn is_u32(&self) -> bool {
		if let Self::U32(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an U32, return a reference to it. Returns Err otherwise.
	pub fn as_u32(&self) -> Result<u32, DowncastError> {
		if let Self::U32(ret) = self {
			Ok(*ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "u32",
			})
		}
	}

	/// If the `Value` is an U32, return it. Returns Err otherwise.
	pub fn into_u32(self) -> Result<u32, DowncastError> {
		if let Self::U32(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "u32",
			})
		}
	}

	/// Returns true if the `Value` is an I32. Returns false otherwise.
	pub fn is_i32(&self) -> bool {
		if let Self::I32(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an I32, return a reference to it. Returns Err otherwise.
	pub fn as_i32(&self) -> Result<i32, DowncastError> {
		if let Self::I32(ret) = self {
			Ok(*ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "i32",
			})
		}
	}

	/// If the `Value` is an I32, return it. Returns Err otherwise.
	pub fn into_i32(self) -> Result<i32, DowncastError> {
		if let Self::I32(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "i32",
			})
		}
	}

	/// Returns true if the `Value` is an U64. Returns false otherwise.
	pub fn is_u64(&self) -> bool {
		if let Self::U64(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an U64, return a reference to it. Returns Err otherwise.
	pub fn as_u64(&self) -> Result<u64, DowncastError> {
		if let Self::U64(ret) = self {
			Ok(*ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "u64",
			})
		}
	}

	/// If the `Value` is an U64, return it. Returns Err otherwise.
	pub fn into_u64(self) -> Result<u64, DowncastError> {
		if let Self::U64(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "u64",
			})
		}
	}

	/// Returns true if the `Value` is an I64. Returns false otherwise.
	pub fn is_i64(&self) -> bool {
		if let Self::I64(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an I64, return a reference to it. Returns Err otherwise.
	pub fn as_i64(&self) -> Result<i64, DowncastError> {
		if let Self::I64(ret) = self {
			Ok(*ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "i64",
			})
		}
	}

	/// If the `Value` is an I64, return it. Returns Err otherwise.
	pub fn into_i64(self) -> Result<i64, DowncastError> {
		if let Self::I64(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "i64",
			})
		}
	}

	/// Returns true if the `Value` is an F32. Returns false otherwise.
	pub fn is_f32(&self) -> bool {
		if let Self::F32(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an F32, return a reference to it. Returns Err otherwise.
	pub fn as_f32(&self) -> Result<f32, DowncastError> {
		if let Self::F32(ret) = self {
			Ok(*ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "f32",
			})
		}
	}

	/// If the `Value` is an F32, return it. Returns Err otherwise.
	pub fn into_f32(self) -> Result<f32, DowncastError> {
		if let Self::F32(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "f32",
			})
		}
	}

	/// Returns true if the `Value` is an F64. Returns false otherwise.
	pub fn is_f64(&self) -> bool {
		if let Self::F64(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an F64, return a reference to it. Returns Err otherwise.
	pub fn as_f64(&self) -> Result<f64, DowncastError> {
		if let Self::F64(ret) = self {
			Ok(*ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "f64",
			})
		}
	}

	/// If the `Value` is an F64, return it. Returns Err otherwise.
	pub fn into_f64(self) -> Result<f64, DowncastError> {
		if let Self::F64(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "f64",
			})
		}
	}

	/// Returns true if the `Value` is an Date. Returns false otherwise.
	pub fn is_date(&self) -> bool {
		if let Self::Date(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Date, return a reference to it. Returns Err otherwise.
	pub fn as_date(&self) -> Result<&Date, DowncastError> {
		if let Self::Date(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "date",
			})
		}
	}

	/// If the `Value` is an Date, return it. Returns Err otherwise.
	pub fn into_date(self) -> Result<Date, DowncastError> {
		if let Self::Date(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "date",
			})
		}
	}

	/// Returns true if the `Value` is an Time. Returns false otherwise.
	pub fn is_time(&self) -> bool {
		if let Self::Time(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Time, return a reference to it. Returns Err otherwise.
	pub fn as_time(&self) -> Result<&Time, DowncastError> {
		if let Self::Time(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "time",
			})
		}
	}

	/// If the `Value` is an Time, return it. Returns Err otherwise.
	pub fn into_time(self) -> Result<Time, DowncastError> {
		if let Self::Time(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "time",
			})
		}
	}

	/// Returns true if the `Value` is an Timestamp. Returns false otherwise.
	pub fn is_timestamp(&self) -> bool {
		if let Self::Timestamp(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Timestamp, return a reference to it. Returns Err otherwise.
	pub fn as_timestamp(&self) -> Result<&Timestamp, DowncastError> {
		if let Self::Timestamp(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "timestamp",
			})
		}
	}

	/// If the `Value` is an Timestamp, return it. Returns Err otherwise.
	pub fn into_timestamp(self) -> Result<Timestamp, DowncastError> {
		if let Self::Timestamp(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "timestamp",
			})
		}
	}

	/// Returns true if the `Value` is an Decimal. Returns false otherwise.
	pub fn is_decimal(&self) -> bool {
		if let Self::Decimal(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Decimal, return a reference to it. Returns Err otherwise.
	pub fn as_decimal(&self) -> Result<&Decimal, DowncastError> {
		if let Self::Decimal(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "decimal",
			})
		}
	}

	/// If the `Value` is an Decimal, return it. Returns Err otherwise.
	pub fn into_decimal(self) -> Result<Decimal, DowncastError> {
		if let Self::Decimal(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "decimal",
			})
		}
	}

	/// Returns true if the `Value` is an ByteArray. Returns false otherwise.
	pub fn is_byte_array(&self) -> bool {
		if let Self::ByteArray(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an ByteArray, return a reference to it. Returns Err otherwise.
	pub fn as_byte_array(&self) -> Result<&Vec<u8>, DowncastError> {
		if let Self::ByteArray(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "byte_array",
			})
		}
	}

	/// If the `Value` is an ByteArray, return it. Returns Err otherwise.
	pub fn into_byte_array(self) -> Result<Vec<u8>, DowncastError> {
		if let Self::ByteArray(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "byte_array",
			})
		}
	}

	/// Returns true if the `Value` is an Bson. Returns false otherwise.
	pub fn is_bson(&self) -> bool {
		if let Self::Bson(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Bson, return a reference to it. Returns Err otherwise.
	pub fn as_bson(&self) -> Result<&Bson, DowncastError> {
		if let Self::Bson(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "bson",
			})
		}
	}

	/// If the `Value` is an Bson, return it. Returns Err otherwise.
	pub fn into_bson(self) -> Result<Bson, DowncastError> {
		if let Self::Bson(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "bson",
			})
		}
	}

	/// Returns true if the `Value` is an String. Returns false otherwise.
	pub fn is_string(&self) -> bool {
		if let Self::String(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an String, return a reference to it. Returns Err otherwise.
	pub fn as_string(&self) -> Result<&String, DowncastError> {
		if let Self::String(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "string",
			})
		}
	}

	/// If the `Value` is an String, return it. Returns Err otherwise.
	pub fn into_string(self) -> Result<String, DowncastError> {
		if let Self::String(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "string",
			})
		}
	}

	/// Returns true if the `Value` is an Json. Returns false otherwise.
	pub fn is_json(&self) -> bool {
		if let Self::Json(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Json, return a reference to it. Returns Err otherwise.
	pub fn as_json(&self) -> Result<&Json, DowncastError> {
		if let Self::Json(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "json",
			})
		}
	}

	/// If the `Value` is an Json, return it. Returns Err otherwise.
	pub fn into_json(self) -> Result<Json, DowncastError> {
		if let Self::Json(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "json",
			})
		}
	}

	/// Returns true if the `Value` is an Enum. Returns false otherwise.
	pub fn is_enum(&self) -> bool {
		if let Self::Enum(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Enum, return a reference to it. Returns Err otherwise.
	pub fn as_enum(&self) -> Result<&Enum, DowncastError> {
		if let Self::Enum(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "enum",
			})
		}
	}

	/// If the `Value` is an Enum, return it. Returns Err otherwise.
	pub fn into_enum(self) -> Result<Enum, DowncastError> {
		if let Self::Enum(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "enum",
			})
		}
	}

	/// Returns true if the `Value` is an List. Returns false otherwise.
	pub fn is_list(&self) -> bool {
		if let Self::List(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an List, return a reference to it. Returns Err otherwise.
	pub fn as_list(&self) -> Result<&List<Self>, DowncastError> {
		if let Self::List(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "list",
			})
		}
	}

	/// If the `Value` is an List, return it. Returns Err otherwise.
	pub fn into_list(self) -> Result<List<Self>, DowncastError> {
		if let Self::List(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "list",
			})
		}
	}

	/// Returns true if the `Value` is an Map. Returns false otherwise.
	pub fn is_map(&self) -> bool {
		if let Self::Map(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Map, return a reference to it. Returns Err otherwise.
	pub fn as_map(&self) -> Result<&Map<Self, Self>, DowncastError> {
		if let Self::Map(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "map",
			})
		}
	}

	/// If the `Value` is an Map, return it. Returns Err otherwise.
	pub fn into_map(self) -> Result<Map<Self, Self>, DowncastError> {
		if let Self::Map(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "map",
			})
		}
	}

	/// Returns true if the `Value` is an Group. Returns false otherwise.
	pub fn is_group(&self) -> bool {
		if let Self::Group(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Group, return a reference to it. Returns Err otherwise.
	pub fn as_group(&self) -> Result<&Group, DowncastError> {
		if let Self::Group(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "group",
			})
		}
	}

	/// If the `Value` is an Group, return it. Returns Err otherwise.
	pub fn into_group(self) -> Result<Group, DowncastError> {
		if let Self::Group(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "group",
			})
		}
	}

	/// Returns true if the `Value` is an Option. Returns false otherwise.
	pub fn is_option(&self) -> bool {
		if let Self::Option(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Option, return a reference to it. Returns Err otherwise.
	fn as_option(&self) -> Result<&Option<ValueRequired>, DowncastError> {
		if let Self::Option(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "option",
			})
		}
	}

	/// If the `Value` is an Option, return it. Returns Err otherwise.
	pub fn into_option(self) -> Result<Option<Self>, DowncastError> {
		if let Self::Option(ret) = self {
			Ok(ret.map(Into::into))
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "option",
			})
		}
	}
}

impl From<bool> for Value {
	fn from(value: bool) -> Self {
		Self::Bool(value)
	}
}
impl From<u8> for Value {
	fn from(value: u8) -> Self {
		Self::U8(value)
	}
}
impl From<i8> for Value {
	fn from(value: i8) -> Self {
		Self::I8(value)
	}
}
impl From<u16> for Value {
	fn from(value: u16) -> Self {
		Self::U16(value)
	}
}
impl From<i16> for Value {
	fn from(value: i16) -> Self {
		Self::I16(value)
	}
}
impl From<u32> for Value {
	fn from(value: u32) -> Self {
		Self::U32(value)
	}
}
impl From<i32> for Value {
	fn from(value: i32) -> Self {
		Self::I32(value)
	}
}
impl From<u64> for Value {
	fn from(value: u64) -> Self {
		Self::U64(value)
	}
}
impl From<i64> for Value {
	fn from(value: i64) -> Self {
		Self::I64(value)
	}
}
impl From<f32> for Value {
	fn from(value: f32) -> Self {
		Self::F32(value)
	}
}
impl From<f64> for Value {
	fn from(value: f64) -> Self {
		Self::F64(value)
	}
}
impl From<Date> for Value {
	fn from(value: Date) -> Self {
		Self::Date(value)
	}
}
impl From<Time> for Value {
	fn from(value: Time) -> Self {
		Self::Time(value)
	}
}
impl From<Timestamp> for Value {
	fn from(value: Timestamp) -> Self {
		Self::Timestamp(value)
	}
}
impl From<Decimal> for Value {
	fn from(value: Decimal) -> Self {
		Self::Decimal(value)
	}
}
impl From<Vec<u8>> for Value {
	fn from(value: Vec<u8>) -> Self {
		Self::ByteArray(value)
	}
}
impl From<Bson> for Value {
	fn from(value: Bson) -> Self {
		Self::Bson(value)
	}
}
impl From<String> for Value {
	fn from(value: String) -> Self {
		Self::String(value)
	}
}
impl From<Json> for Value {
	fn from(value: Json) -> Self {
		Self::Json(value)
	}
}
impl From<Enum> for Value {
	fn from(value: Enum) -> Self {
		Self::Enum(value)
	}
}
impl<T> From<List<T>> for Value
where
	T: Into<Self>,
{
	default fn from(value: List<T>) -> Self {
		Self::List(List(value.0.into_iter().map(Into::into).collect()))
	}
}
impl From<List<Self>> for Value {
	fn from(value: List<Self>) -> Self {
		Self::List(value)
	}
}
impl<K, V> From<Map<K, V>> for Value
where
	K: Into<Self> + Hash + Eq,
	V: Into<Self>,
{
	default fn from(value: Map<K, V>) -> Self {
		Self::Map(Map(value
			.0
			.into_iter()
			.map(|(k, v)| (k.into(), v.into()))
			.collect()))
	}
}
impl From<Map<Self, Self>> for Value {
	fn from(value: Map<Self, Self>) -> Self {
		Self::Map(value)
	}
}
impl From<Group> for Value {
	fn from(value: Group) -> Self {
		Self::Group(value)
	}
}
impl<T> From<Option<T>> for Value
where
	T: Into<Self>,
{
	default fn from(value: Option<T>) -> Self {
		Self::Option(
			value
				.map(Into::into)
				.map(|x| <Option<ValueRequired> as From<Self>>::from(x).unwrap()),
		)
	}
}
impl From<Option<Self>> for Value {
	fn from(value: Option<Self>) -> Self {
		Self::Option(value.map(|x| <Option<ValueRequired>>::from(x).unwrap()))
	}
}
impl<T> From<Box<T>> for Value
where
	T: Into<Self>,
{
	fn from(value: Box<T>) -> Self {
		(*value).into()
	}
}
macro_rules! impl_parquet_record_array1 {
	($($i:tt)*) => {$(
		impl From<[u8; $i]> for Value {
			fn from(value: [u8; $i]) -> Self {
				let x: Box<[u8]> = Box::new(value);
				let x: Vec<u8> = x.into();
				x.into()
			}
		}
	)*}
}
impl_parquet_record_array1!(0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32);

macro_rules! impl_parquet_record_tuple {
	($len:tt $($t:ident $i:tt)*) => (
		 impl<$($t,)*> DowncastImpl<Value> for ($($t,)*) where $($t: DowncastImpl<Value>,)* {
			fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
				#[allow(unused_mut, unused_variables)]
				let mut fields = self_.into_group()?.into_fields().into_iter();
				if fields.len() != $len {
					return Err(DowncastError{from:"",to:""});
				}
				Ok(($({let _ = $i;fields.next().unwrap().downcast()?},)*))
			}
		}
		impl<$($t,)*> DowncastImpl<Group> for ($($t,)*) where $($t: DowncastImpl<Value>,)* {
			fn downcast_impl(self_: Group) -> Result<Self, DowncastError> {
				#[allow(unused_mut, unused_variables)]
				let mut fields = self_.into_fields().into_iter();
				if fields.len() != $len {
					return Err(DowncastError{from:"",to:""});
				}
				Ok(($({let _ = $i;fields.next().unwrap().downcast()?},)*))
			}
		}
		impl<$($t,)*> From<($($t,)*)> for Value where $($t: Into<Value>,)* {
			#[allow(unused_variables)]
			fn from(value: ($($t,)*)) -> Self {
				Value::Group(Group::new(vec![$(value.$i.into(),)*], None))
			}
		}
	   impl<$($t,)*> PartialEq<($($t,)*)> for Value where Value: $(PartialEq<$t> +)* {
			#[allow(unused_variables)]
			fn eq(&self, other: &($($t,)*)) -> bool {
				self.is_group() $(&& self.as_group().unwrap()[$i] == other.$i)*
			}
		}
		impl<$($t,)*> PartialEq<($($t,)*)> for Group where Value: $(PartialEq<$t> +)* {
			#[allow(unused_variables)]
			fn eq(&self, other: &($($t,)*)) -> bool {
				$(self[$i] == other.$i && )* true
			}
		}
	);
}
impl_parquet_record_tuple!(0);
impl_parquet_record_tuple!(1 A 0);
impl_parquet_record_tuple!(2 A 0 B 1);
impl_parquet_record_tuple!(3 A 0 B 1 C 2);
impl_parquet_record_tuple!(4 A 0 B 1 C 2 D 3);
impl_parquet_record_tuple!(5 A 0 B 1 C 2 D 3 E 4);
impl_parquet_record_tuple!(6 A 0 B 1 C 2 D 3 E 4 F 5);
impl_parquet_record_tuple!(7 A 0 B 1 C 2 D 3 E 4 F 5 G 6);
impl_parquet_record_tuple!(8 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7);
impl_parquet_record_tuple!(9 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8);
impl_parquet_record_tuple!(10 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9);
impl_parquet_record_tuple!(11 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10);
impl_parquet_record_tuple!(12 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11);
impl_parquet_record_tuple!(13 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12);
impl_parquet_record_tuple!(14 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13);
impl_parquet_record_tuple!(15 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14);
impl_parquet_record_tuple!(16 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15);
impl_parquet_record_tuple!(17 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16);
impl_parquet_record_tuple!(18 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17);
impl_parquet_record_tuple!(19 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18);
impl_parquet_record_tuple!(20 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19);
impl_parquet_record_tuple!(21 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20);
impl_parquet_record_tuple!(22 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21);
impl_parquet_record_tuple!(23 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22);
impl_parquet_record_tuple!(24 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23);
impl_parquet_record_tuple!(25 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24);
impl_parquet_record_tuple!(26 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25);
impl_parquet_record_tuple!(27 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26);
impl_parquet_record_tuple!(28 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27);
impl_parquet_record_tuple!(29 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28);
impl_parquet_record_tuple!(30 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29);
impl_parquet_record_tuple!(31 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30);
impl_parquet_record_tuple!(32 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30 AF 31);

// Downcast implementations for Value so we can try downcasting it to a specific type if
// we know it.

impl DowncastImpl<Self> for Value {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		Ok(self_)
	}
}
impl DowncastImpl<Value> for bool {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_bool()
	}
}
impl DowncastImpl<Value> for u8 {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_u8()
	}
}
impl DowncastImpl<Value> for i8 {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_i8()
	}
}
impl DowncastImpl<Value> for u16 {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_u16()
	}
}
impl DowncastImpl<Value> for i16 {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_i16()
	}
}
impl DowncastImpl<Value> for u32 {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_u32()
	}
}
impl DowncastImpl<Value> for i32 {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_i32()
	}
}
impl DowncastImpl<Value> for u64 {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_u64()
	}
}
impl DowncastImpl<Value> for i64 {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_i64()
	}
}
impl DowncastImpl<Value> for f32 {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_f32()
	}
}
impl DowncastImpl<Value> for f64 {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_f64()
	}
}
impl DowncastImpl<Value> for Date {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_date()
	}
}
impl DowncastImpl<Value> for Time {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_time()
	}
}
impl DowncastImpl<Value> for Timestamp {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_timestamp()
	}
}
impl DowncastImpl<Value> for Decimal {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_decimal()
	}
}
impl DowncastImpl<Value> for Vec<u8> {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_byte_array()
	}
}
impl DowncastImpl<Value> for Bson {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_bson()
	}
}
impl DowncastImpl<Value> for String {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_string()
	}
}
impl DowncastImpl<Value> for Json {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_json()
	}
}
impl DowncastImpl<Value> for Enum {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_enum()
	}
}
impl<T> DowncastImpl<Value> for List<T>
where
	T: DowncastImpl<Value>,
{
	default fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_list().and_then(|list| {
			list.0
				.into_iter()
				.map(Downcast::downcast)
				.collect::<Result<Vec<_>, _>>()
				.map(List)
		})
	}
}
impl DowncastImpl<Value> for List<Value> {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_list()
	}
}
impl<K, V> DowncastImpl<Value> for Map<K, V>
where
	K: DowncastImpl<Value> + Hash + Eq,
	V: DowncastImpl<Value>,
{
	default fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_map().and_then(|map| {
			map.0
				.into_iter()
				.map(|(k, v)| Ok((k.downcast()?, v.downcast()?)))
				.collect::<Result<HashMap<_, _>, _>>()
				.map(Map)
		})
	}
}
impl DowncastImpl<Value> for Map<Value, Value> {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_map()
	}
}
impl DowncastImpl<Value> for Group {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_group()
	}
}
impl<T> DowncastImpl<Value> for Option<T>
where
	T: DowncastImpl<Value>,
{
	default fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		match self_.into_option()? {
			Some(t) => t.downcast().map(Some),
			None => Ok(None),
		}
	}
}
impl DowncastImpl<Value> for Option<Value> {
	fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
		self_.into_option()
	}
}
macro_rules! impl_parquet_record_array {
	($($i:tt)*) => {$(
		impl DowncastImpl<Value> for [u8; $i] {
			fn downcast_impl(self_: Value) -> Result<Self, DowncastError> {
				let err = DowncastError {
					from: self_.type_name(),
					to: stringify!([u8; $i]),
				};
				let x: Box<[u8]> = self_.into_byte_array().map_err(|_| err)?.into_boxed_slice();
				(&*x).try_into().map_err(|_| err)
			}
		}
	)*}
}
impl_parquet_record_array!(0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32);

// PartialEq implementations for Value so we can compare it with typed values

impl PartialEq<bool> for Value {
	fn eq(&self, other: &bool) -> bool {
		self.as_bool().map(|bool| &bool == other).unwrap_or(false)
	}
}
impl PartialEq<u8> for Value {
	fn eq(&self, other: &u8) -> bool {
		self.as_u8().map(|u8| &u8 == other).unwrap_or(false)
	}
}
impl PartialEq<i8> for Value {
	fn eq(&self, other: &i8) -> bool {
		self.as_i8().map(|i8| &i8 == other).unwrap_or(false)
	}
}
impl PartialEq<u16> for Value {
	fn eq(&self, other: &u16) -> bool {
		self.as_u16().map(|u16| &u16 == other).unwrap_or(false)
	}
}
impl PartialEq<i16> for Value {
	fn eq(&self, other: &i16) -> bool {
		self.as_i16().map(|i16| &i16 == other).unwrap_or(false)
	}
}
impl PartialEq<u32> for Value {
	fn eq(&self, other: &u32) -> bool {
		self.as_u32().map(|u32| &u32 == other).unwrap_or(false)
	}
}
impl PartialEq<i32> for Value {
	fn eq(&self, other: &i32) -> bool {
		self.as_i32().map(|i32| &i32 == other).unwrap_or(false)
	}
}
impl PartialEq<u64> for Value {
	fn eq(&self, other: &u64) -> bool {
		self.as_u64().map(|u64| &u64 == other).unwrap_or(false)
	}
}
impl PartialEq<i64> for Value {
	fn eq(&self, other: &i64) -> bool {
		self.as_i64().map(|i64| &i64 == other).unwrap_or(false)
	}
}
impl PartialEq<f32> for Value {
	fn eq(&self, other: &f32) -> bool {
		self.as_f32().map(|f32| &f32 == other).unwrap_or(false)
	}
}
impl PartialEq<f64> for Value {
	fn eq(&self, other: &f64) -> bool {
		self.as_f64().map(|f64| &f64 == other).unwrap_or(false)
	}
}
impl PartialEq<Date> for Value {
	fn eq(&self, other: &Date) -> bool {
		self.as_date().map(|date| date == other).unwrap_or(false)
	}
}
impl PartialEq<Time> for Value {
	fn eq(&self, other: &Time) -> bool {
		self.as_time().map(|time| time == other).unwrap_or(false)
	}
}
impl PartialEq<Timestamp> for Value {
	fn eq(&self, other: &Timestamp) -> bool {
		self.as_timestamp()
			.map(|timestamp| timestamp == other)
			.unwrap_or(false)
	}
}
impl PartialEq<Decimal> for Value {
	fn eq(&self, other: &Decimal) -> bool {
		self.as_decimal()
			.map(|decimal| decimal == other)
			.unwrap_or(false)
	}
}
impl PartialEq<Vec<u8>> for Value {
	fn eq(&self, other: &Vec<u8>) -> bool {
		self.as_byte_array()
			.map(|byte_array| byte_array == other)
			.unwrap_or(false)
	}
}
impl PartialEq<Bson> for Value {
	fn eq(&self, other: &Bson) -> bool {
		self.as_bson().map(|bson| bson == other).unwrap_or(false)
	}
}
impl PartialEq<String> for Value {
	fn eq(&self, other: &String) -> bool {
		self.as_string()
			.map(|string| string == other)
			.unwrap_or(false)
	}
}
impl PartialEq<Json> for Value {
	fn eq(&self, other: &Json) -> bool {
		self.as_json().map(|json| json == other).unwrap_or(false)
	}
}
impl PartialEq<Enum> for Value {
	fn eq(&self, other: &Enum) -> bool {
		self.as_enum().map(|enum_| enum_ == other).unwrap_or(false)
	}
}
impl<T> PartialEq<List<T>> for Value
where
	Value: PartialEq<T>,
{
	fn eq(&self, other: &List<T>) -> bool {
		self.as_list().map(|list| list == other).unwrap_or(false)
	}
}
impl<K, V> PartialEq<Map<K, V>> for Value
where
	Value: PartialEq<K> + PartialEq<V>,
	K: Hash + Eq + Clone + Into<Value>,
{
	fn eq(&self, other: &Map<K, V>) -> bool {
		self.as_map()
			.map(|map| {
				if map.0.len() != other.0.len() {
					return false;
				}

				// This comparison unfortunately requires a bit of a hack. This could be
				// eliminated by ensuring that Value::X hashes identically to X. TODO.
				let other = other
					.0
					.iter()
					.map(|(k, v)| (k.clone().into(), v))
					.collect::<HashMap<Self, _>>();

				map.0
					.iter()
					.all(|(key, value)| other.get(key).map_or(false, |v| value == *v))
			})
			.unwrap_or(false)
	}
}
impl PartialEq<Group> for Value {
	fn eq(&self, other: &Group) -> bool {
		self.as_group().map(|group| group == other).unwrap_or(false)
	}
}
impl<T> PartialEq<Option<T>> for Value
where
	Value: PartialEq<T>,
{
	fn eq(&self, other: &Option<T>) -> bool {
		self.as_option()
			.map(|option| match (&option, other) {
				(Some(a), Some(b)) if a.eq(b) => true,
				(None, None) => true,
				_ => false,
			})
			.unwrap_or(false)
	}
}
impl<T> PartialEq<Box<T>> for Value
where
	Value: PartialEq<T>,
{
	fn eq(&self, other: &Box<T>) -> bool {
		self == other
	}
}

// impl Serialize for Value {
// 	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
// 	where
// 		S: Serializer,
// 	{
// 		Self::serde_serialize(self, serializer)
// 	}
// }
// impl<'de> Deserialize<'de> for Value {
// 	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
// 	where
// 		D: Deserializer<'de>,
// 	{
// 		Self::serde_deserialize(deserializer, None)
// 	}
// }

// /// A Reader that wraps a Reader, wrapping the read value in a `Record`.
// pub struct ValueReader<T>(T);
// impl<T> internal::record::Reader for ValueReader<T>
// where
// 	T: internal::record::Reader<Item = internal::record::types::Value>,
// {
// 	type Item = Value;

// 	#[inline]
// 	fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item, ParquetError> {
// 		self.0.read(def_level, rep_level).map(Into::into)
// 	}

// 	#[inline]
// 	fn advance_columns(&mut self) -> Result<(), ParquetError> {
// 		self.0.advance_columns()
// 	}

// 	#[inline]
// 	fn has_next(&self) -> bool {
// 		self.0.has_next()
// 	}

// 	#[inline]
// 	fn current_def_level(&self) -> i16 {
// 		self.0.current_def_level()
// 	}

// 	#[inline]
// 	fn current_rep_level(&self) -> i16 {
// 		self.0.current_rep_level()
// 	}
// }
