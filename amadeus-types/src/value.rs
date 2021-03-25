//! Implement [`Record`] for [`Value`] – an enum representing any valid Parquet value.

#![allow(clippy::type_complexity)]

use fxhash::FxBuildHasher;
use hashlink::linked_hash_map::LinkedHashMap;
use recycle::VecExt;
use serde::{de::Deserializer, ser::Serializer, Deserialize, Serialize};
use std::{
	cmp::Ordering, collections::HashMap, convert::TryInto, fmt, fmt::Debug, hash::{BuildHasher, Hash, Hasher}, iter::FromIterator, sync::Arc
};

use crate::list::ListVec;

use super::{
	AmadeusOrd, Bson, Data, Date, DateTime, DateTimeWithoutTimezone, DateWithoutTimezone, Decimal, Downcast, DowncastError, DowncastFrom, Enum, Group, IpAddr, Json, List, Time, TimeWithoutTimezone, Timezone, Url, ValueRequired, Webpage
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
	DateWithoutTimezone,
	Time,
	TimeWithoutTimezone,
	DateTime,
	DateTimeWithoutTimezone,
	Timezone,
	Decimal,
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
	DateWithoutTimezone,
	Time,
	TimeWithoutTimezone,
	DateTime,
	DateTimeWithoutTimezone,
	Timezone,
	Decimal,
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

/// Represents any valid Amadeus value.
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
	/// Date without a time of day, stores the number of days from the Unix epoch, 1
	/// January 1970.
	DateWithoutTimezone(DateWithoutTimezone),
	/// Time of day, stores the number of microseconds from midnight.
	Time(Time),
	/// Time of day, stores the number of microseconds from midnight.
	TimeWithoutTimezone(TimeWithoutTimezone),
	/// Milliseconds from the Unix epoch, 1 January 1970.
	DateTime(DateTime),
	/// Milliseconds from the Unix epoch, 1 January 1970.
	DateTimeWithoutTimezone(DateTimeWithoutTimezone),
	/// Timezone.
	Timezone(Timezone),
	/// Decimal value.
	Decimal(Decimal),
	/// BSON binary value.
	Bson(Bson),
	/// UTF-8 encoded character string.
	String(String),
	/// JSON string.
	Json(Json),
	/// Enum string.
	Enum(Enum),
	/// URL
	Url(Url),
	/// Webpage
	Webpage(Webpage<'static>),
	/// Ip Address
	IpAddr(IpAddr),

	// Complex types
	/// List of elements.
	List(List<Value>),
	/// Map of key-value pairs.
	Map(HashMap<Value, Value>),
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
				ValueRequired::DateWithoutTimezone(value) => serializer.serialize_some(&value),
				ValueRequired::Time(value) => serializer.serialize_some(&value),
				ValueRequired::TimeWithoutTimezone(value) => serializer.serialize_some(&value),
				ValueRequired::DateTime(value) => serializer.serialize_some(&value),
				ValueRequired::DateTimeWithoutTimezone(value) => serializer.serialize_some(&value),
				ValueRequired::Timezone(value) => serializer.serialize_some(&value),
				ValueRequired::Decimal(value) => serializer.serialize_some(&value),
				ValueRequired::Bson(value) => serializer.serialize_some(&value),
				ValueRequired::String(value) => serializer.serialize_some(&value),
				ValueRequired::Json(value) => serializer.serialize_some(&value),
				ValueRequired::Enum(value) => serializer.serialize_some(&value),
				ValueRequired::Url(value) => serializer.serialize_some(&value),
				ValueRequired::Webpage(value) => serializer.serialize_some(&value),
				ValueRequired::IpAddr(value) => serializer.serialize_some(&value),
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
				0_u8.hash(state);
				value.hash(state);
			}
			Self::U8(value) => {
				1_u8.hash(state);
				value.hash(state);
			}
			Self::I8(value) => {
				2_u8.hash(state);
				value.hash(state);
			}
			Self::U16(value) => {
				3_u8.hash(state);
				value.hash(state);
			}
			Self::I16(value) => {
				4_u8.hash(state);
				value.hash(state);
			}
			Self::U32(value) => {
				5_u8.hash(state);
				value.hash(state);
			}
			Self::I32(value) => {
				6_u8.hash(state);
				value.hash(state);
			}
			Self::U64(value) => {
				7_u8.hash(state);
				value.hash(state);
			}
			Self::I64(value) => {
				8_u8.hash(state);
				value.hash(state);
			}
			Self::F32(_value) => {
				9_u8.hash(state);
			}
			Self::F64(_value) => {
				10_u8.hash(state);
			}
			Self::Date(value) => {
				11_u8.hash(state);
				value.hash(state);
			}
			Self::DateWithoutTimezone(value) => {
				11_u8.hash(state);
				value.hash(state);
			}
			Self::Time(value) => {
				12_u8.hash(state);
				value.hash(state);
			}
			Self::TimeWithoutTimezone(value) => {
				12_u8.hash(state);
				value.hash(state);
			}
			Self::DateTime(value) => {
				13_u8.hash(state);
				value.hash(state);
			}
			Self::DateTimeWithoutTimezone(value) => {
				13_u8.hash(state);
				value.hash(state);
			}
			Self::Timezone(value) => {
				13_u8.hash(state);
				value.hash(state);
			}
			Self::Decimal(_value) => {
				14_u8.hash(state);
			}
			Self::Bson(value) => {
				15_u8.hash(state);
				value.hash(state);
			}
			Self::String(value) => {
				16_u8.hash(state);
				value.hash(state);
			}
			Self::Json(value) => {
				17_u8.hash(state);
				value.hash(state);
			}
			Self::Enum(value) => {
				18_u8.hash(state);
				value.hash(state);
			}
			Self::Url(value) => {
				19_u8.hash(state);
				value.hash(state);
			}
			Self::Webpage(value) => {
				20_u8.hash(state);
				value.hash(state);
			}
			Self::IpAddr(value) => {
				21_u8.hash(state);
				value.hash(state);
			}
			Self::List(value) => {
				22_u8.hash(state);
				value.hash(state);
			}
			Self::Map(_value) => {
				23_u8.hash(state);
			}
			Self::Group(_value) => {
				24_u8.hash(state);
			}
			Self::Option(value) => {
				25_u8.hash(state);
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
			(Self::DateWithoutTimezone(a), Self::DateWithoutTimezone(b)) => a.partial_cmp(b),
			(Self::Time(a), Self::Time(b)) => a.partial_cmp(b),
			(Self::TimeWithoutTimezone(a), Self::TimeWithoutTimezone(b)) => a.partial_cmp(b),
			(Self::DateTime(a), Self::DateTime(b)) => a.partial_cmp(b),
			(Self::DateTimeWithoutTimezone(a), Self::DateTimeWithoutTimezone(b)) => {
				a.partial_cmp(b)
			}
			(Self::Timezone(a), Self::Timezone(b)) => a.partial_cmp(b),
			(Self::Decimal(a), Self::Decimal(b)) => a.partial_cmp(b),
			(Self::Bson(a), Self::Bson(b)) => a.partial_cmp(b),
			(Self::String(a), Self::String(b)) => a.partial_cmp(b),
			(Self::Json(a), Self::Json(b)) => a.partial_cmp(b),
			(Self::Enum(a), Self::Enum(b)) => a.partial_cmp(b),
			(Self::Url(a), Self::Url(b)) => a.partial_cmp(b),
			(Self::Webpage(a), Self::Webpage(b)) => a.partial_cmp(b),
			(Self::IpAddr(a), Self::IpAddr(b)) => a.partial_cmp(b),
			(Self::List(a), Self::List(b)) => a.partial_cmp(b),
			(Self::Map(_a), Self::Map(_b)) => None, // TODO?
			(Self::Group(a), Self::Group(b)) => a.partial_cmp(b),
			(Self::Option(a), Self::Option(b)) => a.partial_cmp(b),
			_ => None,
		}
	}
}
impl AmadeusOrd for Value {
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		match (self, other) {
			(Self::Bool(a), Self::Bool(b)) => a.amadeus_cmp(b),
			(Self::U8(a), Self::U8(b)) => a.amadeus_cmp(b),
			(Self::I8(a), Self::I8(b)) => a.amadeus_cmp(b),
			(Self::U16(a), Self::U16(b)) => a.amadeus_cmp(b),
			(Self::I16(a), Self::I16(b)) => a.amadeus_cmp(b),
			(Self::U32(a), Self::U32(b)) => a.amadeus_cmp(b),
			(Self::I32(a), Self::I32(b)) => a.amadeus_cmp(b),
			(Self::U64(a), Self::U64(b)) => a.amadeus_cmp(b),
			(Self::I64(a), Self::I64(b)) => a.amadeus_cmp(b),
			(Self::F32(a), Self::F32(b)) => a.amadeus_cmp(b),
			(Self::F64(a), Self::F64(b)) => a.amadeus_cmp(b),
			(Self::Date(a), Self::Date(b)) => a.amadeus_cmp(b),
			(Self::DateWithoutTimezone(a), Self::DateWithoutTimezone(b)) => a.amadeus_cmp(b),
			(Self::Time(a), Self::Time(b)) => a.amadeus_cmp(b),
			(Self::TimeWithoutTimezone(a), Self::TimeWithoutTimezone(b)) => a.amadeus_cmp(b),
			(Self::DateTime(a), Self::DateTime(b)) => a.amadeus_cmp(b),
			(Self::DateTimeWithoutTimezone(a), Self::DateTimeWithoutTimezone(b)) => {
				a.amadeus_cmp(b)
			}
			(Self::Timezone(a), Self::Timezone(b)) => a.amadeus_cmp(b),
			(Self::Decimal(a), Self::Decimal(b)) => a.amadeus_cmp(b),
			(Self::Bson(a), Self::Bson(b)) => a.amadeus_cmp(b),
			(Self::String(a), Self::String(b)) => a.amadeus_cmp(b),
			(Self::Json(a), Self::Json(b)) => a.amadeus_cmp(b),
			(Self::Enum(a), Self::Enum(b)) => a.amadeus_cmp(b),
			(Self::Url(a), Self::Url(b)) => a.amadeus_cmp(b),
			(Self::Webpage(a), Self::Webpage(b)) => a.amadeus_cmp(b),
			(Self::IpAddr(a), Self::IpAddr(b)) => a.amadeus_cmp(b),
			(Self::List(a), Self::List(b)) => a.amadeus_cmp(b),
			(Self::Map(a), Self::Map(b)) => a.amadeus_cmp(b),
			(Self::Group(a), Self::Group(b)) => a.amadeus_cmp(b),
			(Self::Option(a), Self::Option(b)) => a.amadeus_cmp(b),
			_ => unimplemented!(),
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
			Self::DateWithoutTimezone(_value) => "date_without_timezone",
			Self::Time(_value) => "time",
			Self::TimeWithoutTimezone(_value) => "time_without_timezone",
			Self::DateTime(_value) => "date_time",
			Self::DateTimeWithoutTimezone(_value) => "date_time_without_timezone",
			Self::Timezone(_value) => "timezone",
			Self::Decimal(_value) => "decimal",
			Self::Bson(_value) => "bson",
			Self::String(_value) => "string",
			Self::Json(_value) => "json",
			Self::Enum(_value) => "enum",
			Self::Url(_value) => "url",
			Self::Webpage(_value) => "webpage",
			Self::IpAddr(_value) => "ip_addr",
			Self::List(_value) => "list",
			Self::Map(_value) => "map",
			Self::Group(_value) => "group",
			Self::Option(_value) => "option",
		}
	}

	/// Returns true if the `Value` is an Bool. Returns false otherwise.
	pub fn is_bool(&self) -> bool {
		matches!(self, Self::Bool(_))
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
		matches!(self, Self::U8(_))
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
		matches!(self, Self::I8(_))
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
		matches!(self, Self::U16(_))
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
		matches!(self, Self::I16(_))
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
		matches!(self, Self::U32(_))
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
		matches!(self, Self::I32(_))
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
		matches!(self, Self::U64(_))
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
		matches!(self, Self::I64(_))
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
		matches!(self, Self::F32(_))
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
		matches!(self, Self::F64(_))
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
		matches!(self, Self::Date(_))
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

	/// Returns true if the `Value` is an DateWithoutTimezone. Returns false otherwise.
	pub fn is_date_without_timezone(&self) -> bool {
		matches!(self, Self::DateWithoutTimezone(_))
	}

	/// If the `Value` is an DateWithoutTimezone, return a reference to it. Returns Err otherwise.
	pub fn as_date_without_timezone(&self) -> Result<&DateWithoutTimezone, DowncastError> {
		if let Self::DateWithoutTimezone(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "date_without_timezone",
			})
		}
	}

	/// If the `Value` is an DateWithoutTimezone, return it. Returns Err otherwise.
	pub fn into_date_without_timezone(self) -> Result<DateWithoutTimezone, DowncastError> {
		if let Self::DateWithoutTimezone(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "date_without_timezone",
			})
		}
	}

	/// Returns true if the `Value` is an Time. Returns false otherwise.
	pub fn is_time(&self) -> bool {
		matches!(self, Self::Time(_))
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

	/// Returns true if the `Value` is an TimeWithoutTimezone. Returns false otherwise.
	pub fn is_time_without_timezone(&self) -> bool {
		matches!(self, Self::TimeWithoutTimezone(_))
	}

	/// If the `Value` is an TimeWithoutTimezone, return a reference to it. Returns Err otherwise.
	pub fn as_time_without_timezone(&self) -> Result<&TimeWithoutTimezone, DowncastError> {
		if let Self::TimeWithoutTimezone(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "time_without_timezone",
			})
		}
	}

	/// If the `Value` is an TimeWithoutTimezone, return it. Returns Err otherwise.
	pub fn into_time_without_timezone(self) -> Result<TimeWithoutTimezone, DowncastError> {
		if let Self::TimeWithoutTimezone(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "time_without_timezone",
			})
		}
	}

	/// Returns true if the `Value` is an DateTime. Returns false otherwise.
	pub fn is_date_time(&self) -> bool {
		matches!(self, Self::DateTime(_))
	}

	/// If the `Value` is an DateTime, return a reference to it. Returns Err otherwise.
	pub fn as_date_time(&self) -> Result<&DateTime, DowncastError> {
		if let Self::DateTime(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "date_time",
			})
		}
	}

	/// If the `Value` is an DateTime, return it. Returns Err otherwise.
	pub fn into_date_time(self) -> Result<DateTime, DowncastError> {
		if let Self::DateTime(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "date_time",
			})
		}
	}

	/// Returns true if the `Value` is an DateTimeWithoutTimezone. Returns false otherwise.
	pub fn is_date_time_without_timezone(&self) -> bool {
		matches!(self, Self::DateTimeWithoutTimezone(_))
	}

	/// If the `Value` is an DateTimeWithoutTimezone, return a reference to it. Returns Err otherwise.
	pub fn as_date_time_without_timezone(&self) -> Result<&DateTimeWithoutTimezone, DowncastError> {
		if let Self::DateTimeWithoutTimezone(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "date_time_without_timezone",
			})
		}
	}

	/// If the `Value` is an DateTimeWithoutTimezone, return it. Returns Err otherwise.
	pub fn into_date_time_without_timezone(self) -> Result<DateTimeWithoutTimezone, DowncastError> {
		if let Self::DateTimeWithoutTimezone(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "date_time_without_timezone",
			})
		}
	}

	/// Returns true if the `Value` is an Timezone. Returns false otherwise.
	pub fn is_timezone(&self) -> bool {
		matches!(self, Self::Timezone(_))
	}

	/// If the `Value` is an Timezone, return a reference to it. Returns Err otherwise.
	pub fn as_timezone(&self) -> Result<&Timezone, DowncastError> {
		if let Self::Timezone(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "timezone",
			})
		}
	}

	/// If the `Value` is an Timezone, return it. Returns Err otherwise.
	pub fn into_timezone(self) -> Result<Timezone, DowncastError> {
		if let Self::Timezone(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "timezone",
			})
		}
	}

	/// Returns true if the `Value` is an Decimal. Returns false otherwise.
	pub fn is_decimal(&self) -> bool {
		matches!(self, Self::Decimal(_))
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

	/// Returns true if the `Value` is an Bson. Returns false otherwise.
	pub fn is_bson(&self) -> bool {
		matches!(self, Self::Bson(_))
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
		matches!(self, Self::String(_))
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
		matches!(self, Self::Json(_))
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
		matches!(self, Self::Enum(_))
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

	/// Returns true if the `Value` is an Url. Returns false otherwise.
	pub fn is_url(&self) -> bool {
		matches!(self, Self::Url(_))
	}

	/// If the `Value` is an Url, return a reference to it. Returns Err otherwise.
	pub fn as_url(&self) -> Result<&Url, DowncastError> {
		if let Self::Url(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "url",
			})
		}
	}

	/// If the `Value` is an Url, return it. Returns Err otherwise.
	pub fn into_url(self) -> Result<Url, DowncastError> {
		if let Self::Url(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "url",
			})
		}
	}

	/// Returns true if the `Value` is an Webpage. Returns false otherwise.
	pub fn is_webpage(&self) -> bool {
		matches!(self, Self::Webpage(_))
	}

	/// If the `Value` is an Webpage, return a reference to it. Returns Err otherwise.
	pub fn as_webpage(&self) -> Result<&Webpage, DowncastError> {
		if let Self::Webpage(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "webpage",
			})
		}
	}

	/// If the `Value` is an Webpage, return it. Returns Err otherwise.
	pub fn into_webpage(self) -> Result<Webpage<'static>, DowncastError> {
		if let Self::Webpage(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "webpage",
			})
		}
	}

	/// Returns true if the `Value` is an IpAddr. Returns false otherwise.
	pub fn is_ip_addr(&self) -> bool {
		matches!(self, Self::IpAddr(_))
	}

	/// If the `Value` is an IpAddr, return a reference to it. Returns Err otherwise.
	pub fn as_ip_addr(&self) -> Result<&IpAddr, DowncastError> {
		if let Self::IpAddr(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "ip_addr",
			})
		}
	}

	/// If the `Value` is an IpAddr, return it. Returns Err otherwise.
	pub fn into_ip_addr(self) -> Result<IpAddr, DowncastError> {
		if let Self::IpAddr(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "ip_addr",
			})
		}
	}

	/// Returns true if the `Value` is an List. Returns false otherwise.
	pub fn is_list(&self) -> bool {
		matches!(self, Self::List(_))
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
		matches!(self, Self::Map(_))
	}

	/// If the `Value` is an Map, return a reference to it. Returns Err otherwise.
	pub fn as_map(&self) -> Result<&HashMap<Self, Self>, DowncastError> {
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
	pub fn into_map(self) -> Result<HashMap<Self, Self>, DowncastError> {
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
		matches!(self, Self::Group(_))
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
		matches!(self, Self::Option(_))
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
impl From<DateWithoutTimezone> for Value {
	fn from(value: DateWithoutTimezone) -> Self {
		Self::DateWithoutTimezone(value)
	}
}
impl From<Time> for Value {
	fn from(value: Time) -> Self {
		Self::Time(value)
	}
}
impl From<TimeWithoutTimezone> for Value {
	fn from(value: TimeWithoutTimezone) -> Self {
		Self::TimeWithoutTimezone(value)
	}
}
impl From<DateTime> for Value {
	fn from(value: DateTime) -> Self {
		Self::DateTime(value)
	}
}
impl From<DateTimeWithoutTimezone> for Value {
	fn from(value: DateTimeWithoutTimezone) -> Self {
		Self::DateTimeWithoutTimezone(value)
	}
}
impl From<Timezone> for Value {
	fn from(value: Timezone) -> Self {
		Self::Timezone(value)
	}
}
impl From<Decimal> for Value {
	fn from(value: Decimal) -> Self {
		Self::Decimal(value)
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
impl From<Url> for Value {
	fn from(value: Url) -> Self {
		Self::Url(value)
	}
}
impl From<Webpage<'static>> for Value {
	fn from(value: Webpage<'static>) -> Self {
		Self::Webpage(value)
	}
}
impl From<IpAddr> for Value {
	fn from(value: IpAddr) -> Self {
		Self::IpAddr(value)
	}
}
impl<T: Data> From<List<T>> for Value
where
	T: Into<Self>,
{
	fn from(value: List<T>) -> Self {
		Self::List(value.map(Into::into))
	}
}
impl<K, V, S> From<HashMap<K, V, S>> for Value
where
	K: Into<Self> + Hash + Eq,
	V: Into<Self>,
	S: BuildHasher,
{
	fn from(value: HashMap<K, V, S>) -> Self {
		Self::Map(
			value
				.into_iter()
				.map(|(k, v)| (k.into(), v.into()))
				.collect(),
		)
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
	fn from(value: Option<T>) -> Self {
		Self::Option(
			value
				.map(Into::into)
				.map(|x| <Option<ValueRequired> as From<Self>>::from(x).unwrap()),
		)
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
macro_rules! array_from {
	($($i:tt)*) => {$(
		impl<T: Data> From<[T; $i]> for Value
		where
			T: Into<Self>
		{
			fn from(value: [T; $i]) -> Self {
				let x: Box<[T]> = Box::new(value);
				let x: List<T> = x.into();
				x.into()
			}
		}
	)*}
}
array!(array_from);
macro_rules! tuple_from {
	($len:tt $($t:ident $i:tt)*) => (
		impl<$($t,)*> From<($($t,)*)> for Value where $($t: Into<Value>,)* {
			#[allow(unused_variables)]
			fn from(value: ($($t,)*)) -> Self {
				Value::Group(Group::new(vec![$(value.$i.into(),)*], None))
			}
		}
	);
}
tuple!(tuple_from);

// Downcast implementations for Value so we can try downcasting it to a specific type if
// we know it.

impl DowncastFrom<Self> for Value {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		Ok(self_)
	}
}
impl DowncastFrom<Value> for bool {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_bool()
	}
}
impl DowncastFrom<Value> for u8 {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_u8()
	}
}
impl DowncastFrom<Value> for i8 {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_i8()
	}
}
impl DowncastFrom<Value> for u16 {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_u16()
	}
}
impl DowncastFrom<Value> for i16 {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_i16()
	}
}
impl DowncastFrom<Value> for u32 {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_u32()
	}
}
impl DowncastFrom<Value> for i32 {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_i32()
	}
}
impl DowncastFrom<Value> for u64 {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_u64()
	}
}
impl DowncastFrom<Value> for i64 {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_i64()
	}
}
impl DowncastFrom<Value> for f32 {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_f32()
	}
}
impl DowncastFrom<Value> for f64 {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_f64()
	}
}
impl DowncastFrom<Value> for Date {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_date()
	}
}
impl DowncastFrom<Value> for DateWithoutTimezone {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_date_without_timezone()
	}
}
impl DowncastFrom<Value> for Time {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_time()
	}
}
impl DowncastFrom<Value> for TimeWithoutTimezone {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_time_without_timezone()
	}
}
impl DowncastFrom<Value> for DateTime {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_date_time()
	}
}
impl DowncastFrom<Value> for DateTimeWithoutTimezone {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_date_time_without_timezone()
	}
}
impl DowncastFrom<Value> for Timezone {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_timezone()
	}
}
impl DowncastFrom<Value> for Decimal {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_decimal()
	}
}
impl DowncastFrom<Value> for Bson {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_bson()
	}
}
impl DowncastFrom<Value> for String {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_string()
	}
}
impl DowncastFrom<Value> for Json {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_json()
	}
}
impl DowncastFrom<Value> for Enum {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_enum()
	}
}
impl DowncastFrom<Value> for Url {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_url()
	}
}
impl DowncastFrom<Value> for Webpage<'static> {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_webpage()
	}
}
impl DowncastFrom<Value> for IpAddr {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_ip_addr()
	}
}
impl<T: Data> DowncastFrom<Value> for List<T>
where
	T: DowncastFrom<Value>,
{
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_
			.into_list()
			.and_then(|list| list.try_map(Downcast::downcast))
	}
}
impl<K, V, S> DowncastFrom<Value> for HashMap<K, V, S>
where
	K: DowncastFrom<Value> + Hash + Eq,
	V: DowncastFrom<Value>,
	S: BuildHasher + Default,
{
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_map().and_then(|map| {
			map.into_iter()
				.map(|(k, v)| Ok((k.downcast()?, v.downcast()?)))
				.collect()
		})
	}
}
impl DowncastFrom<Value> for Group {
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		self_.into_group()
	}
}
impl<T> DowncastFrom<Value> for Option<T>
where
	T: DowncastFrom<Value>,
{
	fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
		match self_.into_option()? {
			Some(t) => t.downcast().map(Some),
			None => Ok(None),
		}
	}
}
macro_rules! array_downcast {
	($($i:tt)*) => {$(
		impl<T: Data> DowncastFrom<Value> for [T; $i]
		where
			T: DowncastFrom<Value>
		{
			fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
				let err = DowncastError {
					from: self_.type_name(),
					to: stringify!([T; $i]),
				};
				let x: Box<[T]> = <List<T>>::downcast_from(self_).map_err(|_| err)?.into_boxed_slice();
				let x: Box<Self> = x.try_into().map_err(|_| err)?;
				Ok(*x)
			}
		}
	)*}
}
array!(array_downcast);
macro_rules! tuple_downcast {
	($len:tt $($t:ident $i:tt)*) => (
		 impl<$($t,)*> DowncastFrom<Value> for ($($t,)*) where $($t: DowncastFrom<Value>,)* {
			fn downcast_from(self_: Value) -> Result<Self, DowncastError> {
				#[allow(unused_mut, unused_variables)]
				let mut fields = self_.into_group()?.into_fields().into_iter();
				if fields.len() != $len {
					return Err(DowncastError{from:"",to:""});
				}
				Ok(($({let _ = $i;fields.next().unwrap().downcast()?},)*))
			}
		}
	);
}
tuple!(tuple_downcast);

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
impl PartialEq<DateWithoutTimezone> for Value {
	fn eq(&self, other: &DateWithoutTimezone) -> bool {
		self.as_date_without_timezone()
			.map(|date_without_timezone| date_without_timezone == other)
			.unwrap_or(false)
	}
}
impl PartialEq<Time> for Value {
	fn eq(&self, other: &Time) -> bool {
		self.as_time().map(|time| time == other).unwrap_or(false)
	}
}
impl PartialEq<TimeWithoutTimezone> for Value {
	fn eq(&self, other: &TimeWithoutTimezone) -> bool {
		self.as_time_without_timezone()
			.map(|time_without_timezone| time_without_timezone == other)
			.unwrap_or(false)
	}
}
impl PartialEq<DateTime> for Value {
	fn eq(&self, other: &DateTime) -> bool {
		self.as_date_time()
			.map(|date_time| date_time == other)
			.unwrap_or(false)
	}
}
impl PartialEq<DateTimeWithoutTimezone> for Value {
	fn eq(&self, other: &DateTimeWithoutTimezone) -> bool {
		self.as_date_time_without_timezone()
			.map(|date_time_without_timezone| date_time_without_timezone == other)
			.unwrap_or(false)
	}
}
impl PartialEq<Timezone> for Value {
	fn eq(&self, other: &Timezone) -> bool {
		self.as_timezone()
			.map(|timezone| timezone == other)
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
impl PartialEq<Url> for Value {
	fn eq(&self, other: &Url) -> bool {
		self.as_url().map(|url| url == other).unwrap_or(false)
	}
}
impl<'a> PartialEq<Webpage<'a>> for Value {
	fn eq(&self, other: &Webpage<'a>) -> bool {
		self.as_webpage()
			.map(|webpage| webpage == other)
			.unwrap_or(false)
	}
}
impl PartialEq<IpAddr> for Value {
	fn eq(&self, other: &IpAddr) -> bool {
		self.as_ip_addr()
			.map(|ip_addr| ip_addr == other)
			.unwrap_or(false)
	}
}
impl<T: Data> PartialEq<List<T>> for Value
where
	Value: PartialEq<T>,
{
	fn eq(&self, other: &List<T>) -> bool {
		self.as_list().map(|list| list == other).unwrap_or(false)
	}
}
impl<K, V, S> PartialEq<HashMap<K, V, S>> for Value
where
	Value: PartialEq<K> + PartialEq<V>,
	K: Hash + Eq + Clone + Into<Value>,
	S: BuildHasher,
{
	fn eq(&self, other: &HashMap<K, V, S>) -> bool {
		self.as_map()
			.map(|map| {
				if map.len() != other.len() {
					return false;
				}

				// This comparison unfortunately requires a bit of a hack. This could be
				// eliminated by ensuring that Value::X hashes identically to X. TODO.
				let other = other
					.iter()
					.map(|(k, v)| (k.clone().into(), v))
					.collect::<HashMap<Self, _>>();

				map.iter()
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
				(Some(a), Some(b)) => match a {
					ValueRequired::Bool(value) => &Value::Bool(*value) == b,
					ValueRequired::U8(value) => &Value::U8(*value) == b,
					ValueRequired::I8(value) => &Value::I8(*value) == b,
					ValueRequired::U16(value) => &Value::U16(*value) == b,
					ValueRequired::I16(value) => &Value::I16(*value) == b,
					ValueRequired::U32(value) => &Value::U32(*value) == b,
					ValueRequired::I32(value) => &Value::I32(*value) == b,
					ValueRequired::U64(value) => &Value::U64(*value) == b,
					ValueRequired::I64(value) => &Value::I64(*value) == b,
					ValueRequired::F32(value) => &Value::F32(*value) == b,
					ValueRequired::F64(value) => &Value::F64(*value) == b,
					ValueRequired::Date(value) => &Value::Date(*value) == b,
					ValueRequired::DateWithoutTimezone(value) => {
						&Value::DateWithoutTimezone(*value) == b
					}
					ValueRequired::Time(value) => &Value::Time(*value) == b,
					ValueRequired::TimeWithoutTimezone(value) => {
						&Value::TimeWithoutTimezone(*value) == b
					}
					ValueRequired::DateTime(value) => &Value::DateTime(*value) == b,
					ValueRequired::DateTimeWithoutTimezone(value) => {
						&Value::DateTimeWithoutTimezone(*value) == b
					}
					ValueRequired::Timezone(value) => &Value::Timezone(*value) == b,
					ValueRequired::Decimal(value) => &Value::Decimal(value.clone()) == b,
					ValueRequired::Bson(value) => &Value::Bson(value.clone()) == b,
					ValueRequired::String(value) => &Value::String(value.clone()) == b,
					ValueRequired::Json(value) => &Value::Json(value.clone()) == b,
					ValueRequired::Enum(value) => &Value::Enum(value.clone()) == b,
					ValueRequired::Url(value) => &Value::Url(value.clone()) == b,
					ValueRequired::Webpage(value) => &Value::Webpage(value.clone()) == b,
					ValueRequired::IpAddr(value) => &Value::IpAddr(*value) == b,
					ValueRequired::List(value) => &Value::List(value.clone()) == b,
					ValueRequired::Map(value) => &Value::Map(value.clone()) == b,
					ValueRequired::Group(value) => &Value::Group(value.clone()) == b,
				},
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

macro_rules! tuple_partialeq {
	($len:tt $($t:ident $i:tt)*) => (
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
tuple!(tuple_partialeq);

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

impl From<Vec<u8>> for List<Value> {
	#[inline(always)]
	fn from(vec: Vec<u8>) -> Self {
		Self::from_(ValueVec::U8(vec))
	}
}
impl FromIterator<u8> for List<Value> {
	#[inline(always)]
	fn from_iter<I: IntoIterator<Item = u8>>(iter: I) -> Self {
		Vec::from_iter(iter).into()
	}
}
impl Extend<u8> for List<Value> {
	#[inline(always)]
	fn extend<I: IntoIterator<Item = u8>>(&mut self, _iter: I) {
		unimplemented!()
	}
}

// #[derive(Clone, Debug)]
// enum Value {
// 	U8(u8),
// 	U16(u16),
// 	List(Box<List<Value>>),
// }
// impl Value {
// 	fn u8(self) -> u8 {
// 		if let Self::U8(ret) = self {
// 			ret
// 		} else {
// 			panic!()
// 		}
// 	}
// 	fn u16(self) -> u16 {
// 		if let Self::U16(ret) = self {
// 			ret
// 		} else {
// 			panic!()
// 		}
// 	}
// 	fn list(self) -> List<Value> {
// 		if let Self::List(ret) = self {
// 			*ret
// 		} else {
// 			panic!()
// 		}
// 	}
// }
#[doc(hidden)]
#[derive(Clone, Debug)]
pub enum ValueType {
	U8,
	U16,
	List,
}
#[doc(hidden)]
#[derive(Clone, Hash, Serialize, Deserialize)]
pub enum ValueVec {
	U8(Vec<u8>),
	U16(Vec<u16>),
	List(Vec<List<Value>>),
	Value(Vec<Value>),
}
impl Debug for ValueVec {
	fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::U8(self_) => fmt.debug_tuple("U8").field(self_).finish(),
			Self::U16(self_) => fmt.debug_tuple("U16").field(self_).finish(),
			Self::List(self_) => fmt.debug_tuple("List").field(self_).finish(),
			Self::Value(self_) => self_.fmt(fmt),
		}
	}
}
impl Data for Value {
	type Vec = ValueVec;
	type DynamicType = ValueType;

	fn new_vec(type_: Self::DynamicType) -> Self::Vec {
		match type_ {
			ValueType::U8 => ValueVec::U8(ListVec::new()),
			ValueType::U16 => ValueVec::U16(ListVec::new()),
			ValueType::List => ValueVec::List(ListVec::new()),
		}
	}
}
impl ListVec<Value> for ValueVec {
	type IntoIter = std::vec::IntoIter<Value>;

	#[inline(always)]
	fn new() -> Self {
		Self::Value(vec![])
	}
	#[inline(always)]
	fn push(&mut self, t: Value) {
		match self {
			ValueVec::U8(list) => list.push(t.into_u8().unwrap()),
			ValueVec::U16(list) => list.push(t.into_u16().unwrap()),
			ValueVec::List(list) => list.push(t.into_list().unwrap()),
			ValueVec::Value(list) => list.push(t),
		}
	}
	fn pop(&mut self) -> Option<Value> {
		match self {
			ValueVec::U8(list) => list.pop().map(Value::U8),
			ValueVec::U16(list) => list.pop().map(Value::U16),
			ValueVec::List(list) => list.pop().map(Value::List),
			ValueVec::Value(list) => list.pop(),
		}
	}
	#[inline(always)]
	fn len(&self) -> usize {
		match self {
			ValueVec::U8(list) => list.len(),
			ValueVec::U16(list) => list.len(),
			ValueVec::List(list) => list.len(),
			ValueVec::Value(list) => list.len(),
		}
	}
	#[inline(always)]
	fn from_vec(vec: Vec<Value>) -> Self {
		Self::Value(vec)
	}
	#[inline(always)]
	fn into_vec(self) -> Vec<Value> {
		match self {
			Self::U8(vec) => vec.map(Into::into),
			Self::U16(vec) => vec.map(Into::into),
			Self::List(vec) => vec.map(Into::into),
			Self::Value(vec) => vec,
		}
	}
	#[inline(always)]
	fn into_iter_a(self) -> Self::IntoIter {
		// TODO
		self.into_vec().into_iter()
	}
	// #[inline(always)]
	// fn iter_a<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Value> + 'a> {
	// 	todo!()
	// 	// Box::new(match self {
	// 	// 	Self::U8(vec) => vec.iter().map(Value::U8),
	// 	// 	Self::U16(vec) => vec.iter().map(Value::U16),
	// 	// 	Self::List(vec) => vec.iter().map(Value::List),
	// 	// 	Self::Value(vec) => vec.iter(),
	// 	// })
	// }
	#[inline(always)]
	fn clone_a(&self) -> Self
	where
		Value: Clone,
	{
		self.clone()
	}
	#[inline(always)]
	fn hash_a<H>(&self, state: &mut H)
	where
		H: Hasher,
		Value: Hash,
	{
		self.hash(state)
	}
	#[inline(always)]
	fn serialize_a<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
		Value: Serialize,
	{
		self.serialize(serializer)
	}
	#[inline(always)]
	fn deserialize_a<'de, D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
		Value: Deserialize<'de>,
		Self: Sized,
	{
		Self::deserialize(deserializer)
	}
	fn fmt_a(&self, fmt: &mut fmt::Formatter) -> fmt::Result
	where
		Value: Debug,
	{
		self.fmt(fmt)
	}
}

// struct Iter<'a, T>(std::slice::Iter<'a, T>);
// impl<'a> Iterator for Iter<'a, u8> {
// 	type Item = ValueRef<'a, Value>;

// 	#[inline(always)]
// 	fn next(&mut self) -> Option<Self::Item> {
// 		self.0
// 			.next()
// 			.map(|next| ValueRef(ManuallyDrop::new(Value::U8(*next)), &()))
// 	}
// }
// impl<'a> Iterator for Iter<'a, u16> {
// 	type Item = ValueRef<'a, Value>;

// 	#[inline(always)]
// 	fn next(&mut self) -> Option<Self::Item> {
// 		self.0
// 			.next()
// 			.map(|next| ValueRef(ManuallyDrop::new(Value::U16(*next)), &()))
// 	}
// }
// impl<'a> Iterator for Iter<'a, List<Value>> {
// 	type Item = ValueRef<'a, Value>;

// 	#[inline(always)]
// 	fn next(&mut self) -> Option<Self::Item> {
// 		self.0.next().map(|next| {
// 			ValueRef(
// 				ManuallyDrop::new(Value::List(Box::new(unsafe { ptr::read(next) }))),
// 				&(),
// 			)
// 		})
// 	}
// }
// impl<'a> Iterator for Iter<'a, Value> {
// 	type Item = ValueRef<'a, Value>;

// 	#[inline(always)]
// 	fn next(&mut self) -> Option<Self::Item> {
// 		self.0
// 			.next()
// 			.map(|next| ValueRef(ManuallyDrop::new(unsafe { ptr::read(next) }), &()))
// 	}
// }

// // TODO! Iterating a List<Value>{vec:ValueVec::List(..)} will leak a load of boxes.

// // impl<'a,T> Drop for ValueRef<'a,T> {
// // 	fn drop(&mut self) {
// // 		if let Some(self_) = try_type_coerce::<&mut ValueRef<'a,T>,&mut ValueRef<'a,Value>>(self) {
// // 			match unsafe { ManuallyDrop::take(&mut self_.0) } {
// // 				Value::Bool(x) => assert_copy(x),
// // 				Value::U8(x) => assert_copy(x),
// // 				Value::I8(x) => assert_copy(x),
// // 				Value::U16(x) => assert_copy(x),
// // 				Value::I16(x) => assert_copy(x),
// // 				Value::U32(x) => assert_copy(x),
// // 				Value::I32(x) => assert_copy(x),
// // 				Value::U64(x) => assert_copy(x),
// // 				Value::I64(x) => assert_copy(x),
// // 				Value::F32(x) => assert_copy(x),
// // 				Value::F64(x) => assert_copy(x),
// // 				Value::Date(x) => assert_copy(x),
// // 				Value::DateWithoutTimezone(x) => assert_copy(x),
// // 				Value::Time(x) => assert_copy(x),
// // 				Value::TimeWithoutTimezone(x) => assert_copy(x),
// // 				Value::DateTime(x) => assert_copy(x),
// // 				Value::DateTimeWithoutTimezone(x) => assert_copy(x),
// // 				Value::Timezone(x) => assert_copy(x),
// // 				Value::Decimal(x) => forget(x),
// // 				Value::Bson(x) => forget(x),
// // 				Value::String(x) => forget(x),
// // 				Value::Json(x) => forget(x),
// // 				Value::Enum(x) => forget(x),
// // 				Value::Url(x) => forget(x),
// // 				Value::Webpage(x) => forget(x),
// // 				Value::IpAddr(x) => assert_copy(x),
// // 				Value::List(x) => forget(*x),
// // 				Value::Map(x) => forget(x),
// // 				Value::Group(x) => forget(x),
// // 				Value::Option(x) => forget(x),
// // 			}
// // 		}
// // 	}
// // }
// // fn assert_copy<T: Copy>(_t: T) {}

// impl<'a, T> Serialize for ValueRef<'a, T>
// where
// 	T: Serialize,
// {
// 	#[inline(always)]
// 	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
// 	where
// 		S: Serializer,
// 	{
// 		(**self).serialize(serializer)
// 	}
// }
// impl<'a, T> AmadeusOrd for ValueRef<'a, T>
// where
// 	T: AmadeusOrd,
// {
// 	#[inline(always)]
// 	fn amadeus_cmp(&self, _other: &Self) -> Ordering {
// 		unimplemented!()
// 	}
// }
