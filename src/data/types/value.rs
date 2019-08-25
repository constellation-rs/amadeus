//! Implement [`Record`] for [`Value`] – an enum representing any valid Parquet value.

use amadeus_parquet::{
	basic::Repetition, column::reader::ColumnReader, errors::ParquetError, record::Record, schema::types::{ColumnPath, Type}
};
use fxhash::FxBuildHasher;
use linked_hash_map::LinkedHashMap;
use serde::{
	de::{MapAccess, SeqAccess, Visitor}, Deserialize, Deserializer, Serialize, Serializer
};
use std::{
	cmp::Ordering, collections::HashMap, convert::TryInto, fmt, hash::{Hash, Hasher}, sync::Arc
};

use super::{
	super::{
		data::{SerdeDeserialize, SerdeSerialize}, Data
	}, Bson, Date, Decimal, Downcast, DowncastError, Enum, Group, Json, List, Map, Time, Timestamp, ValueRequired
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
	use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
			Value::Bool(value) => {
				0u8.hash(state);
				value.hash(state);
			}
			Value::U8(value) => {
				1u8.hash(state);
				value.hash(state);
			}
			Value::I8(value) => {
				2u8.hash(state);
				value.hash(state);
			}
			Value::U16(value) => {
				3u8.hash(state);
				value.hash(state);
			}
			Value::I16(value) => {
				4u8.hash(state);
				value.hash(state);
			}
			Value::U32(value) => {
				5u8.hash(state);
				value.hash(state);
			}
			Value::I32(value) => {
				6u8.hash(state);
				value.hash(state);
			}
			Value::U64(value) => {
				7u8.hash(state);
				value.hash(state);
			}
			Value::I64(value) => {
				8u8.hash(state);
				value.hash(state);
			}
			Value::F32(_value) => {
				9u8.hash(state);
			}
			Value::F64(_value) => {
				10u8.hash(state);
			}
			Value::Date(value) => {
				11u8.hash(state);
				value.hash(state);
			}
			Value::Time(value) => {
				12u8.hash(state);
				value.hash(state);
			}
			Value::Timestamp(value) => {
				13u8.hash(state);
				value.hash(state);
			}
			Value::Decimal(_value) => {
				14u8.hash(state);
			}
			Value::ByteArray(value) => {
				15u8.hash(state);
				value.hash(state);
			}
			Value::Bson(value) => {
				16u8.hash(state);
				value.hash(state);
			}
			Value::String(value) => {
				17u8.hash(state);
				value.hash(state);
			}
			Value::Json(value) => {
				18u8.hash(state);
				value.hash(state);
			}
			Value::Enum(value) => {
				19u8.hash(state);
				value.hash(state);
			}
			Value::List(value) => {
				20u8.hash(state);
				value.hash(state);
			}
			Value::Map(_value) => {
				21u8.hash(state);
			}
			Value::Group(_value) => {
				22u8.hash(state);
			}
			Value::Option(value) => {
				23u8.hash(state);
				value.hash(state);
			}
		}
	}
}
impl Eq for Value {}
impl PartialOrd for Value {
	fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
		None
	}
}

impl Value {
	fn type_name(&self) -> &'static str {
		match self {
			Value::Bool(value) => "bool",
			Value::U8(value) => "u8",
			Value::I8(value) => "i8",
			Value::U16(value) => "u16",
			Value::I16(value) => "i16",
			Value::U32(value) => "u32",
			Value::I32(value) => "i32",
			Value::U64(value) => "u64",
			Value::I64(value) => "i64",
			Value::F32(_value) => "f32",
			Value::F64(_value) => "f64",
			Value::Date(value) => "date",
			Value::Time(value) => "time",
			Value::Timestamp(value) => "timestamp",
			Value::Decimal(_value) => "decimal",
			Value::ByteArray(value) => "byte_array",
			Value::Bson(value) => "bson",
			Value::String(value) => "string",
			Value::Json(value) => "json",
			Value::Enum(value) => "enum",
			Value::List(value) => "list",
			Value::Map(_value) => "map",
			Value::Group(_value) => "group",
			Value::Option(value) => "option",
		}
	}

	/// Returns true if the `Value` is an Bool. Returns false otherwise.
	pub fn is_bool(&self) -> bool {
		if let Value::Bool(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Bool, return a reference to it. Returns Err otherwise.
	pub fn as_bool(&self) -> Result<bool, DowncastError> {
		if let Value::Bool(ret) = self {
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
		if let Value::Bool(ret) = self {
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
		if let Value::U8(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an U8, return a reference to it. Returns Err otherwise.
	pub fn as_u8(&self) -> Result<u8, DowncastError> {
		if let Value::U8(ret) = self {
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
		if let Value::U8(ret) = self {
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
		if let Value::I8(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an I8, return a reference to it. Returns Err otherwise.
	pub fn as_i8(&self) -> Result<i8, DowncastError> {
		if let Value::I8(ret) = self {
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
		if let Value::I8(ret) = self {
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
		if let Value::U16(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an U16, return a reference to it. Returns Err otherwise.
	pub fn as_u16(&self) -> Result<u16, DowncastError> {
		if let Value::U16(ret) = self {
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
		if let Value::U16(ret) = self {
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
		if let Value::I16(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an I16, return a reference to it. Returns Err otherwise.
	pub fn as_i16(&self) -> Result<i16, DowncastError> {
		if let Value::I16(ret) = self {
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
		if let Value::I16(ret) = self {
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
		if let Value::U32(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an U32, return a reference to it. Returns Err otherwise.
	pub fn as_u32(&self) -> Result<u32, DowncastError> {
		if let Value::U32(ret) = self {
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
		if let Value::U32(ret) = self {
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
		if let Value::I32(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an I32, return a reference to it. Returns Err otherwise.
	pub fn as_i32(&self) -> Result<i32, DowncastError> {
		if let Value::I32(ret) = self {
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
		if let Value::I32(ret) = self {
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
		if let Value::U64(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an U64, return a reference to it. Returns Err otherwise.
	pub fn as_u64(&self) -> Result<u64, DowncastError> {
		if let Value::U64(ret) = self {
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
		if let Value::U64(ret) = self {
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
		if let Value::I64(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an I64, return a reference to it. Returns Err otherwise.
	pub fn as_i64(&self) -> Result<i64, DowncastError> {
		if let Value::I64(ret) = self {
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
		if let Value::I64(ret) = self {
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
		if let Value::F32(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an F32, return a reference to it. Returns Err otherwise.
	pub fn as_f32(&self) -> Result<f32, DowncastError> {
		if let Value::F32(ret) = self {
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
		if let Value::F32(ret) = self {
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
		if let Value::F64(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an F64, return a reference to it. Returns Err otherwise.
	pub fn as_f64(&self) -> Result<f64, DowncastError> {
		if let Value::F64(ret) = self {
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
		if let Value::F64(ret) = self {
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
		if let Value::Date(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Date, return a reference to it. Returns Err otherwise.
	pub fn as_date(&self) -> Result<&Date, DowncastError> {
		if let Value::Date(ret) = self {
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
		if let Value::Date(ret) = self {
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
		if let Value::Time(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Time, return a reference to it. Returns Err otherwise.
	pub fn as_time(&self) -> Result<&Time, DowncastError> {
		if let Value::Time(ret) = self {
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
		if let Value::Time(ret) = self {
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
		if let Value::Timestamp(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Timestamp, return a reference to it. Returns Err otherwise.
	pub fn as_timestamp(&self) -> Result<&Timestamp, DowncastError> {
		if let Value::Timestamp(ret) = self {
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
		if let Value::Timestamp(ret) = self {
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
		if let Value::Decimal(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Decimal, return a reference to it. Returns Err otherwise.
	pub fn as_decimal(&self) -> Result<&Decimal, DowncastError> {
		if let Value::Decimal(ret) = self {
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
		if let Value::Decimal(ret) = self {
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
		if let Value::ByteArray(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an ByteArray, return a reference to it. Returns Err otherwise.
	pub fn as_byte_array(&self) -> Result<&Vec<u8>, DowncastError> {
		if let Value::ByteArray(ret) = self {
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
		if let Value::ByteArray(ret) = self {
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
		if let Value::Bson(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Bson, return a reference to it. Returns Err otherwise.
	pub fn as_bson(&self) -> Result<&Bson, DowncastError> {
		if let Value::Bson(ret) = self {
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
		if let Value::Bson(ret) = self {
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
		if let Value::String(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an String, return a reference to it. Returns Err otherwise.
	pub fn as_string(&self) -> Result<&String, DowncastError> {
		if let Value::String(ret) = self {
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
		if let Value::String(ret) = self {
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
		if let Value::Json(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Json, return a reference to it. Returns Err otherwise.
	pub fn as_json(&self) -> Result<&Json, DowncastError> {
		if let Value::Json(ret) = self {
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
		if let Value::Json(ret) = self {
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
		if let Value::Enum(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Enum, return a reference to it. Returns Err otherwise.
	pub fn as_enum(&self) -> Result<&Enum, DowncastError> {
		if let Value::Enum(ret) = self {
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
		if let Value::Enum(ret) = self {
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
		if let Value::List(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an List, return a reference to it. Returns Err otherwise.
	pub fn as_list(&self) -> Result<&List<Value>, DowncastError> {
		if let Value::List(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "list",
			})
		}
	}

	/// If the `Value` is an List, return it. Returns Err otherwise.
	pub fn into_list(self) -> Result<List<Value>, DowncastError> {
		if let Value::List(ret) = self {
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
		if let Value::Map(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Map, return a reference to it. Returns Err otherwise.
	pub fn as_map(&self) -> Result<&Map<Value, Value>, DowncastError> {
		if let Value::Map(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "map",
			})
		}
	}

	/// If the `Value` is an Map, return it. Returns Err otherwise.
	pub fn into_map(self) -> Result<Map<Value, Value>, DowncastError> {
		if let Value::Map(ret) = self {
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
		if let Value::Group(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Group, return a reference to it. Returns Err otherwise.
	pub fn as_group(&self) -> Result<&Group, DowncastError> {
		if let Value::Group(ret) = self {
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
		if let Value::Group(ret) = self {
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
		if let Value::Option(_) = self {
			true
		} else {
			false
		}
	}

	/// If the `Value` is an Option, return a reference to it. Returns Err otherwise.
	fn as_option(&self) -> Result<&Option<ValueRequired>, DowncastError> {
		if let Value::Option(ret) = self {
			Ok(ret)
		} else {
			Err(DowncastError {
				from: self.type_name(),
				to: "option",
			})
		}
	}

	/// If the `Value` is an Option, return it. Returns Err otherwise.
	pub fn into_option(self) -> Result<Option<Value>, DowncastError> {
		if let Value::Option(ret) = self {
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
		Value::Bool(value)
	}
}
impl From<u8> for Value {
	fn from(value: u8) -> Self {
		Value::U8(value)
	}
}
impl From<i8> for Value {
	fn from(value: i8) -> Self {
		Value::I8(value)
	}
}
impl From<u16> for Value {
	fn from(value: u16) -> Self {
		Value::U16(value)
	}
}
impl From<i16> for Value {
	fn from(value: i16) -> Self {
		Value::I16(value)
	}
}
impl From<u32> for Value {
	fn from(value: u32) -> Self {
		Value::U32(value)
	}
}
impl From<i32> for Value {
	fn from(value: i32) -> Self {
		Value::I32(value)
	}
}
impl From<u64> for Value {
	fn from(value: u64) -> Self {
		Value::U64(value)
	}
}
impl From<i64> for Value {
	fn from(value: i64) -> Self {
		Value::I64(value)
	}
}
impl From<f32> for Value {
	fn from(value: f32) -> Self {
		Value::F32(value)
	}
}
impl From<f64> for Value {
	fn from(value: f64) -> Self {
		Value::F64(value)
	}
}
impl From<Date> for Value {
	fn from(value: Date) -> Self {
		Value::Date(value)
	}
}
impl From<Time> for Value {
	fn from(value: Time) -> Self {
		Value::Time(value)
	}
}
impl From<Timestamp> for Value {
	fn from(value: Timestamp) -> Self {
		Value::Timestamp(value)
	}
}
impl From<Decimal> for Value {
	fn from(value: Decimal) -> Self {
		Value::Decimal(value)
	}
}
impl From<Vec<u8>> for Value {
	fn from(value: Vec<u8>) -> Self {
		Value::ByteArray(value)
	}
}
impl From<Bson> for Value {
	fn from(value: Bson) -> Self {
		Value::Bson(value)
	}
}
impl From<String> for Value {
	fn from(value: String) -> Self {
		Value::String(value)
	}
}
impl From<Json> for Value {
	fn from(value: Json) -> Self {
		Value::Json(value)
	}
}
impl From<Enum> for Value {
	fn from(value: Enum) -> Self {
		Value::Enum(value)
	}
}
impl<T> From<List<T>> for Value
where
	Value: From<T>,
{
	default fn from(value: List<T>) -> Self {
		Value::List(List(value.0.into_iter().map(Into::into).collect()))
	}
}
impl From<List<Value>> for Value {
	fn from(value: List<Value>) -> Self {
		Value::List(value)
	}
}
impl<K, V> From<Map<K, V>> for Value
where
	Value: From<K> + From<V>,
	K: Hash + Eq,
{
	default fn from(value: Map<K, V>) -> Self {
		Value::Map(Map(value
			.0
			.into_iter()
			.map(|(k, v)| (k.into(), v.into()))
			.collect()))
	}
}
impl From<Map<Value, Value>> for Value {
	fn from(value: Map<Value, Value>) -> Self {
		Value::Map(value)
	}
}
impl From<Group> for Value {
	fn from(value: Group) -> Self {
		Value::Group(value)
	}
}
impl<T> From<Option<T>> for Value
where
	Value: From<T>,
{
	default fn from(value: Option<T>) -> Self {
		Value::Option(
			value
				.map(Into::into)
				.map(|x| <Option<ValueRequired> as From<Value>>::from(x).unwrap()),
		)
	}
}
impl From<Option<Value>> for Value {
	fn from(value: Option<Value>) -> Self {
		Value::Option(value.map(|x| <Option<ValueRequired>>::from(x).unwrap()))
	}
}

// Downcast implementations for Value so we can try downcasting it to a specific type if
// we know it.

impl Downcast<Value> for Value {
	fn downcast(self) -> Result<Value, DowncastError> {
		Ok(self)
	}
}
impl Downcast<bool> for Value {
	fn downcast(self) -> Result<bool, DowncastError> {
		self.into_bool()
	}
}
impl Downcast<u8> for Value {
	fn downcast(self) -> Result<u8, DowncastError> {
		self.into_u8()
	}
}
impl Downcast<i8> for Value {
	fn downcast(self) -> Result<i8, DowncastError> {
		self.into_i8()
	}
}
impl Downcast<u16> for Value {
	fn downcast(self) -> Result<u16, DowncastError> {
		self.into_u16()
	}
}
impl Downcast<i16> for Value {
	fn downcast(self) -> Result<i16, DowncastError> {
		self.into_i16()
	}
}
impl Downcast<u32> for Value {
	fn downcast(self) -> Result<u32, DowncastError> {
		self.into_u32()
	}
}
impl Downcast<i32> for Value {
	fn downcast(self) -> Result<i32, DowncastError> {
		self.into_i32()
	}
}
impl Downcast<u64> for Value {
	fn downcast(self) -> Result<u64, DowncastError> {
		self.into_u64()
	}
}
impl Downcast<i64> for Value {
	fn downcast(self) -> Result<i64, DowncastError> {
		self.into_i64()
	}
}
impl Downcast<f32> for Value {
	fn downcast(self) -> Result<f32, DowncastError> {
		self.into_f32()
	}
}
impl Downcast<f64> for Value {
	fn downcast(self) -> Result<f64, DowncastError> {
		self.into_f64()
	}
}
impl Downcast<Date> for Value {
	fn downcast(self) -> Result<Date, DowncastError> {
		self.into_date()
	}
}
impl Downcast<Time> for Value {
	fn downcast(self) -> Result<Time, DowncastError> {
		self.into_time()
	}
}
impl Downcast<Timestamp> for Value {
	fn downcast(self) -> Result<Timestamp, DowncastError> {
		self.into_timestamp()
	}
}
impl Downcast<Decimal> for Value {
	fn downcast(self) -> Result<Decimal, DowncastError> {
		self.into_decimal()
	}
}
impl Downcast<Vec<u8>> for Value {
	fn downcast(self) -> Result<Vec<u8>, DowncastError> {
		self.into_byte_array()
	}
}
impl Downcast<Bson> for Value {
	fn downcast(self) -> Result<Bson, DowncastError> {
		self.into_bson()
	}
}
impl Downcast<String> for Value {
	fn downcast(self) -> Result<String, DowncastError> {
		self.into_string()
	}
}
impl Downcast<Json> for Value {
	fn downcast(self) -> Result<Json, DowncastError> {
		self.into_json()
	}
}
impl Downcast<Enum> for Value {
	fn downcast(self) -> Result<Enum, DowncastError> {
		self.into_enum()
	}
}
impl<T> Downcast<List<T>> for Value
where
	Value: Downcast<T>,
{
	default fn downcast(self) -> Result<List<T>, DowncastError> {
		self.into_list().and_then(|list| {
			list.0
				.into_iter()
				.map(Downcast::downcast)
				.collect::<Result<Vec<_>, _>>()
				.map(List)
		})
	}
}
impl Downcast<List<Value>> for Value {
	fn downcast(self) -> Result<List<Value>, DowncastError> {
		self.into_list()
	}
}
impl<K, V> Downcast<Map<K, V>> for Value
where
	Value: Downcast<K> + Downcast<V>,
	K: Hash + Eq,
{
	default fn downcast(self) -> Result<Map<K, V>, DowncastError> {
		self.into_map().and_then(|map| {
			map.0
				.into_iter()
				.map(|(k, v)| Ok((k.downcast()?, v.downcast()?)))
				.collect::<Result<HashMap<_, _>, _>>()
				.map(Map)
		})
	}
}
impl Downcast<Map<Value, Value>> for Value {
	fn downcast(self) -> Result<Map<Value, Value>, DowncastError> {
		self.into_map()
	}
}
impl Downcast<Group> for Value {
	fn downcast(self) -> Result<Group, DowncastError> {
		self.into_group()
	}
}
impl<T> Downcast<Option<T>> for Value
where
	Value: Downcast<T>,
{
	default fn downcast(self) -> Result<Option<T>, DowncastError> {
		match self.into_option()? {
			Some(t) => Downcast::<T>::downcast(t).map(Some),
			None => Ok(None),
		}
	}
}
impl Downcast<Option<Value>> for Value {
	fn downcast(self) -> Result<Option<Value>, DowncastError> {
		self.into_option()
	}
}

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
					.collect::<HashMap<Value, _>>();

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

impl Data for Value {
	type ParquetReader = ValueReader<<amadeus_parquet::record::types::Value as Record>::Reader>;
	type ParquetSchema = <amadeus_parquet::record::types::Value as Record>::Schema;

	fn postgres_query(
		f: &mut fmt::Formatter, name: Option<&crate::source::postgres::Names<'_>>,
	) -> fmt::Result {
		unimplemented!()
	}
	fn postgres_decode(
		type_: &::postgres::types::Type, buf: Option<&[u8]>,
	) -> Result<Self, Box<std::error::Error + Sync + Send>> {
		unimplemented!()
	}

	fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		match self {
			Value::Bool(value) => value.serde_serialize(serializer),
			Value::U8(value) => value.serde_serialize(serializer),
			Value::I8(value) => value.serde_serialize(serializer),
			Value::U16(value) => value.serde_serialize(serializer),
			Value::I16(value) => value.serde_serialize(serializer),
			Value::U32(value) => value.serde_serialize(serializer),
			Value::I32(value) => value.serde_serialize(serializer),
			Value::U64(value) => value.serde_serialize(serializer),
			Value::I64(value) => value.serde_serialize(serializer),
			Value::F32(value) => value.serde_serialize(serializer),
			Value::F64(value) => value.serde_serialize(serializer),
			Value::Date(value) => value.serde_serialize(serializer),
			Value::Time(value) => value.serde_serialize(serializer),
			Value::Timestamp(value) => value.serde_serialize(serializer),
			Value::Decimal(value) => value.serde_serialize(serializer),
			Value::ByteArray(value) => value.serde_serialize(serializer),
			Value::Bson(value) => value.serde_serialize(serializer),
			Value::String(value) => value.serde_serialize(serializer),
			Value::Json(value) => value.serde_serialize(serializer),
			Value::Enum(value) => value.serde_serialize(serializer),
			Value::List(value) => value.serde_serialize(serializer),
			Value::Map(value) => value.serde_serialize(serializer),
			Value::Group(value) => value.serde_serialize(serializer),
			Value::Option(value) => match value {
				Some(value) => match value {
					ValueRequired::Bool(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::U8(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::I8(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::U16(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::I16(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::U32(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::I32(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::U64(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::I64(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::F32(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::F64(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::Date(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::Time(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::Timestamp(value) => {
						serializer.serialize_some(&SerdeSerialize(value))
					}
					ValueRequired::Decimal(value) => {
						serializer.serialize_some(&SerdeSerialize(value))
					}
					ValueRequired::ByteArray(value) => {
						serializer.serialize_some(&SerdeSerialize(value))
					}
					ValueRequired::Bson(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::String(value) => {
						serializer.serialize_some(&SerdeSerialize(value))
					}
					ValueRequired::Json(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::Enum(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::List(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::Map(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::Group(value) => {
						serializer.serialize_some(&SerdeSerialize(value))
					}
				},
				None => serializer.serialize_none(),
			},
		}
	}
	fn serde_deserialize<'de, D>(
		deserializer: D, schema: Option<SchemaIncomplete>,
	) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		if let Some(schema) = schema {
			assert_eq!(schema, SchemaIncomplete::Group(None)); // TODO
			return Group::serde_deserialize(deserializer, Some(schema)).map(Value::Group);
		}
		struct ValueVisitor;

		impl<'de> Visitor<'de> for ValueVisitor {
			type Value = Value;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("any valid Amadeus value")
			}

			#[inline]
			fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
				Ok(Value::Bool(value))
			}

			#[inline]
			fn visit_u8<E>(self, value: u8) -> Result<Self::Value, E> {
				Ok(Value::U8(value))
			}
			#[inline]
			fn visit_i8<E>(self, value: i8) -> Result<Self::Value, E> {
				Ok(Value::I8(value))
			}
			#[inline]
			fn visit_u16<E>(self, value: u16) -> Result<Self::Value, E> {
				Ok(Value::U16(value))
			}
			#[inline]
			fn visit_i16<E>(self, value: i16) -> Result<Self::Value, E> {
				Ok(Value::I16(value))
			}
			#[inline]
			fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E> {
				Ok(Value::U32(value))
			}
			#[inline]
			fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E> {
				Ok(Value::I32(value))
			}
			#[inline]
			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(Value::U64(value))
			}
			#[inline]
			fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
				Ok(Value::I64(value))
			}

			#[inline]
			fn visit_f32<E>(self, value: f32) -> Result<Self::Value, E> {
				Ok(Value::F32(value))
			}
			#[inline]
			fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
				Ok(Value::F64(value))
			}

			#[inline]
			fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				self.visit_string(String::from(value))
			}

			#[inline]
			fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
				Ok(Value::String(value))
			}

			#[inline]
			fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E> {
				Ok(Value::ByteArray(v.to_owned()))
			}

			#[inline]
			fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E> {
				Ok(Value::ByteArray(v))
			}

			#[inline]
			fn visit_none<E>(self) -> Result<Self::Value, E> {
				Ok(Value::Option(None))
			}

			#[inline]
			fn visit_unit<E>(self) -> Result<Self::Value, E> {
				Ok(Value::Option(None))
			}

			#[inline]
			fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
			where
				D: serde::Deserializer<'de>,
			{
				Deserialize::deserialize(deserializer).map(|value: SerdeDeserialize<Value>| {
					Value::Option(Some(<Option<ValueRequired>>::from(value.0).unwrap()))
				})
			}

			#[inline]
			fn visit_seq<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
			where
				V: SeqAccess<'de>,
			{
				let mut vec = Vec::with_capacity(visitor.size_hint().unwrap_or(0));

				while let Some(elem) = visitor.next_element::<SerdeDeserialize<Value>>()? {
					vec.push(elem.0);
				}

				Ok(Value::List(List(vec)))
			}

			fn visit_map<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
			where
				V: MapAccess<'de>,
			{
				let mut values = HashMap::with_capacity(visitor.size_hint().unwrap_or(0));
				while let Some((key, value)) =
					visitor.next_entry::<SerdeDeserialize<Value>, SerdeDeserialize<Value>>()?
				{
					values.insert(key.0, value.0);
				}

				Ok(Value::Map(Map(values)))
			}
		}

		deserializer.deserialize_any(ValueVisitor)
	}

	fn parquet_parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::ParquetSchema), ParquetError> {
		amadeus_parquet::record::types::Value::parse(schema, repetition)
	}
	fn parquet_reader(
		schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::ParquetReader {
		ValueReader(amadeus_parquet::record::types::Value::reader(
			schema, path, def_level, rep_level, paths, batch_size,
		))
	}
}

impl From<amadeus_parquet::record::types::Value> for Value {
	fn from(value: amadeus_parquet::record::types::Value) -> Self {
		match value {
			amadeus_parquet::record::types::Value::Bool(value) => Value::Bool(value),
			amadeus_parquet::record::types::Value::U8(value) => Value::U8(value),
			amadeus_parquet::record::types::Value::I8(value) => Value::I8(value),
			amadeus_parquet::record::types::Value::U16(value) => Value::U16(value),
			amadeus_parquet::record::types::Value::I16(value) => Value::I16(value),
			amadeus_parquet::record::types::Value::U32(value) => Value::U32(value),
			amadeus_parquet::record::types::Value::I32(value) => Value::I32(value),
			amadeus_parquet::record::types::Value::U64(value) => Value::U64(value),
			amadeus_parquet::record::types::Value::I64(value) => Value::I64(value),
			amadeus_parquet::record::types::Value::F32(value) => Value::F32(value),
			amadeus_parquet::record::types::Value::F64(value) => Value::F64(value),
			amadeus_parquet::record::types::Value::Date(value) => Value::Date(value.into()),
			amadeus_parquet::record::types::Value::Time(value) => Value::Time(value.into()),
			amadeus_parquet::record::types::Value::Timestamp(value) => {
				Value::Timestamp(value.into())
			}
			amadeus_parquet::record::types::Value::Decimal(value) => Value::Decimal(value.into()),
			amadeus_parquet::record::types::Value::ByteArray(value) => {
				Value::ByteArray(value.into())
			}
			amadeus_parquet::record::types::Value::Bson(value) => Value::Bson(value.into()),
			amadeus_parquet::record::types::Value::String(value) => Value::String(value.into()),
			amadeus_parquet::record::types::Value::Json(value) => Value::Json(value.into()),
			amadeus_parquet::record::types::Value::Enum(value) => Value::Enum(value.into()),
			amadeus_parquet::record::types::Value::List(value) => Value::List(value.into()),
			amadeus_parquet::record::types::Value::Map(value) => Value::Map(value.into()),
			amadeus_parquet::record::types::Value::Group(value) => Value::Group(value.into()),
			amadeus_parquet::record::types::Value::Option(value) => {
				Value::Option(value.map(|value| match value {
					amadeus_parquet::record::types::ValueRequired::Bool(value) => {
						ValueRequired::Bool(value)
					}
					amadeus_parquet::record::types::ValueRequired::U8(value) => {
						ValueRequired::U8(value)
					}
					amadeus_parquet::record::types::ValueRequired::I8(value) => {
						ValueRequired::I8(value)
					}
					amadeus_parquet::record::types::ValueRequired::U16(value) => {
						ValueRequired::U16(value)
					}
					amadeus_parquet::record::types::ValueRequired::I16(value) => {
						ValueRequired::I16(value)
					}
					amadeus_parquet::record::types::ValueRequired::U32(value) => {
						ValueRequired::U32(value)
					}
					amadeus_parquet::record::types::ValueRequired::I32(value) => {
						ValueRequired::I32(value)
					}
					amadeus_parquet::record::types::ValueRequired::U64(value) => {
						ValueRequired::U64(value)
					}
					amadeus_parquet::record::types::ValueRequired::I64(value) => {
						ValueRequired::I64(value)
					}
					amadeus_parquet::record::types::ValueRequired::F32(value) => {
						ValueRequired::F32(value)
					}
					amadeus_parquet::record::types::ValueRequired::F64(value) => {
						ValueRequired::F64(value)
					}
					amadeus_parquet::record::types::ValueRequired::Date(value) => {
						ValueRequired::Date(value.into())
					}
					amadeus_parquet::record::types::ValueRequired::Time(value) => {
						ValueRequired::Time(value.into())
					}
					amadeus_parquet::record::types::ValueRequired::Timestamp(value) => {
						ValueRequired::Timestamp(value.into())
					}
					amadeus_parquet::record::types::ValueRequired::Decimal(value) => {
						ValueRequired::Decimal(value.into())
					}
					amadeus_parquet::record::types::ValueRequired::ByteArray(value) => {
						ValueRequired::ByteArray(value.into())
					}
					amadeus_parquet::record::types::ValueRequired::Bson(value) => {
						ValueRequired::Bson(value.into())
					}
					amadeus_parquet::record::types::ValueRequired::String(value) => {
						ValueRequired::String(value.into())
					}
					amadeus_parquet::record::types::ValueRequired::Json(value) => {
						ValueRequired::Json(value.into())
					}
					amadeus_parquet::record::types::ValueRequired::Enum(value) => {
						ValueRequired::Enum(value.into())
					}
					amadeus_parquet::record::types::ValueRequired::List(value) => {
						ValueRequired::List(value.into())
					}
					amadeus_parquet::record::types::ValueRequired::Map(value) => {
						ValueRequired::Map(value.into())
					}
					amadeus_parquet::record::types::ValueRequired::Group(value) => {
						ValueRequired::Group(value.into())
					}
				}))
			}
		}
	}
}

/// A Reader that wraps a Reader, wrapping the read value in a `Record`.
pub struct ValueReader<T>(T);
impl<T> amadeus_parquet::record::Reader for ValueReader<T>
where
	T: amadeus_parquet::record::Reader<Item = amadeus_parquet::record::types::Value>,
{
	type Item = Value;

	#[inline]
	fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item, ParquetError> {
		self.0.read(def_level, rep_level).map(Into::into)
	}

	#[inline]
	fn advance_columns(&mut self) -> Result<(), ParquetError> {
		self.0.advance_columns()
	}

	#[inline]
	fn has_next(&self) -> bool {
		self.0.has_next()
	}

	#[inline]
	fn current_def_level(&self) -> i16 {
		self.0.current_def_level()
	}

	#[inline]
	fn current_rep_level(&self) -> i16 {
		self.0.current_rep_level()
	}
}
