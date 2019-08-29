//! Implement [`Record`] for [`ValueRequired`] – an enum representing any valid required
//! Parquet value.

use std::{
	cmp::Ordering, hash::{Hash, Hasher}
};

use super::{Bson, Date, Decimal, Enum, Group, Json, List, Map, Time, Timestamp, Value};

/// Represents any valid required Parquet value. Exists to avoid [`Value`] being recursive
/// and thus infinitely sized.
#[derive(Clone, PartialEq, Debug)]
pub enum ValueRequired {
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
}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for ValueRequired {
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
		}
	}
}
impl Eq for ValueRequired {}
impl PartialOrd for ValueRequired {
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
			_ => None,
		}
	}
}

impl ValueRequired {
	pub(crate) fn eq<T>(&self, other: &T) -> bool
	where
		Value: PartialEq<T>,
	{
		let self_ = unsafe { std::ptr::read(self) };
		let self_: Value = self_.into();
		let ret = &self_ == other;
		std::mem::forget(self_);
		ret
	}
	// pub(crate) fn as_value<F, O>(&self, f: F) -> O
	// where
	// 	F: FnOnce(&Value) -> O,
	// {
	// 	let self_ = unsafe { std::ptr::read(self) };
	// 	let self_: ManuallyDrop<Value> = ManuallyDrop::new(self_.into());
	// 	f(&self_)
	// }
}

impl From<ValueRequired> for Value {
	fn from(value: ValueRequired) -> Self {
		match value {
			ValueRequired::Bool(value) => Self::Bool(value),
			ValueRequired::U8(value) => Self::U8(value),
			ValueRequired::I8(value) => Self::I8(value),
			ValueRequired::U16(value) => Self::U16(value),
			ValueRequired::I16(value) => Self::I16(value),
			ValueRequired::U32(value) => Self::U32(value),
			ValueRequired::I32(value) => Self::I32(value),
			ValueRequired::U64(value) => Self::U64(value),
			ValueRequired::I64(value) => Self::I64(value),
			ValueRequired::F32(value) => Self::F32(value),
			ValueRequired::F64(value) => Self::F64(value),
			ValueRequired::Date(value) => Self::Date(value),
			ValueRequired::Time(value) => Self::Time(value),
			ValueRequired::Timestamp(value) => Self::Timestamp(value),
			ValueRequired::Decimal(value) => Self::Decimal(value),
			ValueRequired::ByteArray(value) => Self::ByteArray(value),
			ValueRequired::Bson(value) => Self::Bson(value),
			ValueRequired::String(value) => Self::String(value),
			ValueRequired::Json(value) => Self::Json(value),
			ValueRequired::Enum(value) => Self::Enum(value),
			ValueRequired::List(value) => Self::List(value),
			ValueRequired::Map(value) => Self::Map(value),
			ValueRequired::Group(value) => Self::Group(value),
		}
	}
}

impl From<Value> for Option<ValueRequired> {
	fn from(value: Value) -> Self {
		Some(match value {
			Value::Bool(value) => ValueRequired::Bool(value),
			Value::U8(value) => ValueRequired::U8(value),
			Value::I8(value) => ValueRequired::I8(value),
			Value::U16(value) => ValueRequired::U16(value),
			Value::I16(value) => ValueRequired::I16(value),
			Value::U32(value) => ValueRequired::U32(value),
			Value::I32(value) => ValueRequired::I32(value),
			Value::U64(value) => ValueRequired::U64(value),
			Value::I64(value) => ValueRequired::I64(value),
			Value::F32(value) => ValueRequired::F32(value),
			Value::F64(value) => ValueRequired::F64(value),
			Value::Date(value) => ValueRequired::Date(value),
			Value::Time(value) => ValueRequired::Time(value),
			Value::Timestamp(value) => ValueRequired::Timestamp(value),
			Value::Decimal(value) => ValueRequired::Decimal(value),
			Value::ByteArray(value) => ValueRequired::ByteArray(value),
			Value::Bson(value) => ValueRequired::Bson(value),
			Value::String(value) => ValueRequired::String(value),
			Value::Json(value) => ValueRequired::Json(value),
			Value::Enum(value) => ValueRequired::Enum(value),
			Value::List(value) => ValueRequired::List(value),
			Value::Map(value) => ValueRequired::Map(value),
			Value::Group(value) => ValueRequired::Group(value),
			Value::Option(_) => return None,
		})
	}
}

// impl Serialize for ValueRequired {
// 	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
// 	where
// 		S: Serializer,
// 	{
// 		match self {
// 			ValueRequired::Bool(value) => value.serialize(serializer),
// 			ValueRequired::U8(value) => value.serialize(serializer),
// 			ValueRequired::I8(value) => value.serialize(serializer),
// 			ValueRequired::U16(value) => value.serialize(serializer),
// 			ValueRequired::I16(value) => value.serialize(serializer),
// 			ValueRequired::U32(value) => value.serialize(serializer),
// 			ValueRequired::I32(value) => value.serialize(serializer),
// 			ValueRequired::U64(value) => value.serialize(serializer),
// 			ValueRequired::I64(value) => value.serialize(serializer),
// 			ValueRequired::F32(value) => value.serialize(serializer),
// 			ValueRequired::F64(value) => value.serialize(serializer),
// 			ValueRequired::Date(value) => value.serialize(serializer),
// 			ValueRequired::Time(value) => value.serialize(serializer),
// 			ValueRequired::Timestamp(value) => value.serialize(serializer),
// 			ValueRequired::Decimal(value) => value.serialize(serializer),
// 			ValueRequired::ByteArray(value) => value.serialize(serializer),
// 			ValueRequired::Bson(value) => value.serialize(serializer),
// 			ValueRequired::String(value) => value.serialize(serializer),
// 			ValueRequired::Json(value) => value.serialize(serializer),
// 			ValueRequired::Enum(value) => value.serialize(serializer),
// 			ValueRequired::List(value) => value.serialize(serializer),
// 			ValueRequired::Map(value) => value.serialize(serializer),
// 			ValueRequired::Group(value) => value.serialize(serializer),
// 		}
// 	}
// }
// impl<'de> Deserialize<'de> for ValueRequired {
// 	fn deserialize<D>(deserializer: D) -> Result<ValueRequired, D::Error>
// 	where
// 		D: Deserializer<'de>,
// 	{
// 		struct ValueVisitor;

// 		impl<'de> Visitor<'de> for ValueVisitor {
// 			type Value = ValueRequired;

// 			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
// 				formatter.write_str("any valid Amadeus value")
// 			}

// 			#[inline]
// 			fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
// 				Ok(ValueRequired::Bool(value))
// 			}

// 			#[inline]
// 			fn visit_u8<E>(self, value: u8) -> Result<Self::Value, E> {
// 				Ok(ValueRequired::U8(value))
// 			}
// 			#[inline]
// 			fn visit_i8<E>(self, value: i8) -> Result<Self::Value, E> {
// 				Ok(ValueRequired::I8(value))
// 			}
// 			#[inline]
// 			fn visit_u16<E>(self, value: u16) -> Result<Self::Value, E> {
// 				Ok(ValueRequired::U16(value))
// 			}
// 			#[inline]
// 			fn visit_i16<E>(self, value: i16) -> Result<Self::Value, E> {
// 				Ok(ValueRequired::I16(value))
// 			}
// 			#[inline]
// 			fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E> {
// 				Ok(ValueRequired::U32(value))
// 			}
// 			#[inline]
// 			fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E> {
// 				Ok(ValueRequired::I32(value))
// 			}
// 			#[inline]
// 			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
// 				Ok(ValueRequired::U64(value))
// 			}
// 			#[inline]
// 			fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
// 				Ok(ValueRequired::I64(value))
// 			}

// 			#[inline]
// 			fn visit_f32<E>(self, value: f32) -> Result<Self::Value, E> {
// 				Ok(ValueRequired::F32(value))
// 			}
// 			#[inline]
// 			fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
// 				Ok(ValueRequired::F64(value))
// 			}

// 			#[inline]
// 			fn visit_str<E>(self, value: &str) -> Result<ValueRequired, E>
// 			where
// 				E: serde::de::Error,
// 			{
// 				self.visit_string(String::from(value))
// 			}

// 			#[inline]
// 			fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
// 				Ok(ValueRequired::String(value))
// 			}

// 			#[inline]
// 			fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E> {
// 				Ok(ValueRequired::ByteArray(v.to_owned()))
// 			}

// 			#[inline]
// 			fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E> {
// 				Ok(ValueRequired::ByteArray(v))
// 			}

// 			// #[inline]
// 			// fn visit_none<E>(self) -> Result<Self::Value, E> {
// 			//     // Ok(ValueRequired::Null)
// 			//     panic!()
// 			// }

// 			// #[inline]
// 			// fn visit_some<D>(self, deserializer: D) -> Result<ValueRequired, D::Error>
// 			// where
// 			//     D: serde::Deserializer<'de>,
// 			// {
// 			//     Deserialize::deserialize(deserializer)
// 			//         .map(|value: ValueRequired| ValueRequired::Option(Some(<Option<ValueRequiredRequired>>::from(value).unwrap())))
// 			// }

// 			// #[inline]
// 			// fn visit_unit<E>(self) -> Result<Self::Value, E> {
// 			//     // Ok(ValueRequired::Null)
// 			//     panic!()
// 			// }

// 			#[inline]
// 			fn visit_seq<V>(self, mut visitor: V) -> Result<ValueRequired, V::Error>
// 			where
// 				V: SeqAccess<'de>,
// 			{
// 				let mut vec = Vec::new();

// 				while let Some(elem) = visitor.next_element()? {
// 					vec.push(elem);
// 				}

// 				Ok(ValueRequired::List(List(vec)))
// 			}

// 			fn visit_map<V>(self, mut visitor: V) -> Result<ValueRequired, V::Error>
// 			where
// 				V: MapAccess<'de>,
// 			{
// 				let mut values = HashMap::new();
// 				while let Some((key, value)) = visitor.next_entry()? {
// 					values.insert(key, value);
// 				}

// 				Ok(ValueRequired::Map(Map(values)))
// 			}
// 		}

// 		deserializer.deserialize_any(ValueVisitor)
// 	}
// }
