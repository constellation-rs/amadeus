//! Implement [`Record`] for [`ValueRequired`] – an enum representing any valid required
//! Parquet value.

use std::{
	cmp::Ordering, collections::HashMap, hash::{Hash, Hasher}, mem, ptr
};

use super::{
	AmadeusOrd, Bson, Date, DateTime, DateTimeWithoutTimezone, DateWithoutTimezone, Decimal, Enum, Group, IpAddr, Json, List, Time, TimeWithoutTimezone, Timezone, Url, Value, Webpage
};

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
	/// Timezone
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
}

impl ValueRequired {
	pub fn as_value<R>(&self, f: impl FnOnce(&Value) -> R) -> R {
		#[allow(unsafe_code)]
		let value = mem::ManuallyDrop::new(unsafe {
			match self {
				Self::Bool(value) => Value::Bool(ptr::read(value)),
				Self::U8(value) => Value::U8(ptr::read(value)),
				Self::I8(value) => Value::I8(ptr::read(value)),
				Self::U16(value) => Value::U16(ptr::read(value)),
				Self::I16(value) => Value::I16(ptr::read(value)),
				Self::U32(value) => Value::U32(ptr::read(value)),
				Self::I32(value) => Value::I32(ptr::read(value)),
				Self::U64(value) => Value::U64(ptr::read(value)),
				Self::I64(value) => Value::I64(ptr::read(value)),
				Self::F32(value) => Value::F32(ptr::read(value)),
				Self::F64(value) => Value::F64(ptr::read(value)),
				Self::Date(value) => Value::Date(ptr::read(value)),
				Self::DateWithoutTimezone(value) => Value::DateWithoutTimezone(ptr::read(value)),
				Self::Time(value) => Value::Time(ptr::read(value)),
				Self::TimeWithoutTimezone(value) => Value::TimeWithoutTimezone(ptr::read(value)),
				Self::DateTime(value) => Value::DateTime(ptr::read(value)),
				Self::DateTimeWithoutTimezone(value) => {
					Value::DateTimeWithoutTimezone(ptr::read(value))
				}
				Self::Timezone(value) => Value::Timezone(ptr::read(value)),
				Self::Decimal(value) => Value::Decimal(ptr::read(value)),
				Self::Bson(value) => Value::Bson(ptr::read(value)),
				Self::String(value) => Value::String(ptr::read(value)),
				Self::Json(value) => Value::Json(ptr::read(value)),
				Self::Enum(value) => Value::Enum(ptr::read(value)),
				Self::Url(value) => Value::Url(ptr::read(value)),
				Self::Webpage(value) => Value::Webpage(ptr::read(value)),
				Self::IpAddr(value) => Value::IpAddr(ptr::read(value)),
				Self::List(value) => Value::List(ptr::read(value)),
				Self::Map(value) => Value::Map(ptr::read(value)),
				Self::Group(value) => Value::Group(ptr::read(value)),
			}
		});
		f(&value)
	}
}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for ValueRequired {
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
			_ => None,
		}
	}
}
impl AmadeusOrd for ValueRequired {
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
			_ => unimplemented!(),
		}
	}
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
			ValueRequired::DateWithoutTimezone(value) => Self::DateWithoutTimezone(value),
			ValueRequired::Time(value) => Self::Time(value),
			ValueRequired::TimeWithoutTimezone(value) => Self::TimeWithoutTimezone(value),
			ValueRequired::DateTime(value) => Self::DateTime(value),
			ValueRequired::DateTimeWithoutTimezone(value) => Self::DateTimeWithoutTimezone(value),
			ValueRequired::Timezone(value) => Self::Timezone(value),
			ValueRequired::Decimal(value) => Self::Decimal(value),
			ValueRequired::Bson(value) => Self::Bson(value),
			ValueRequired::String(value) => Self::String(value),
			ValueRequired::Json(value) => Self::Json(value),
			ValueRequired::Enum(value) => Self::Enum(value),
			ValueRequired::Url(value) => Self::Url(value),
			ValueRequired::Webpage(value) => Self::Webpage(value),
			ValueRequired::IpAddr(value) => Self::IpAddr(value),
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
			Value::DateWithoutTimezone(value) => ValueRequired::DateWithoutTimezone(value),
			Value::Time(value) => ValueRequired::Time(value),
			Value::TimeWithoutTimezone(value) => ValueRequired::TimeWithoutTimezone(value),
			Value::DateTime(value) => ValueRequired::DateTime(value),
			Value::DateTimeWithoutTimezone(value) => ValueRequired::DateTimeWithoutTimezone(value),
			Value::Timezone(value) => ValueRequired::Timezone(value),
			Value::Decimal(value) => ValueRequired::Decimal(value),
			Value::Bson(value) => ValueRequired::Bson(value),
			Value::String(value) => ValueRequired::String(value),
			Value::Json(value) => ValueRequired::Json(value),
			Value::Enum(value) => ValueRequired::Enum(value),
			Value::Url(value) => ValueRequired::Url(value),
			Value::Webpage(value) => ValueRequired::Webpage(value),
			Value::IpAddr(value) => ValueRequired::IpAddr(value),
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
// 			ValueRequired::DateTime(value) => value.serialize(serializer),
// 			ValueRequired::Decimal(value) => value.serialize(serializer),
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
