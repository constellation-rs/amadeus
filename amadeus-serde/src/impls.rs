#![allow(clippy::too_many_lines)]

use hashlink::linked_hash_map::LinkedHashMap;
use recycle::VecExt;
use serde::{
	de::{self, MapAccess, SeqAccess, Visitor}, ser::{SerializeSeq, SerializeStruct, SerializeTupleStruct}, Deserializer, Serializer
};
use std::{
	collections::HashMap, fmt, hash::{BuildHasher, Hash}, str, sync::Arc
};

use amadeus_core::util::{type_coerce, type_coerce_ref, type_eq};
use amadeus_types::{
	Bson, Data, Date, DateTime, DateTimeWithoutTimezone, DateWithoutTimezone, Decimal, Enum, Group, IpAddr, Json, List, SchemaIncomplete, Time, TimeWithoutTimezone, Timezone, Url, Value, ValueRequired, Webpage
};

use super::{SerdeData, SerdeDeserialize, SerdeSerialize};

macro_rules! forward {
	($($t:ty)*) => {$(
		impl SerdeData for $t {
			fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
			where
				S: Serializer,
			{
				serde::Serialize::serialize(self, serializer)
			}
			fn deserialize<'de, D>(
				deserializer: D, _schema: Option<SchemaIncomplete>,
			) -> Result<Self, D::Error>
			where
				D: Deserializer<'de>,
			{
				serde::Deserialize::deserialize(deserializer)
			}
		}
	)*};
}

forward!(bool u8 i8 u16 i16 u32 i32 u64 i64 f32 f64 Bson String Enum Json);

macro_rules! via_string {
	($($t:ty)*) => {$(
		impl SerdeData for $t {
			fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
			where
				S: Serializer,
			{
				serde::Serialize::serialize(&self.to_string(), serializer)
			}
			fn deserialize<'de, D>(
				deserializer: D, _schema: Option<SchemaIncomplete>,
			) -> Result<Self, D::Error>
			where
				D: Deserializer<'de>,
			{
				let s: String = serde::Deserialize::deserialize(deserializer)?;
				s.parse().map_err(de::Error::custom)
			}
		}
	)*};
}

via_string!(Decimal Date DateWithoutTimezone Time TimeWithoutTimezone DateTime DateTimeWithoutTimezone Timezone Webpage<'static> Url IpAddr);

impl<T> SerdeData for Option<T>
where
	T: SerdeData,
{
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serde::Serialize::serialize(&self.as_ref().map(SerdeSerialize), serializer)
	}
	fn deserialize<'de, D>(
		deserializer: D, _schema: Option<SchemaIncomplete>,
	) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		<Option<SerdeDeserialize<T>> as serde::Deserialize>::deserialize(deserializer)
			.map(|x| x.map(|x| x.0))
	}
}

impl SerdeData for Group {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		if let Some(field_names) = self.field_names() {
			let mut struct_serializer =
				serializer.serialize_struct("Group", self.fields().len())?;
			for (name, field) in field_names
				.iter()
				.map(|(name, _index)| name)
				.zip(self.fields().iter())
			{
				struct_serializer.serialize_field(
					Box::leak(name.clone().into_boxed_str()),
					&SerdeSerialize(field),
				)?; // TODO!! static hashmap caching strings? aka string interning
				 // https://github.com/serde-rs/serde/issues/708
			}
			struct_serializer.end()
		} else {
			let mut tuple_serializer =
				serializer.serialize_tuple_struct("Group", self.fields().len())?;
			for field in self.fields() {
				tuple_serializer.serialize_field(&SerdeSerialize(field))?;
			}
			tuple_serializer.end()
		}
	}
	fn deserialize<'de, D>(
		deserializer: D, _schema: Option<SchemaIncomplete>,
	) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct GroupVisitor;

		impl<'de> Visitor<'de> for GroupVisitor {
			type Value = Group;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("any valid Amadeus group")
			}

			#[inline]
			fn visit_seq<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
			where
				V: SeqAccess<'de>,
			{
				let mut values = Vec::with_capacity(visitor.size_hint().unwrap_or(0));

				while let Some(elem) = visitor.next_element::<SerdeDeserialize<Value>>()? {
					values.push(elem.0);
				}

				Ok(Group::new(values, None))
			}

			fn visit_map<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
			where
				V: MapAccess<'de>,
			{
				let cap = visitor.size_hint().unwrap_or(0);
				let mut keys = LinkedHashMap::with_capacity_and_hasher(cap, Default::default());
				let mut values = Vec::with_capacity(cap);
				while let Some((key, value)) =
					visitor.next_entry::<String, SerdeDeserialize<Value>>()?
				{
					if keys.insert(key, values.len()).is_some() {
						return Err(de::Error::duplicate_field(""));
					}
					values.push(value.0);
				}

				Ok(Group::new(values, Some(Arc::new(keys))))
			}
		}

		deserializer.deserialize_struct("Group", &[], GroupVisitor)
	}
}

impl<T: Data> SerdeData for List<T>
where
	T: SerdeData,
{
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		if let Some(self_) = type_coerce_ref::<_, List<u8>>(self) {
			return serde_bytes::Serialize::serialize(&**self_, serializer);
		}
		let mut serializer = serializer.serialize_seq(Some(self.len()))?;
		for item in self.clone() {
			serializer.serialize_element(&SerdeSerialize(&item))?;
		}
		serializer.end()
	}
	fn deserialize<'de, D>(
		deserializer: D, _schema: Option<SchemaIncomplete>,
	) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		if type_eq::<List<u8>, Self>() {
			return serde_bytes::Deserialize::deserialize(deserializer)
				.map(|res: Vec<u8>| type_coerce::<List<u8>, _>(res.into()).unwrap());
		}
		// Self::deserialize(deserializer)
		unimplemented!()
	}
}

impl<K, V, S> SerdeData for HashMap<K, V, S>
where
	K: Hash + Eq + SerdeData,
	V: SerdeData,
	S: BuildHasher + Clone + 'static,
{
	fn serialize<S1>(&self, _serializer: S1) -> Result<S1::Ok, S1::Error>
	where
		S1: Serializer,
	{
		// self.serialize(serializer)
		unimplemented!()
	}
	fn deserialize<'de, D>(
		_deserializer: D, _schema: Option<SchemaIncomplete>,
	) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		// Self::deserialize(deserializer)
		unimplemented!()
	}
}

impl SerdeData for Value {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		match self {
			Self::Bool(value) => SerdeData::serialize(value, serializer),
			Self::U8(value) => SerdeData::serialize(value, serializer),
			Self::I8(value) => SerdeData::serialize(value, serializer),
			Self::U16(value) => SerdeData::serialize(value, serializer),
			Self::I16(value) => SerdeData::serialize(value, serializer),
			Self::U32(value) => SerdeData::serialize(value, serializer),
			Self::I32(value) => SerdeData::serialize(value, serializer),
			Self::U64(value) => SerdeData::serialize(value, serializer),
			Self::I64(value) => SerdeData::serialize(value, serializer),
			Self::F32(value) => SerdeData::serialize(value, serializer),
			Self::F64(value) => SerdeData::serialize(value, serializer),
			Self::Date(value) => SerdeData::serialize(value, serializer),
			Self::DateWithoutTimezone(value) => SerdeData::serialize(value, serializer),
			Self::Time(value) => SerdeData::serialize(value, serializer),
			Self::TimeWithoutTimezone(value) => SerdeData::serialize(value, serializer),
			Self::DateTime(value) => SerdeData::serialize(value, serializer),
			Self::DateTimeWithoutTimezone(value) => SerdeData::serialize(value, serializer),
			Self::Timezone(value) => SerdeData::serialize(value, serializer),
			Self::Decimal(value) => SerdeData::serialize(value, serializer),
			Self::Bson(value) => SerdeData::serialize(value, serializer),
			Self::String(value) => SerdeData::serialize(value, serializer),
			Self::Json(value) => SerdeData::serialize(value, serializer),
			Self::Enum(value) => SerdeData::serialize(value, serializer),
			Self::Url(value) => SerdeData::serialize(value, serializer),
			Self::Webpage(value) => SerdeData::serialize(value, serializer),
			Self::IpAddr(value) => SerdeData::serialize(value, serializer),
			Self::List(value) => SerdeData::serialize(value, serializer),
			Self::Map(value) => SerdeData::serialize(value, serializer),
			Self::Group(value) => SerdeData::serialize(value, serializer),
			Self::Option(value) => match value {
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
					ValueRequired::DateWithoutTimezone(value) => {
						serializer.serialize_some(&SerdeSerialize(value))
					}
					ValueRequired::Time(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::TimeWithoutTimezone(value) => {
						serializer.serialize_some(&SerdeSerialize(value))
					}
					ValueRequired::DateTime(value) => {
						serializer.serialize_some(&SerdeSerialize(value))
					}
					ValueRequired::DateTimeWithoutTimezone(value) => {
						serializer.serialize_some(&SerdeSerialize(value))
					}
					ValueRequired::Timezone(value) => {
						serializer.serialize_some(&SerdeSerialize(value))
					}
					ValueRequired::Decimal(value) => {
						serializer.serialize_some(&SerdeSerialize(value))
					}
					ValueRequired::Bson(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::String(value) => {
						serializer.serialize_some(&SerdeSerialize(value))
					}
					ValueRequired::Json(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::Enum(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::Url(value) => serializer.serialize_some(&SerdeSerialize(value)),
					ValueRequired::Webpage(value) => {
						serializer.serialize_some(&SerdeSerialize(value))
					}
					ValueRequired::IpAddr(value) => {
						serializer.serialize_some(&SerdeSerialize(value))
					}
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
	fn deserialize<'de, D>(
		deserializer: D, schema: Option<SchemaIncomplete>,
	) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
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
				E: de::Error,
			{
				self.visit_string(String::from(value))
			}

			#[inline]
			fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
				Ok(Value::String(value))
			}

			#[inline]
			fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E> {
				Ok(<List<u8>>::from(v.to_owned()).into())
			}

			#[inline]
			fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E> {
				Ok(<List<u8>>::from(v).into())
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
				D: Deserializer<'de>,
			{
				serde::Deserialize::deserialize(deserializer).map(
					|value: SerdeDeserialize<Value>| {
						Value::Option(Some(<Option<ValueRequired>>::from(value.0).unwrap()))
					},
				)
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

				Ok(Value::List(vec.into()))
			}

			fn visit_map<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
			where
				V: MapAccess<'de>,
			{
				let mut values = HashMap::with_capacity(visitor.size_hint().unwrap_or(0));
				while let Some((key, value)) =
					visitor.next_entry::<SerdeDeserialize<Value>, SerdeDeserialize<Value>>()?
				{
					if values.insert(key.0, value.0).is_some() {
						return Err(de::Error::duplicate_field(""));
					}
				}

				Ok(Value::Map(values))
			}
		}

		if let Some(schema) = schema {
			assert_eq!(schema, SchemaIncomplete::Group(None)); // TODO
			return Group::deserialize(deserializer, Some(schema)).map(Self::Group);
		}
		deserializer.deserialize_any(ValueVisitor)
	}
}

use std::convert::TryInto;

/// Implement SerdeData for common array lengths.
macro_rules! array {
	($($i:tt)*) => {$(
		// TODO: Specialize Box<[T; N]> to avoid passing a potentially large array around on the stack.
		impl<T> SerdeData for [T; $i]
		where
			T: SerdeData
		{
			fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
			where
				S: Serializer,
			{
				if let Some(self_) = type_coerce_ref::<_, [u8; $i]>(self) {
					return serde_bytes::Serialize::serialize(self_ as &[u8], serializer);
				}
				let self_: *const Self = self;
				#[allow(unsafe_code)]
				let self_: &[SerdeSerialize<T>; $i] = unsafe{ &*(self_ as *const _)};
				serde::Serialize::serialize(self_, serializer)
			}
			fn deserialize<'de, D>(
				deserializer: D, _schema: Option<SchemaIncomplete>,
			) -> Result<Self, D::Error>
			where
				D: Deserializer<'de>,
			{
				Ok(*if type_eq::<[u8; $i], Self>() {
					let self_: Vec<u8> = serde_bytes::Deserialize::deserialize(deserializer)?;
					let self_: Box<[u8; $i]> = self_.into_boxed_slice().try_into().unwrap();
					type_coerce(self_).unwrap()
				} else {
					let self_: [SerdeDeserialize<T>; $i] = serde::Deserialize::deserialize(deserializer)?;
					let self_: Box<Self> = <Vec<SerdeDeserialize<T>>>::from(self_).map(|a|a.0).into_boxed_slice().try_into().unwrap();
					self_
				})
			}
		}
	)*};
}
amadeus_types::array!(array);

/// Implement SerdeData on tuples up to length 12.
macro_rules! tuple {
	($len:tt $($t:ident $i:tt)*) => (
		impl<$($t,)*> SerdeData for ($($t,)*) where $($t: SerdeData,)* {
			fn serialize<S_>(&self, serializer: S_) -> Result<S_::Ok, S_::Error>
			where
				S_: Serializer {
				serde::Serialize::serialize(&($(SerdeSerialize(&self.$i),)*), serializer)
			}
			#[allow(unused_variables)]
			fn deserialize<'de,D_>(deserializer: D_, _schema: Option<SchemaIncomplete>) -> Result<Self, D_::Error>
			where
				D_: Deserializer<'de> {
				<($(SerdeDeserialize<$t>,)*) as serde::Deserialize>::deserialize(deserializer).map(|self_|($((self_.$i).0,)*))
			}
		}
	);
}
amadeus_types::tuple!(tuple);
