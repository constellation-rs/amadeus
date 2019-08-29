use super::{SerdeData, SerdeDeserialize, SerdeSerialize};
use amadeus_types::{
	Bson, Date, Decimal, Enum, Group, Json, List, Map, SchemaIncomplete, Time, Timestamp, Value, ValueRequired
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use linked_hash_map::LinkedHashMap;
use serde::{
	de::{self, MapAccess, SeqAccess, Visitor}, ser::{SerializeStruct, SerializeTupleStruct}, Deserializer, Serializer
};
use std::{collections::HashMap, convert::TryFrom, fmt, hash::Hash, str, sync::Arc};

impl SerdeData for Vec<u8> {
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

impl SerdeData for Bson {
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

impl SerdeData for Json {
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

impl SerdeData for Enum {
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

macro_rules! impl_parquet_record_array {
	($i:tt) => {
		impl SerdeData for [u8; $i] {
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

		// Specialize the implementation to avoid passing a potentially large array around
		// on the stack.
		impl SerdeData for Box<[u8; $i]> {
			fn deserialize<'de, D>(
				deserializer: D, _schema: Option<SchemaIncomplete>,
			) -> Result<Self, D::Error>
			where
				D: Deserializer<'de>,
			{
				// TODO
				serde::Deserialize::deserialize(deserializer)
			}
		}
	};
}

// Implement Record for common array lengths, copied from arrayvec
impl_parquet_record_array!(0);
impl_parquet_record_array!(1);
impl_parquet_record_array!(2);
impl_parquet_record_array!(3);
impl_parquet_record_array!(4);
impl_parquet_record_array!(5);
impl_parquet_record_array!(6);
impl_parquet_record_array!(7);
impl_parquet_record_array!(8);
impl_parquet_record_array!(9);
impl_parquet_record_array!(10);
impl_parquet_record_array!(11);
impl_parquet_record_array!(12);
impl_parquet_record_array!(13);
impl_parquet_record_array!(14);
impl_parquet_record_array!(15);
impl_parquet_record_array!(16);
impl_parquet_record_array!(17);
impl_parquet_record_array!(18);
impl_parquet_record_array!(19);
impl_parquet_record_array!(20);
impl_parquet_record_array!(21);
impl_parquet_record_array!(22);
impl_parquet_record_array!(23);
impl_parquet_record_array!(24);
impl_parquet_record_array!(25);
impl_parquet_record_array!(26);
impl_parquet_record_array!(27);
impl_parquet_record_array!(28);
impl_parquet_record_array!(29);
impl_parquet_record_array!(30);
impl_parquet_record_array!(31);
impl_parquet_record_array!(32);
// impl_parquet_record_array!(40);
// impl_parquet_record_array!(48);
// impl_parquet_record_array!(50);
// impl_parquet_record_array!(56);
// impl_parquet_record_array!(64);
// impl_parquet_record_array!(72);
// impl_parquet_record_array!(96);
// impl_parquet_record_array!(100);
// impl_parquet_record_array!(128);
// impl_parquet_record_array!(160);
// impl_parquet_record_array!(192);
// impl_parquet_record_array!(200);
// impl_parquet_record_array!(224);
// impl_parquet_record_array!(256);
// impl_parquet_record_array!(384);
// impl_parquet_record_array!(512);
// impl_parquet_record_array!(768);
// impl_parquet_record_array!(1024);
// impl_parquet_record_array!(2048);
// impl_parquet_record_array!(4096);
// impl_parquet_record_array!(8192);
// impl_parquet_record_array!(16384);
// impl_parquet_record_array!(32768);
// impl_parquet_record_array!(65536);

/// Macro to implement [`Reader`] on tuples up to length 32.
macro_rules! impl_parquet_record_tuple {
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
// impl_parquet_record_tuple!(13 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12);
// impl_parquet_record_tuple!(14 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13);
// impl_parquet_record_tuple!(15 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14);
// impl_parquet_record_tuple!(16 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15);
// impl_parquet_record_tuple!(17 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16);
// impl_parquet_record_tuple!(18 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17);
// impl_parquet_record_tuple!(19 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18);
// impl_parquet_record_tuple!(20 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19);
// impl_parquet_record_tuple!(21 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20);
// impl_parquet_record_tuple!(22 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21);
// impl_parquet_record_tuple!(23 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22);
// impl_parquet_record_tuple!(24 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23);
// impl_parquet_record_tuple!(25 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24);
// impl_parquet_record_tuple!(26 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25);
// impl_parquet_record_tuple!(27 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26);
// impl_parquet_record_tuple!(28 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27);
// impl_parquet_record_tuple!(29 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28);
// impl_parquet_record_tuple!(30 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29);
// impl_parquet_record_tuple!(31 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30);
// impl_parquet_record_tuple!(32 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30 AF 31);

impl SerdeData for Decimal {
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

impl<T> SerdeData for List<T>
where
	T: SerdeData,
{
	fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
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

impl<K, V> SerdeData for Map<K, V>
where
	K: Hash + Eq + SerdeData,
	V: SerdeData,
{
	fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
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

impl SerdeData for Date {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serde::Serialize::serialize(
			&NaiveDate::try_from(*self).expect("not implemented yet"),
			serializer,
		)
	}
	fn deserialize<'de, D>(
		deserializer: D, _schema: Option<SchemaIncomplete>,
	) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		<NaiveDate as serde::Deserialize>::deserialize(deserializer).map(Into::into)
	}
}

impl SerdeData for Time {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serde::Serialize::serialize(
			&NaiveTime::try_from(*self).expect("not implemented yet"),
			serializer,
		)
	}
	fn deserialize<'de, D>(
		deserializer: D, _schema: Option<SchemaIncomplete>,
	) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		<NaiveTime as serde::Deserialize>::deserialize(deserializer).map(Into::into)
	}
}

impl SerdeData for Timestamp {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serde::Serialize::serialize(
			&NaiveDateTime::try_from(*self).expect("not implemented yet"),
			serializer,
		)
	}
	fn deserialize<'de, D>(
		deserializer: D, _schema: Option<SchemaIncomplete>,
	) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		<NaiveDateTime as serde::Deserialize>::deserialize(deserializer).map(Into::into)
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
			Self::Time(value) => SerdeData::serialize(value, serializer),
			Self::Timestamp(value) => SerdeData::serialize(value, serializer),
			Self::Decimal(value) => SerdeData::serialize(value, serializer),
			Self::ByteArray(value) => SerdeData::serialize(value, serializer),
			Self::Bson(value) => SerdeData::serialize(value, serializer),
			Self::String(value) => SerdeData::serialize(value, serializer),
			Self::Json(value) => SerdeData::serialize(value, serializer),
			Self::Enum(value) => SerdeData::serialize(value, serializer),
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

				Ok(Value::Map(values.into()))
			}
		}

		if let Some(schema) = schema {
			assert_eq!(schema, SchemaIncomplete::Group(None)); // TODO
			return Group::deserialize(deserializer, Some(schema)).map(Self::Group);
		}
		deserializer.deserialize_any(ValueVisitor)
	}
}

macro_rules! impl_data_for_record {
	($($t:ty : $pt:ty),*) => (
		$(
			#[allow(clippy::use_self)]
			impl SerdeData for $t {
				fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
				where
					S: Serializer {
					serde::Serialize::serialize(self, serializer)
				}
				fn deserialize<'de, D>(deserializer: D, _schema: Option<SchemaIncomplete>) -> Result<Self, D::Error>
				where
					D: Deserializer<'de> {
					serde::Deserialize::deserialize(deserializer)
				}
			}
		)*
	);
}
impl_data_for_record!(
	bool: bool,
	u8: i8,
	i8: i8,
	u16: i16,
	i16: i16,
	u32: i32,
	i32: i32,
	u64: i64,
	i64: i64,
	f32: f32,
	f64: f64,
	String: String
);
// use super::types::{Date,Time,Timestamp,Decimal};

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
