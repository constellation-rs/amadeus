#![allow(clippy::type_complexity)]

use super::ParquetData;
use amadeus_types::{
	Bson, Date, Decimal, Enum, Group, Json, List, Map, Time, Timestamp, Value, ValueRequired
};
use parchet::{
	basic::Repetition, column::reader::ColumnReader, errors::ParquetError, record::Record as ParquetRecord, schema::types::{ColumnPath, Type}
};
use std::{
	collections::HashMap, convert::{TryFrom, TryInto}, hash::Hash, marker::PhantomData, mem::transmute
};

pub trait InternalInto<T> {
	fn internal_into(self) -> T;
}
impl InternalInto<Bson> for parchet::record::types::Bson {
	fn internal_into(self) -> Bson {
		let vec: Vec<u8> = self.into();
		vec.into()
	}
}
impl InternalInto<Json> for parchet::record::types::Json {
	fn internal_into(self) -> Json {
		let vec: String = self.into();
		vec.into()
	}
}
impl InternalInto<Enum> for parchet::record::types::Enum {
	fn internal_into(self) -> Enum {
		let vec: String = self.into();
		vec.into()
	}
}
impl InternalInto<Decimal> for parchet::data_type::Decimal {
	fn internal_into(self) -> Decimal {
		unimplemented!()
	}
}
impl InternalInto<Group> for parchet::record::types::Group {
	fn internal_into(self) -> Group {
		let field_names = self.field_names();
		Group::new(
			self.into_fields()
				.into_iter()
				.map(InternalInto::internal_into)
				.collect(),
			Some(field_names),
		)
	}
}
const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
const NANOS_PER_MICRO: i64 = 1_000;
impl InternalInto<Date> for parchet::record::types::Date {
	fn internal_into(self) -> Date {
		Date::from_days(self.as_days().into()).unwrap()
	}
}
impl InternalInto<Time> for parchet::record::types::Time {
	fn internal_into(self) -> Time {
		Time::from_nanos(self.as_micros() * u64::try_from(NANOS_PER_MICRO).unwrap()).unwrap()
	}
}
impl InternalInto<Timestamp> for parchet::record::types::Timestamp {
	fn internal_into(self) -> Timestamp {
		// Self::from_day_nanos(self.as_nanos().unwrap())
		let (date, time) = self.as_day_nanos();
		let date = Date::from_days(date - JULIAN_DAY_OF_EPOCH).unwrap();
		let time = Time::from_nanos(time.try_into().unwrap()).unwrap();
		Timestamp::from_date_time(date, time).unwrap()
	}
}
impl InternalInto<Value> for parchet::record::types::Value {
	fn internal_into(self) -> Value {
		match self {
			Self::Bool(value) => Value::Bool(value),
			Self::U8(value) => Value::U8(value),
			Self::I8(value) => Value::I8(value),
			Self::U16(value) => Value::U16(value),
			Self::I16(value) => Value::I16(value),
			Self::U32(value) => Value::U32(value),
			Self::I32(value) => Value::I32(value),
			Self::U64(value) => Value::U64(value),
			Self::I64(value) => Value::I64(value),
			Self::F32(value) => Value::F32(value),
			Self::F64(value) => Value::F64(value),
			Self::Date(value) => Value::Date(value.internal_into()),
			Self::Time(value) => Value::Time(value.internal_into()),
			Self::Timestamp(value) => Value::Timestamp(value.internal_into()),
			Self::Decimal(value) => Value::Decimal(value.internal_into()),
			Self::ByteArray(value) => Value::ByteArray(value),
			Self::Bson(value) => Value::Bson(value.internal_into()),
			Self::String(value) => Value::String(value),
			Self::Json(value) => Value::Json(value.internal_into()),
			Self::Enum(value) => Value::Enum(value.internal_into()),
			Self::List(value) => Value::List(value.internal_into()),
			Self::Map(value) => Value::Map(value.internal_into()),
			Self::Group(value) => Value::Group(value.internal_into()),
			Self::Option(value) => Value::Option(value.map(|value| match value {
				parchet::record::types::ValueRequired::Bool(value) => ValueRequired::Bool(value),
				parchet::record::types::ValueRequired::U8(value) => ValueRequired::U8(value),
				parchet::record::types::ValueRequired::I8(value) => ValueRequired::I8(value),
				parchet::record::types::ValueRequired::U16(value) => ValueRequired::U16(value),
				parchet::record::types::ValueRequired::I16(value) => ValueRequired::I16(value),
				parchet::record::types::ValueRequired::U32(value) => ValueRequired::U32(value),
				parchet::record::types::ValueRequired::I32(value) => ValueRequired::I32(value),
				parchet::record::types::ValueRequired::U64(value) => ValueRequired::U64(value),
				parchet::record::types::ValueRequired::I64(value) => ValueRequired::I64(value),
				parchet::record::types::ValueRequired::F32(value) => ValueRequired::F32(value),
				parchet::record::types::ValueRequired::F64(value) => ValueRequired::F64(value),
				parchet::record::types::ValueRequired::Date(value) => {
					ValueRequired::Date(value.internal_into())
				}
				parchet::record::types::ValueRequired::Time(value) => {
					ValueRequired::Time(value.internal_into())
				}
				parchet::record::types::ValueRequired::Timestamp(value) => {
					ValueRequired::Timestamp(value.internal_into())
				}
				parchet::record::types::ValueRequired::Decimal(value) => {
					ValueRequired::Decimal(value.internal_into())
				}
				parchet::record::types::ValueRequired::ByteArray(value) => {
					ValueRequired::ByteArray(value)
				}
				parchet::record::types::ValueRequired::Bson(value) => {
					ValueRequired::Bson(value.internal_into())
				}
				parchet::record::types::ValueRequired::String(value) => {
					ValueRequired::String(value)
				}
				parchet::record::types::ValueRequired::Json(value) => {
					ValueRequired::Json(value.internal_into())
				}
				parchet::record::types::ValueRequired::Enum(value) => {
					ValueRequired::Enum(value.internal_into())
				}
				parchet::record::types::ValueRequired::List(value) => {
					ValueRequired::List(value.internal_into())
				}
				parchet::record::types::ValueRequired::Map(value) => {
					ValueRequired::Map(value.internal_into())
				}
				parchet::record::types::ValueRequired::Group(value) => {
					ValueRequired::Group(value.internal_into())
				}
			})),
		}
	}
}
impl<T, U> InternalInto<List<T>> for parchet::record::types::List<U>
where
	T: ParquetData,
	U: InternalInto<T>,
{
	fn internal_into(self) -> List<T> {
		<_ as Into<Vec<U>>>::into(self)
			.into_iter()
			.map(InternalInto::internal_into)
			.collect::<Vec<_>>()
			.into()
	}
}
impl<K, V, K1, V1> InternalInto<Map<K, V>> for parchet::record::types::Map<K1, V1>
where
	K: Hash + Eq,
	K1: Hash + Eq + InternalInto<K>,
	V1: InternalInto<V>,
{
	fn internal_into(self) -> Map<K, V> {
		<_ as Into<HashMap<K1, V1>>>::into(self)
			.into_iter()
			.map(|(k, v)| (k.internal_into(), v.internal_into()))
			.collect::<HashMap<_, _>>()
			.into()
	}
}

// default impl<T> ParquetRecord for T where T: ParquetData {
// 	type Schema = <Self as ParquetData>::Schema;
// 	type Reader = <Self as ParquetData>::Reader;

// 	fn parse(
// 		schema: &Type, repetition: Option<Repetition>,
// 	) -> Result<(String, Self::Schema), ParquetError> {
// 		<Self as ParquetData>::parse(schema, repetition)
// 	}
// 	fn reader(
// 		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
// 		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
// 	) -> Self::Reader {
// 		<Self as ParquetData>::reader(schema, path, def_level, rep_level, paths, batch_size)
// 	}
// }

impl<T> ParquetData for Box<T>
where
	T: ParquetData,
{
	default type Schema = <T as ParquetData>::Schema;
	default type Reader = MapReader<<T as ParquetData>::Reader, fn(T) -> Box<T>>;

	default fn parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema), ParquetError> {
		unsafe { known_type(<T as ParquetData>::parse(schema, repetition)) }
	}
	default fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		unsafe {
			known_type(MapReader::new(
				<T as ParquetData>::reader(
					known_type(schema),
					path,
					def_level,
					rep_level,
					paths,
					batch_size,
				),
				Box::<T>::new,
			))
		}
	}
}
impl<T> ParquetData for Box<T>
where
	T: ParquetData,
	Box<T>: ParquetRecord,
{
	type Schema = <Self as ParquetRecord>::Schema;
	type Reader = <Self as ParquetRecord>::Reader;

	fn parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema), ParquetError> {
		<Self as ParquetRecord>::parse(schema, repetition)
	}
	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		<Self as ParquetRecord>::reader(schema, path, def_level, rep_level, paths, batch_size)
	}
}

impl ParquetData for Vec<u8> {
	type Schema = <Self as ParquetRecord>::Schema;
	type Reader = <Self as ParquetRecord>::Reader;

	fn parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema), ParquetError> {
		<Self as ParquetRecord>::parse(schema, repetition)
	}
	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		<Self as ParquetRecord>::reader(schema, path, def_level, rep_level, paths, batch_size)
	}
}

impl ParquetData for Bson {
	type Schema = <parchet::record::types::Bson as ParquetRecord>::Schema;
	type Reader = IntoReader<<parchet::record::types::Bson as ParquetRecord>::Reader, Self>;

	fn parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema), ParquetError> {
		<parchet::record::types::Bson as ParquetRecord>::parse(schema, repetition)
	}
	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		IntoReader::new(<parchet::record::types::Bson as ParquetRecord>::reader(
			schema, path, def_level, rep_level, paths, batch_size,
		))
	}
}

impl ParquetData for Json {
	type Schema = <parchet::record::types::Json as ParquetRecord>::Schema;
	type Reader = IntoReader<<parchet::record::types::Json as ParquetRecord>::Reader, Self>;

	fn parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema), ParquetError> {
		<parchet::record::types::Json as ParquetRecord>::parse(schema, repetition)
	}
	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		IntoReader::new(<parchet::record::types::Json as ParquetRecord>::reader(
			schema, path, def_level, rep_level, paths, batch_size,
		))
	}
}

impl ParquetData for Enum {
	type Schema = <parchet::record::types::Enum as ParquetRecord>::Schema;
	type Reader = IntoReader<<parchet::record::types::Enum as ParquetRecord>::Reader, Self>;

	fn parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema), ParquetError> {
		<parchet::record::types::Enum as ParquetRecord>::parse(schema, repetition)
	}
	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		IntoReader::new(<parchet::record::types::Enum as ParquetRecord>::reader(
			schema, path, def_level, rep_level, paths, batch_size,
		))
	}
}

macro_rules! impl_parquet_record_array {
	($i:tt) => {
		impl ParquetData for [u8; $i] {
			type Reader = <Self as ParquetRecord>::Reader;
			type Schema = <Self as ParquetRecord>::Schema;

			fn parse(
				schema: &Type, repetition: Option<Repetition>,
			) -> Result<(String, Self::Schema), ParquetError> {
				<Self as ParquetRecord>::parse(schema, repetition)
			}

			fn reader(
				schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
				paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
			) -> Self::Reader {
				<Self as ParquetRecord>::reader(
					schema, path, def_level, rep_level, paths, batch_size,
				)
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
		impl<$($t,)*> ParquetData for ($($t,)*) where $($t: ParquetData,)* {
			type Schema = <($(super::Record<$t>,)*) as ParquetRecord>::Schema;
			type Reader = impl parchet::record::Reader<Item = Self>;

			fn parse(schema: &Type, repetition: Option<Repetition>) -> Result<(String, Self::Schema), ParquetError> {
				<($(super::Record<$t>,)*) as ParquetRecord>::parse(schema, repetition)
			}
			#[allow(unused_variables)]
			fn reader(schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16, paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize) -> Self::Reader {
				MapReader::new(<($(super::Record<$t>,)*) as ParquetRecord>::reader(schema, path, def_level, rep_level, paths, batch_size), |self_:($(super::Record<$t>,)*)|($((self_.$i).0,)*))
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

impl ParquetData for Decimal {
	type Schema = <parchet::data_type::Decimal as ParquetRecord>::Schema;
	type Reader = IntoReader<<parchet::data_type::Decimal as ParquetRecord>::Reader, Self>;

	fn parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema), ParquetError> {
		<parchet::data_type::Decimal as ParquetRecord>::parse(schema, repetition)
	}
	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		IntoReader::new(<parchet::data_type::Decimal as ParquetRecord>::reader(
			schema, path, def_level, rep_level, paths, batch_size,
		))
	}
}

impl ParquetData for Group {
	type Schema = <parchet::record::types::Group as ParquetRecord>::Schema;
	type Reader = IntoReader<<parchet::record::types::Group as ParquetRecord>::Reader, Self>;

	fn parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema), ParquetError> {
		<parchet::record::types::Group as ParquetRecord>::parse(schema, repetition)
	}
	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		IntoReader::new(<parchet::record::types::Group as ParquetRecord>::reader(
			schema, path, def_level, rep_level, paths, batch_size,
		))
	}
}

impl<T> ParquetData for List<T>
where
	T: ParquetData,
{
	type Schema = <parchet::record::types::List<super::Record<T>> as ParquetRecord>::Schema;
	type Reader = impl parchet::record::Reader<Item = Self>;

	fn parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema), ParquetError> {
		<parchet::record::types::List<super::Record<T>> as ParquetRecord>::parse(schema, repetition)
	}
	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		MapReader::new(
			<parchet::record::types::List<super::Record<T>> as ParquetRecord>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			),
			|list| {
				<_ as Into<Vec<T>>>::into(unsafe {
					transmute::<
						parchet::record::types::List<super::Record<T>>,
						parchet::record::types::List<T>,
					>(list)
				})
				.into()
			},
		)
	}
}

impl<K, V> ParquetData for Map<K, V>
where
	K: Hash + Eq + ParquetData,
	V: ParquetData,
{
	type Schema =
		<parchet::record::types::Map<super::Record<K>, super::Record<V>> as ParquetRecord>::Schema;
	type Reader = impl parchet::record::Reader<Item = Self>;

	fn parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema), ParquetError> {
		<parchet::record::types::Map<super::Record<K>, super::Record<V>> as ParquetRecord>::parse(
			schema, repetition,
		)
	}
	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		MapReader::new(
			<parchet::record::types::Map<
				super::Record<K>,
				super::Record<V>,
			> as ParquetRecord>::reader(
				schema, path, def_level, rep_level, paths, batch_size
			),
			|map| {
				<_ as Into<HashMap<K,V>>>::into(unsafe {
					transmute::<
						parchet::record::types::Map<
							super::Record<K>,
							super::Record<V>,
						>,
						parchet::record::types::Map<K, V>,
					>(map)
				}).into()
			},
		)
	}
}

impl ParquetData for Date {
	type Schema = <parchet::record::types::Date as ParquetRecord>::Schema;
	type Reader = IntoReader<<parchet::record::types::Date as ParquetRecord>::Reader, Self>;

	fn parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema), ParquetError> {
		<parchet::record::types::Date as ParquetRecord>::parse(schema, repetition)
	}
	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		IntoReader::new(<parchet::record::types::Date as ParquetRecord>::reader(
			schema, path, def_level, rep_level, paths, batch_size,
		))
	}
}

impl ParquetData for Time {
	type Schema = <parchet::record::types::Time as ParquetRecord>::Schema;
	type Reader = IntoReader<<parchet::record::types::Time as ParquetRecord>::Reader, Self>;

	fn parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema), ParquetError> {
		<parchet::record::types::Time as ParquetRecord>::parse(schema, repetition)
	}
	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		IntoReader::new(<parchet::record::types::Time as ParquetRecord>::reader(
			schema, path, def_level, rep_level, paths, batch_size,
		))
	}
}

impl ParquetData for Timestamp {
	type Schema = <parchet::record::types::Timestamp as ParquetRecord>::Schema;
	type Reader = IntoReader<<parchet::record::types::Timestamp as ParquetRecord>::Reader, Self>;

	fn parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema), ParquetError> {
		<parchet::record::types::Timestamp as ParquetRecord>::parse(schema, repetition)
	}
	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		IntoReader::new(
			<parchet::record::types::Timestamp as ParquetRecord>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			),
		)
	}
}

impl ParquetData for Value {
	type Reader = IntoReader<<parchet::record::types::Value as ParquetRecord>::Reader, Self>;
	type Schema = <parchet::record::types::Value as ParquetRecord>::Schema;

	fn parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema), ParquetError> {
		parchet::record::types::Value::parse(schema, repetition)
	}
	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		IntoReader::new(parchet::record::types::Value::reader(
			schema, path, def_level, rep_level, paths, batch_size,
		))
	}
}
// /// A Reader that wraps a Reader, wrapping the read value in a `Record`.
// pub struct ValueReader<T>(T);
// impl<T> parchet::record::Reader for ValueReader<T>
// where
// 	T: parchet::record::Reader<Item = parchet::record::types::Value>,
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

macro_rules! impl_data_for_record {
	($($t:ty)*) => (
		$(
			#[allow(clippy::use_self)]
			impl ParquetData for $t {
				type Schema = <Self as ParquetRecord>::Schema;
				type Reader = <Self as ParquetRecord>::Reader;

				fn parse(
					schema: &Type, repetition: Option<Repetition>,
				) -> Result<(String, Self::Schema), ParquetError> {
					<Self as ParquetRecord>::parse(schema, repetition)
				}
				fn reader(
					schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
					paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
				) -> Self::Reader {
					<Self as ParquetRecord>::reader(schema, path, def_level, rep_level, paths, batch_size)
				}
			}
		)*
	);
}
impl_data_for_record!(bool u8 i8 u16 i16 u32 i32 u64 i64 f32 f64 String);

impl<T> ParquetData for Option<T>
where
	T: ParquetData,
{
	type Schema = <Option<super::Record<T>> as ParquetRecord>::Schema;
	type Reader = OptionReader<<Option<super::Record<T>> as ParquetRecord>::Reader>;

	fn parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema), ParquetError> {
		<Option<super::Record<T>> as ParquetRecord>::parse(schema, repetition)
	}
	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		OptionReader(<Option<super::Record<T>> as ParquetRecord>::reader(
			schema, path, def_level, rep_level, paths, batch_size,
		))
	}
}
/// A Reader that wraps a Reader, wrapping the read value in a `Record`.
pub struct OptionReader<T>(T);
impl<T, U> parchet::record::Reader for OptionReader<T>
where
	T: parchet::record::Reader<Item = Option<super::Record<U>>>,
	U: ParquetData,
{
	type Item = Option<U>;

	#[inline]
	fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item, ParquetError> {
		self.0.read(def_level, rep_level).map(|x| x.map(|x| x.0))
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

/// A convenience Reader that maps the read value using [`TryInto`].
pub struct IntoReader<R: parchet::record::Reader, T>(R, PhantomData<fn(T)>);
impl<R: parchet::record::Reader, T> IntoReader<R, T> {
	fn new(reader: R) -> Self {
		Self(reader, PhantomData)
	}
}
impl<R: parchet::record::Reader, T> parchet::record::Reader for IntoReader<R, T>
where
	R::Item: InternalInto<T>,
{
	type Item = T;

	#[inline]
	fn read(
		&mut self, def_level: i16, rep_level: i16,
	) -> Result<Self::Item, parchet::errors::ParquetError> {
		self.0
			.read(def_level, rep_level)
			.map(InternalInto::internal_into)
	}

	#[inline]
	fn advance_columns(&mut self) -> Result<(), parchet::errors::ParquetError> {
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

/// A convenience Reader that maps the read value using the supplied closure.
pub struct MapReader<R: parchet::record::Reader, F>(R, F);
impl<R: parchet::record::Reader, F> MapReader<R, F> {
	fn new(reader: R, f: F) -> Self {
		Self(reader, f)
	}
}
impl<R: parchet::record::Reader, F, T> parchet::record::Reader for MapReader<R, F>
where
	F: FnMut(R::Item) -> T,
{
	type Item = T;

	#[inline]
	fn read(
		&mut self, def_level: i16, rep_level: i16,
	) -> Result<Self::Item, parchet::errors::ParquetError> {
		self.0.read(def_level, rep_level).map(&mut self.1)
	}

	#[inline]
	fn advance_columns(&mut self) -> Result<(), parchet::errors::ParquetError> {
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

// /// A convenience Reader that maps the read value using [`TryInto`].
// pub struct TryIntoReader<R: parchet::record::Reader, T>(R, PhantomData<fn(T)>);
// impl<R: parchet::record::Reader, T> TryIntoReader<R, T> {
//     fn new(reader: R) -> Self {
//         TryIntoReader(reader, PhantomData)
//     }
// }
// impl<R: parchet::record::Reader, T> parchet::record::Reader for TryIntoReader<R, T>
// where
//     R::Item: TryInto<T>,
//     <R::Item as TryInto<T>>::Error: Error,
// {
//     type Item = T;

//     #[inline]
//     fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item, parchet::errors::ParquetError> {
//         self.0.read(def_level, rep_level).and_then(|x| {
//             x.try_into()
//                 .map_err(|err| ParquetError::General(err.description().to_owned()))
//         })
//     }

//     #[inline]
//     fn advance_columns(&mut self) -> Result<(), parchet::errors::ParquetError> {
//         self.0.advance_columns()
//     }

//     #[inline]
//     fn has_next(&self) -> bool {
//         self.0.has_next()
//     }

//     #[inline]
//     fn current_def_level(&self) -> i16 {
//         self.0.current_def_level()
//     }

//     #[inline]
//     fn current_rep_level(&self) -> i16 {
//         self.0.current_rep_level()
//     }
// }

/// This is used until specialization can handle groups of items together
unsafe fn known_type<A, B>(a: A) -> B {
	use std::mem;
	assert_eq!(
		(
			mem::size_of::<A>(),
			mem::align_of::<A>(),
			std::any::type_name::<A>()
		),
		(
			mem::size_of::<B>(),
			mem::align_of::<B>(),
			std::any::type_name::<B>()
		)
	);
	let ret = mem::transmute_copy(&a);
	mem::forget(a);
	ret
}
