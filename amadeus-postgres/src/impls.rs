use super::{Names, PostgresData};
use amadeus_types::{Bson, Date, Decimal, Enum, Group, Json, List, Map, Time, Timestamp, Value};
use chrono::NaiveDate;
use std::{
	error::Error, fmt::{self, Display}, hash::Hash
};

impl PostgresData for Vec<u8> {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(
		_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}
}

impl PostgresData for Bson {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(
		_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}
}

impl PostgresData for Json {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(
		_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}
}

impl PostgresData for Enum {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(
		_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}
}

macro_rules! impl_parquet_record_array {
	($i:tt) => {
		impl PostgresData for [u8; $i] {
			fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
				name.unwrap().fmt(f)
			}
			fn decode(
				_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
			) -> Result<Self, Box<dyn Error + Sync + Send>> {
				unimplemented!()
			}
		}

		// Specialize the implementation to avoid passing a potentially large array around
		// on the stack.
		impl PostgresData for Box<[u8; $i]> {
			fn decode(
				_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
			) -> Result<Self, Box<dyn Error + Sync + Send>> {
				unimplemented!()
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
		impl<$($t,)*> PostgresData for ($($t,)*) where $($t: PostgresData,)* {
			fn query(_f: &mut fmt::Formatter, _name: Option<&Names<'_>>) -> fmt::Result {
				unimplemented!()
			}
			fn decode(_type_: &::postgres::types::Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
				unimplemented!()
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

impl PostgresData for Decimal {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(
		_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		unimplemented!()
	}
}

impl PostgresData for Group {
	fn query(_f: &mut fmt::Formatter, _name: Option<&Names<'_>>) -> fmt::Result {
		unimplemented!()
	}
	fn decode(
		_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		unimplemented!()
	}
}

impl<T> PostgresData for List<T>
where
	T: PostgresData,
{
	fn query(_f: &mut fmt::Formatter, _name: Option<&Names<'_>>) -> fmt::Result {
		unimplemented!()
	}
	fn decode(
		_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		unimplemented!()
	}
}

impl<K, V> PostgresData for Map<K, V>
where
	K: Hash + Eq + PostgresData,
	V: PostgresData,
{
	fn query(_f: &mut fmt::Formatter, _name: Option<&Names<'_>>) -> fmt::Result {
		unimplemented!()
	}
	fn decode(
		_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		unimplemented!()
	}
}

impl PostgresData for Date {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(
		type_: &::postgres::types::Type, buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		if !<NaiveDate as ::postgres::types::FromSql>::accepts(type_) {
			return Err(Into::into("invalid type"));
		}
		<NaiveDate as ::postgres::types::FromSql>::from_sql(
			type_,
			buf.ok_or_else(|| Box::new(::postgres::types::WasNull))?,
		)
		.map(Into::into)
	}
}

impl PostgresData for Time {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(
		_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		unimplemented!()
	}
}

impl PostgresData for Timestamp {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(
		_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		unimplemented!()
	}
}

impl PostgresData for Value {
	fn query(_f: &mut fmt::Formatter, _name: Option<&Names<'_>>) -> fmt::Result {
		unimplemented!()
	}
	fn decode(
		_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		unimplemented!()
	}
}

macro_rules! impl_data_for_record {
	($($t:ty : $pt:ty),*) => (
		$(
			#[allow(clippy::use_self)]
			impl PostgresData for $t {
				fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
					name.unwrap().fmt(f)
				}
				fn decode(type_: &::postgres::types::Type, buf: Option<&[u8]>) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
					if !<$pt as ::postgres::types::FromSql>::accepts(type_) {
						return Err(Into::into("invalid type"));
					}
					#[allow(trivial_numeric_casts)]
					<$pt as ::postgres::types::FromSql>::from_sql(type_, buf.ok_or_else(||Box::new(::postgres::types::WasNull))?).map(|x|x as Self)
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

impl<T> PostgresData for Option<T>
where
	T: PostgresData,
{
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		T::query(f, name)
	}
	fn decode(
		type_: &::postgres::types::Type, buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		match buf {
			Some(buf) => T::decode(type_, Some(buf)).map(Some),
			None => Ok(None),
		}
	}
}
