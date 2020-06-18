use super::{Names, PostgresData};
use amadeus_types::{
	Bson, Date, DateTime, DateTimeWithoutTimezone, DateWithoutTimezone, Decimal, Enum, Group, IpAddr, Json, List, Time, TimeWithoutTimezone, Timezone, Url, Value, Webpage
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use postgres::types::{FromSql, Type, WasNull};
use std::{
	collections::HashMap, error::Error, fmt::{self, Display}, hash::{BuildHasher, Hash}
};

macro_rules! forward {
	($($t:ty : $pt:ty),*) => (
		$(
			#[allow(clippy::use_self)]
			impl PostgresData for $t {
				fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
					name.unwrap().fmt(f)
				}
				fn decode(type_: &Type, buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
					if !<$pt as FromSql>::accepts(type_) {
						return Err(Into::into("invalid type"));
					}
					#[allow(trivial_numeric_casts)]
					<$pt as FromSql>::from_sql(type_, buf.ok_or_else(||Box::new(WasNull))?).map(|x|x as Self)
				}
			}
		)*
	);
}
forward!(
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

impl<T> PostgresData for Option<T>
where
	T: PostgresData,
{
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		T::query(f, name)
	}
	fn decode(type_: &Type, buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		match buf {
			Some(buf) => T::decode(type_, Some(buf)).map(Some),
			None => Ok(None),
		}
	}
}

impl PostgresData for Bson {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
}

impl PostgresData for Json {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
}

impl PostgresData for Enum {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
}

impl PostgresData for Url {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
}

impl PostgresData for Webpage<'static> {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
}

impl PostgresData for IpAddr {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
}

impl PostgresData for Decimal {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
}

impl PostgresData for Group {
	fn query(_f: &mut fmt::Formatter, _name: Option<&Names<'_>>) -> fmt::Result {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
}

impl<T> PostgresData for List<T>
where
	T: PostgresData,
{
	default fn query(_f: &mut fmt::Formatter, _name: Option<&Names<'_>>) -> fmt::Result {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
	default fn decode(
		_type_: &Type, _buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn Error + Sync + Send>> {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
}
/// BYTEA
impl PostgresData for List<u8> {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
}

impl<K, V, S> PostgresData for HashMap<K, V, S>
where
	K: Hash + Eq + PostgresData,
	V: PostgresData,
	S: BuildHasher + Clone + 'static,
{
	fn query(_f: &mut fmt::Formatter, _name: Option<&Names<'_>>) -> fmt::Result {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
}

impl PostgresData for Date {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
}

impl PostgresData for DateWithoutTimezone {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(type_: &Type, buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		if !<NaiveDate as FromSql>::accepts(type_) {
			return Err(Into::into("invalid type"));
		}
		<NaiveDate as FromSql>::from_sql(type_, buf.ok_or_else(|| Box::new(WasNull))?)
			.map(|date| Self::from_chrono(&date))
	}
}

impl PostgresData for Time {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
}

impl PostgresData for TimeWithoutTimezone {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(type_: &Type, buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		if !<NaiveTime as FromSql>::accepts(type_) {
			return Err(Into::into("invalid type"));
		}
		<NaiveTime as FromSql>::from_sql(type_, buf.ok_or_else(|| Box::new(WasNull))?)
			.map(|date| Self::from_chrono(&date))
	}
}

impl PostgresData for DateTime {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(type_: &Type, buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		if !<chrono::DateTime<chrono::Utc> as FromSql>::accepts(type_) {
			return Err(Into::into("invalid type"));
		}
		<chrono::DateTime<chrono::Utc> as FromSql>::from_sql(
			type_,
			buf.ok_or_else(|| Box::new(WasNull))?,
		)
		.map(|date| Self::from_chrono(&date))
	}
}

impl PostgresData for DateTimeWithoutTimezone {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(type_: &Type, buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		if !<NaiveDateTime as FromSql>::accepts(type_) {
			return Err(Into::into("invalid type"));
		}
		<NaiveDateTime as FromSql>::from_sql(type_, buf.ok_or_else(|| Box::new(WasNull))?)
			.map(|date| Self::from_chrono(&date))
	}
}

impl PostgresData for Timezone {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
}

impl PostgresData for Value {
	fn query(_f: &mut fmt::Formatter, _name: Option<&Names<'_>>) -> fmt::Result {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
	}
}

// Implement PostgresData for common array lengths.
macro_rules! array {
	($($i:tt)*) => {$(
		impl<T> PostgresData for [T; $i]
		where
			T: PostgresData
		{
			fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
				name.unwrap().fmt(f)
			}
			fn decode(
				_type_: &Type, _buf: Option<&[u8]>,
			) -> Result<Self, Box<dyn Error + Sync + Send>> {
				todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
			}
		}

		// Specialize the implementation to avoid passing a potentially large array around
		// on the stack.
		impl<T> PostgresData for Box<[T; $i]>
		where
			T: PostgresData
		{
			fn decode(
				_type_: &Type, _buf: Option<&[u8]>,
			) -> Result<Self, Box<dyn Error + Sync + Send>> {
				todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
			}
		}
	)*};
}
amadeus_types::array!(array);

// Implement PostgresData for tuples up to length 12.
macro_rules! tuple {
	($len:tt $($t:ident $i:tt)*) => (
		impl<$($t,)*> PostgresData for ($($t,)*) where $($t: PostgresData,)* {
			fn query(_f: &mut fmt::Formatter, _name: Option<&Names<'_>>) -> fmt::Result {
				todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
			}
			fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
				todo!("Tracking at https://github.com/constellation-rs/amadeus/issues/63")
			}
		}
	);
}
amadeus_types::tuple!(tuple);
