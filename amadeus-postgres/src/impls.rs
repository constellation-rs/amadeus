use super::{Names, PostgresData};
use amadeus_types::{
	Bson, Date, DateTime, DateTimeWithoutTimezone, DateWithoutTimezone, Decimal, Enum, Group, IpAddr, Json, Map, Time, TimeWithoutTimezone, Timezone, Url, Value, Webpage
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use postgres::types::{FromSql, Type, WasNull};
use std::{
	error::Error, fmt::{self, Display}, hash::Hash
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

/// BYTEA
impl PostgresData for Vec<u8> {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}
}

impl PostgresData for Bson {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}
}

impl PostgresData for Json {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}
}

impl PostgresData for Enum {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}
}

impl PostgresData for Url {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}
}

impl PostgresData for Webpage<'static> {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}
}

impl PostgresData for IpAddr {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}
}

impl PostgresData for Decimal {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}
}

impl PostgresData for Group {
	fn query(_f: &mut fmt::Formatter, _name: Option<&Names<'_>>) -> fmt::Result {
		unimplemented!()
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}
}

impl<T> PostgresData for Vec<T>
where
	T: PostgresData,
{
	default fn query(_f: &mut fmt::Formatter, _name: Option<&Names<'_>>) -> fmt::Result {
		unimplemented!()
	}
	default fn decode(
		_type_: &Type, _buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn Error + Sync + Send>> {
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
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}
}

impl PostgresData for Date {
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
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
		unimplemented!()
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
		unimplemented!()
	}
}

impl PostgresData for Value {
	fn query(_f: &mut fmt::Formatter, _name: Option<&Names<'_>>) -> fmt::Result {
		unimplemented!()
	}
	fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}
}

// Implement PostgresData for common array lengths, copied from arrayvec
macro_rules! array {
	($($i:tt)*) => {
		$(impl PostgresData for [u8; $i] {
			fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
				name.unwrap().fmt(f)
			}
			fn decode(
				_type_: &Type, _buf: Option<&[u8]>,
			) -> Result<Self, Box<dyn Error + Sync + Send>> {
				unimplemented!()
			}
		}

		// Specialize the implementation to avoid passing a potentially large array around
		// on the stack.
		impl PostgresData for Box<[u8; $i]> {
			fn decode(
				_type_: &Type, _buf: Option<&[u8]>,
			) -> Result<Self, Box<dyn Error + Sync + Send>> {
				unimplemented!()
			}
		})*
	};
}
amadeus_types::array!(array);

// Implement PostgresData for tuples up to length 32.
macro_rules! tuple {
	($len:tt $($t:ident $i:tt)*) => (
		impl<$($t,)*> PostgresData for ($($t,)*) where $($t: PostgresData,)* {
			fn query(_f: &mut fmt::Formatter, _name: Option<&Names<'_>>) -> fmt::Result {
				unimplemented!()
			}
			fn decode(_type_: &Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn Error + Sync + Send>> {
				unimplemented!()
			}
		}
	);
}
amadeus_types::tuple!(tuple);
