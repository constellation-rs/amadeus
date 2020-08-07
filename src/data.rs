use serde::{Deserialize, Serialize};
use std::{
	borrow::Cow, cmp::Ordering, collections::HashMap, fmt::Debug, hash::{BuildHasher, Hash, Hasher}, mem
};

#[cfg(feature = "parquet")]
use amadeus_parquet::ParquetData;
#[cfg(feature = "postgres")]
use amadeus_postgres::PostgresData;
#[cfg(feature = "amadeus-serde")]
use amadeus_serde::SerdeData;

#[cfg(not(feature = "parquet"))]
use std::any::Any as ParquetData;
#[cfg(not(feature = "postgres"))]
use std::any::Any as PostgresData;
#[cfg(not(feature = "amadeus-serde"))]
use std::any::Any as SerdeData;

pub use amadeus_derive::Data;
pub use amadeus_types::{
	AmadeusOrd, Bson, Date, DateTime, DateTimeWithoutTimezone, DateWithoutTimezone, Decimal, Downcast, DowncastFrom, Enum, Group, IpAddr, Json, List, Time, TimeWithoutTimezone, Timezone, Url, Value, Webpage
};

pub trait Data:
	Clone
	+ amadeus_types::Data
	+ AmadeusOrd
	+ ParquetData
	+ PostgresData
	+ SerdeData
	+ DowncastFrom<Value>
	+ Into<Value>
	+ Debug
	+ Send
	+ 'static
{
	fn size(&self) -> usize {
		mem::size_of::<Self>() + self.heap()
	}
	fn heap(&self) -> usize;
	fn cast<D: Data>(self) -> Result<D, CastError> {
		self.into().downcast().map_err(|_| CastError)
	}
	fn eq<D: Data>(self, other: D) -> bool {
		self.into() == other.into()
	}
	fn partial_cmp<D: Data>(self, other: D) -> Option<Ordering> {
		self.into().partial_cmp(other.into())
	}
	fn hash<H: Hasher>(self, state: &mut H) {
		<Value as Hash>::hash(&self.into(), state)
	}
}

pub struct CastError;

// impl<T> PartialEq<T> for dyn Data {
// 	fn eq(&self, other: &T) -> bool {
// 		other.eq(self)
// 	}
// }

impl<T> Data for Option<T>
where
	T: Data,
{
	fn heap(&self) -> usize {
		self.as_ref().map_or(0, Data::heap)
	}
}
impl<T> Data for Box<T>
where
	T: Data,
{
	fn heap(&self) -> usize {
		mem::size_of::<T>() + (**self).heap()
	}
}
impl<T> Data for List<T>
where
	T: Data,
{
	fn heap(&self) -> usize {
		todo!()
		// self.capacity() * mem::size_of::<T>() + self.iter()
	}
}
impl<K, V, S> Data for HashMap<K, V, S>
where
	K: Hash + Eq + Data,
	V: Data,
	S: BuildHasher + Clone + Default + Send + 'static,
{
	fn heap(&self) -> usize {
		self.capacity() * mem::size_of::<(K, V)>()
			+ self.iter().map(|(k, v)| k.heap() + v.heap()).sum::<usize>()
	}
}

macro_rules! impl_data {
	($($t:ty)*) => ($(
		impl Data for $t {
			fn heap(&self) -> usize {
				0
			}
		}
	)*);
}
impl_data!(bool u8 i8 u16 i16 u32 i32 u64 i64 f32 f64 Decimal Group Date DateWithoutTimezone Time TimeWithoutTimezone DateTime DateTimeWithoutTimezone Timezone IpAddr);

macro_rules! impl_data {
	($($t:ty)*) => ($(
		impl Data for $t {
			fn heap(&self) -> usize {
				self.capacity()
			}
		}
	)*);
}
impl_data!(String Bson Json Enum);

impl Data for Url {
	fn heap(&self) -> usize {
		self.as_str().len()
	}
}
impl Data for Webpage<'static> {
	fn heap(&self) -> usize {
		self.url.heap()
			+ if let Cow::Owned(slice) = &self.contents {
				slice.capacity()
			} else {
				0
			}
	}
}

impl Data for Value {
	fn heap(&self) -> usize {
		match self {
			Self::Bool(value) => value.heap(),
			Self::U8(value) => value.heap(),
			Self::I8(value) => value.heap(),
			Self::U16(value) => value.heap(),
			Self::I16(value) => value.heap(),
			Self::U32(value) => value.heap(),
			Self::I32(value) => value.heap(),
			Self::U64(value) => value.heap(),
			Self::I64(value) => value.heap(),
			Self::F32(value) => value.heap(),
			Self::F64(value) => value.heap(),
			Self::Date(value) => value.heap(),
			Self::DateWithoutTimezone(value) => value.heap(),
			Self::Time(value) => value.heap(),
			Self::TimeWithoutTimezone(value) => value.heap(),
			Self::DateTime(value) => value.heap(),
			Self::DateTimeWithoutTimezone(value) => value.heap(),
			Self::Timezone(value) => value.heap(),
			Self::Decimal(value) => value.heap(),
			Self::Bson(value) => value.heap(),
			Self::String(value) => value.heap(),
			Self::Json(value) => value.heap(),
			Self::Enum(value) => value.heap(),
			Self::Url(value) => value.heap(),
			Self::Webpage(value) => value.heap(),
			Self::IpAddr(value) => value.heap(),
			Self::List(value) => value.heap(),
			Self::Map(value) => value.heap(),
			Self::Group(value) => value.heap(),
			Self::Option(value) => value
				.as_ref()
				.map_or(0, |value| value.as_value(|value| value.heap())),
		}
	}
}

// Implement Record for common array lengths.
macro_rules! array {
	($($i:tt)*) => {$(
		impl Data for [u8; $i] {
			fn heap(&self) -> usize {
				0
			}
		}
		// TODO: fix parquet then impl<T> Data for [T; $i] where T: Data {}
	)*};
}
amadeus_types::array!(array);

// Implement Record on tuples up to length 12.
macro_rules! tuple {
	($len:tt $($t:ident $i:tt)*) => {
		impl<$($t,)*> Data for ($($t,)*) where $($t: Data,)* {
			fn heap(&self) -> usize {
				$(self.$i.heap() + )* 0
			}
		}
	};
}
amadeus_types::tuple!(tuple);

#[cfg(feature = "amadeus-serde")]
#[doc(hidden)]
pub mod serde_data {
	use super::SerdeData;
	use serde::{Deserializer, Serializer};

	pub fn serialize<T, S>(self_: &T, serializer: S) -> Result<S::Ok, S::Error>
	where
		T: SerdeData + ?Sized,
		S: Serializer,
	{
		self_.serialize(serializer)
	}
	pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
	where
		T: SerdeData,
		D: Deserializer<'de>,
	{
		T::deserialize(deserializer, None)
	}
}

#[derive(
	amadeus_derive::Data, Clone, Eq, PartialEq, PartialOrd, Hash, Serialize, Deserialize, Debug,
)]
#[amadeus(crate = "crate")]
pub struct CloudfrontRow {
	pub time: DateTime,
	pub edge_location: String,
	pub response_bytes: u64,
	pub remote_ip: IpAddr,
	// #[serde(with = "http_serde")]
	// pub method: Method,
	pub host: String,
	pub url: Url,
	// #[serde(with = "http_serde")]
	// pub status: Option<StatusCode>,
	pub user_agent: Option<String>,
	pub referer: Option<String>,
	pub cookie: Option<String>,
	pub result_type: String,
	pub request_id: String,
	pub request_bytes: u64,
	// pub time_taken: Duration,
	pub forwarded_for: Option<String>,
	pub ssl_protocol_cipher: Option<(String, String)>,
	pub response_result_type: String,
	pub http_version: String,
	pub fle_status: Option<String>,
	pub fle_encrypted_fields: Option<String>,
}
#[cfg(feature = "aws")]
impl From<amadeus_aws::CloudfrontRow> for CloudfrontRow {
	fn from(from: amadeus_aws::CloudfrontRow) -> Self {
		Self {
			time: from.time,
			edge_location: from.edge_location,
			response_bytes: from.response_bytes,
			remote_ip: from.remote_ip,
			// method: from.method,
			host: from.host,
			url: from.url,
			// status: from.status,
			user_agent: from.user_agent,
			referer: from.referer,
			cookie: from.cookie,
			result_type: from.result_type,
			request_id: from.request_id,
			request_bytes: from.request_bytes,
			// time_taken: from.time_taken,
			forwarded_for: from.forwarded_for,
			ssl_protocol_cipher: from.ssl_protocol_cipher,
			response_result_type: from.response_result_type,
			http_version: from.http_version,
			fle_status: from.fle_status,
			fle_encrypted_fields: from.fle_encrypted_fields,
		}
	}
}
