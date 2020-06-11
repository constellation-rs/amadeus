use serde::{Deserialize, Serialize};
use std::{
	cmp::Ordering, collections::HashMap, fmt::Debug, hash::{BuildHasher, Hash, Hasher}
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

impl<T> Data for Option<T> where T: Data {}
impl<T> Data for Box<T> where T: Data {}
impl<T> Data for List<T> where T: Data {}
impl<K, V, S> Data for HashMap<K, V, S>
where
	K: Hash + Eq + Data,
	V: Data,
	S: BuildHasher + Clone + Default + Send + 'static,
{
}

macro_rules! impl_data {
	($($t:ty)*) => ($(
		impl Data for $t {}
	)*);
}
impl_data!(bool u8 i8 u16 i16 u32 i32 u64 i64 f32 f64 String Bson Json Enum Decimal Group Date DateWithoutTimezone Time TimeWithoutTimezone DateTime DateTimeWithoutTimezone Timezone Value Webpage<'static> Url IpAddr);

// Implement Record for common array lengths.
macro_rules! array {
	($($i:tt)*) => {$(
		impl Data for [u8; $i] {}
		// TODO: impl<T> Data for [T; $i] where T: Data {}
	)*};
}
amadeus_types::array!(array);

// Implement Record on tuples up to length 12.
macro_rules! tuple {
	($len:tt $($t:ident $i:tt)*) => {
		impl<$($t,)*> Data for ($($t,)*) where $($t: Data,)* {}
	};
}
amadeus_types::tuple!(tuple);

#[cfg(feature = "amadeus-serde")]
#[doc(hidden)]
pub mod serde_data {
	use super::Data;
	use serde::{Deserializer, Serializer};

	pub fn serialize<T, S>(self_: &T, serializer: S) -> Result<S::Ok, S::Error>
	where
		T: Data + ?Sized,
		S: Serializer,
	{
		self_.serialize(serializer)
	}
	pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
	where
		T: Data,
		D: Deserializer<'de>,
	{
		T::deserialize(deserializer, None)
	}
}

#[derive(amadeus_derive::Data, Clone, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
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

// impl<T: ?Sized> Data for T where T: PartialEq + Eq + Clone + 'static {}

// pub trait DataSource: crate::dist_stream::DistributedStream<Item = <Self as DataSource>::Itemm> {
// 	type Itemm: Data;
// }

// impl<T: ?Sized> DataSource for T where T: crate::dist_stream::DistributedStream, <T as crate::dist_stream::DistributedStream>::Item: Data {
// 	type Itemm = <T as crate::dist_stream::DistributedStream>::Item;
// }

// pub trait DataSource
// where
// 	Self: amadeus_core::dist_stream::DistributedStream<Item = <Self as DataSource>::Item>,
// 	<Self as amadeus_core::dist_stream::DistributedStream>::Item: Data,
// {
// 	type Item;
// }

// impl<T: ?Sized> DataSource for T where T: crate::dist_stream::DistributedStream, <T as crate::dist_stream::DistributedStream>::Item: Data {
// 	// type Itemm = <T as crate::dist_stream::DistributedStream>::Item;
// 	type Item = <T as crate::dist_stream::DistributedStream>::Item;
// }
