use std::{
	cmp::Ordering, fmt::Debug, hash::{Hash, Hasher}
};

pub use amadeus_derive::Data;
#[cfg(feature = "parquet")]
use amadeus_parquet::ParquetData;
#[cfg(feature = "postgres")]
use amadeus_postgres::PostgresData;
#[cfg(feature = "amadeus-serde")]
use amadeus_serde::SerdeData;
pub use amadeus_types as types;
pub use amadeus_types::{
	Bson, Date, Decimal, Downcast, DowncastImpl, Enum, Group, Json, List, Map, Time, Timestamp, Value
};

#[cfg(not(feature = "parquet"))]
use std::any::Any as ParquetData;
#[cfg(not(feature = "postgres"))]
use std::any::Any as PostgresData;
#[cfg(not(feature = "amadeus-serde"))]
use std::any::Any as SerdeData;

pub trait Data:
	Clone
	+ PartialEq
	+ PartialOrd
	+ ParquetData
	+ PostgresData
	+ SerdeData
	+ DowncastImpl<Value>
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

#[derive(amadeus_derive::Data, Clone, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
#[amadeus(crate = "crate")]
pub struct CloudfrontRow {
	// pub time: DateTime<Utc>,
	pub edge_location: String,
	pub response_bytes: u64,
	// pub remote_ip: net::IpAddr,
	// #[serde(with = "http_serde")]
	// pub method: Method,
	pub host: String,
	// pub url: Url,
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
			edge_location: from.edge_location,
			response_bytes: from.response_bytes,
			host: from.host,
			user_agent: from.user_agent,
			referer: from.referer,
			cookie: from.cookie,
			result_type: from.result_type,
			request_id: from.request_id,
			request_bytes: from.request_bytes,
			forwarded_for: from.forwarded_for,
			ssl_protocol_cipher: from.ssl_protocol_cipher,
			response_result_type: from.response_result_type,
			http_version: from.http_version,
			fle_status: from.fle_status,
			fle_encrypted_fields: from.fle_encrypted_fields,
		}
	}
}

pub struct CastError;

// impl<T> PartialEq<T> for dyn Data {
// 	fn eq(&self, other: &T) -> bool {
// 		other.eq(self)
// 	}
// }

// `Vec<u8>` corresponds to the `binary`/`byte_array` and `fixed_len_byte_array` physical
// types.
impl Data for Vec<u8> {}
impl Data for Bson {}
impl Data for Json {}
impl Data for Enum {}
impl Data for Decimal {}
impl Data for Group {}
impl<T> Data for List<T> where T: Data {}
impl<K, V> Data for Map<K, V>
where
	K: Hash + Eq + Data,
	V: Data,
{
}
impl Data for Date {}
impl Data for Time {}
impl Data for Timestamp {}
impl Data for Value {}

impl<T> Data for Option<T> where T: Data {}
impl<T> Data for Box<T> where T: Data {}

macro_rules! impl_data_for_record {
	($($t:ty)*) => (
		$(impl Data for $t {})*
	);
}
impl_data_for_record!(bool u8 i8 u16 i16 u32 i32 u64 i64 f32 f64 String);

macro_rules! impl_parquet_record_array {
	($i:tt) => {
		impl Data for [u8; $i] {}
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

macro_rules! impl_parquet_record_tuple {
	($($t:ident $i:tt)*) => {
		impl<$($t,)*> Data for ($($t,)*) where $($t: Data,)* {}
	};
}

impl_parquet_record_tuple!();
impl_parquet_record_tuple!(A 0);
impl_parquet_record_tuple!(A 0 B 1);
impl_parquet_record_tuple!(A 0 B 1 C 2);
impl_parquet_record_tuple!(A 0 B 1 C 2 D 3);
impl_parquet_record_tuple!(A 0 B 1 C 2 D 3 E 4);
impl_parquet_record_tuple!(A 0 B 1 C 2 D 3 E 4 F 5);
impl_parquet_record_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6);
impl_parquet_record_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7);
impl_parquet_record_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8);
impl_parquet_record_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9);
impl_parquet_record_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10);
impl_parquet_record_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11);

#[cfg(feature = "amadeus-serde")]
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

// impl<T: ?Sized> Data for T where T: PartialEq + Eq + Clone + 'static {}

// pub trait DataSource: crate::dist_iter::DistributedIterator<Item = <Self as DataSource>::Itemm> {
// 	type Itemm: Data;
// }

// impl<T: ?Sized> DataSource for T where T: crate::dist_iter::DistributedIterator, <T as crate::dist_iter::DistributedIterator>::Item: Data {
// 	type Itemm = <T as crate::dist_iter::DistributedIterator>::Item;
// }

// pub trait DataSource
// where
// 	Self: amadeus_core::dist_iter::DistributedIterator<Item = <Self as DataSource>::Item>,
// 	<Self as amadeus_core::dist_iter::DistributedIterator>::Item: Data,
// {
// 	type Item;
// }

// impl<T: ?Sized> DataSource for T where T: crate::dist_iter::DistributedIterator, <T as crate::dist_iter::DistributedIterator>::Item: Data {
// 	// type Itemm = <T as crate::dist_iter::DistributedIterator>::Item;
// 	type Item = <T as crate::dist_iter::DistributedIterator>::Item;
// }

// mod tuple {
// 	use super::{
// 		data::{SerdeDeserialize, SerdeSerialize}, types::SchemaIncomplete, Data
// 	};
// 	use std::{collections::HashMap, fmt, marker::PhantomData};

// 	use amadeus_parquet::internal::{
// 		basic::Repetition, column::reader::ColumnReader, errors::Result as ParquetResult, record::Record as ParquetRecord, schema::types::{ColumnPath, Type}
// 	};
// 	use serde::{Deserialize, Deserializer, Serialize, Serializer};

// 	/// A Reader that wraps a Reader, wrapping the read value in a `Record`.
// 	pub struct TupleXxxReader<T, U>(U, PhantomData<fn(T)>);

// 	/// Macro to implement [`Reader`] on tuples up to length 32.
// 	macro_rules! impl_parquet_record_tuple {
// 		($len:tt $($t:ident $i:tt)*) => (
// 			// // Tuples correspond to Parquet groups with an equal number of fields with corresponding types.
// 			impl<$($t,)*> super::types::Downcast<($($t,)*)> for super::types::Value where super::types::Value: $(super::types::Downcast<$t> +)* {
// 				fn downcast(self) -> Result<($($t,)*), super::types::DowncastError> {
// 					#[allow(unused_mut,unused_variables)]
// 					let mut fields = self.into_group()?.0.into_iter();
// 					if fields.len() != $len {
// 						return Err(super::types::DowncastError{from:"group",to:concat!("tuple of length ", $len)});
// 					}
// 					Ok(($({let _ = $i;fields.next().unwrap().downcast()?},)*))
// 				}
// 			}
// 			// impl<$($t,)*> Downcast<($($t,)*)> for Group where Value: $(Downcast<$t> +)* {
// 			// 	fn downcast(self) -> Result<($($t,)*)> {
// 			// 		#[allow(unused_mut,unused_variables)]
// 			// 		let mut fields = self.0.into_iter();
// 			// 		if fields.len() != $len {
// 			// 			return Err(ParquetError::General(format!("Can't downcast group of length {} to tuple of length {}", fields.len(), $len)));
// 			// 		}
// 			// 		Ok(($({let _ = $i;fields.next().unwrap().downcast()?},)*))
// 			// 	}
// 			// }
// 			// impl<$($t,)*> PartialEq<($($t,)*)> for Value where Value: $(PartialEq<$t> +)* {
// 			// 	#[allow(unused_variables)]
// 			// 	fn eq(&self, other: &($($t,)*)) -> bool {
// 			// 		self.is_group() $(&& self.as_group().unwrap()[$i] == other.$i)*
// 			// 	}
// 			// }
// 			// impl<$($t,)*> PartialEq<($($t,)*)> for Group where Value: $(PartialEq<$t> +)* {
// 			// 	#[allow(unused_variables)]
// 			// 	fn eq(&self, other: &($($t,)*)) -> bool {
// 			// 		$(self[$i] == other.$i && )* true
// 			// 	}
// 			// }
// 			// impl<$($t,)*> Downcast<TupleSchema<($((String,$t,),)*)>> for ValueSchema where ValueSchema: $(Downcast<$t> +)* {
// 			// 	fn downcast(self) -> Result<TupleSchema<($((String,$t,),)*)>> {
// 			// 		let group = self.into_group()?;
// 			// 		#[allow(unused_mut,unused_variables)]
// 			// 		let mut fields = group.0.into_iter();
// 			// 		let mut names = vec![None; group.1.len()];
// 			// 		for (name,&index) in group.1.iter() {
// 			// 			names[index].replace(name.to_owned());
// 			// 		}
// 			// 		#[allow(unused_mut,unused_variables)]
// 			// 		let mut names = names.into_iter().map(Option::unwrap);
// 			// 		Ok(TupleSchema(($({let _ = $i;(names.next().unwrap(),fields.next().unwrap().downcast()?)},)*)))
// 			// 	}
// 			// }
// 		);
// 	}

// 	impl_parquet_record_tuple!(0);
// 	impl_parquet_record_tuple!(1 A 0);
// 	impl_parquet_record_tuple!(2 A 0 B 1);
// 	impl_parquet_record_tuple!(3 A 0 B 1 C 2);
// 	impl_parquet_record_tuple!(4 A 0 B 1 C 2 D 3);
// 	impl_parquet_record_tuple!(5 A 0 B 1 C 2 D 3 E 4);
// 	impl_parquet_record_tuple!(6 A 0 B 1 C 2 D 3 E 4 F 5);
// 	impl_parquet_record_tuple!(7 A 0 B 1 C 2 D 3 E 4 F 5 G 6);
// 	impl_parquet_record_tuple!(8 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7);
// 	impl_parquet_record_tuple!(9 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8);
// 	impl_parquet_record_tuple!(10 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9);
// 	impl_parquet_record_tuple!(11 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10);
// 	impl_parquet_record_tuple!(12 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11);
// 	// impl_parquet_record_tuple!(13 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12);
// 	// impl_parquet_record_tuple!(14 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13);
// 	// impl_parquet_record_tuple!(15 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14);
// 	// impl_parquet_record_tuple!(16 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15);
// 	// impl_parquet_record_tuple!(17 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16);
// 	// impl_parquet_record_tuple!(18 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17);
// 	// impl_parquet_record_tuple!(19 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18);
// 	// impl_parquet_record_tuple!(20 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19);
// 	// impl_parquet_record_tuple!(21 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20);
// 	// impl_parquet_record_tuple!(22 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21);
// 	// impl_parquet_record_tuple!(23 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22);
// 	// impl_parquet_record_tuple!(24 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23);
// 	// impl_parquet_record_tuple!(25 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24);
// 	// impl_parquet_record_tuple!(26 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25);
// 	// impl_parquet_record_tuple!(27 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26);
// 	// impl_parquet_record_tuple!(28 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27);
// 	// impl_parquet_record_tuple!(29 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28);
// 	// impl_parquet_record_tuple!(30 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29);
// 	// impl_parquet_record_tuple!(31 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30);
// 	// impl_parquet_record_tuple!(32 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30 AF 31);
// }
