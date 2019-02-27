//! Implement [`Record`] for [`Map`].

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
	borrow::Borrow, collections::{hash_map, HashMap}, fmt::{self, Debug}, hash::Hash, mem::transmute
};

use super::{super::Data, MapReader, SchemaIncomplete, Value};
use parquet::{
	basic::Repetition, column::reader::ColumnReader, errors::ParquetError, schema::types::{ColumnPath, Type}
};
// use parquet::{
//     basic::{LogicalType, Repetition},
//     column::reader::ColumnReader,
//     errors::{ParquetError, Result},
//     record::{
//         reader::{KeyValueReader, MapReader},
//         schemas::MapSchema,
//         Reader, Record,
//     },
//     schema::types::{ColumnPath, Type},
// };

/// [`Map<K, V>`](Map) corresponds to the [Map logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps).
#[derive(Clone, Eq, Serialize, Deserialize)]
pub struct Map<K: Hash + Eq, V>(pub(in super::super) HashMap<K, V>);

impl<K, V> Map<K, V>
where
	K: Hash + Eq,
{
	/// Returns a reference to the value corresponding to the key.
	pub fn get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
	where
		K: Borrow<Q>,
		Q: Hash + Eq,
	{
		self.0.get(k)
	}

	/// Returns an iterator over the `(ref key, ref value)` pairs of the Map.
	pub fn iter(&self) -> hash_map::Iter<'_, K, V> {
		self.0.iter()
	}

	/// Creates an iterator over the `(key, value)` pairs of the Map.
	pub fn into_iter(self) -> hash_map::IntoIter<K, V> {
		self.0.into_iter()
	}
}
impl<K, V> Data for Map<K, V>
where
	K: Hash + Eq + Data,
	V: Data,
{
	type ParquetSchema = <parquet::record::types::Map<
		crate::source::parquet::Record<K>,
		crate::source::parquet::Record<V>,
	> as parquet::record::Record>::Schema;
	existential type ParquetReader: parquet::record::Reader<Item = Self>;
	// type ParquetReader =
	//     IntoReader<<parquet::record::types::Map<crate::source::parquet::Record<K>,crate::source::parquet::Record<V>> as parquet::record::Record>::Reader, Self>;

	fn postgres_query(
		f: &mut fmt::Formatter, name: Option<&crate::source::postgres::Names<'_>>,
	) -> fmt::Result {
		unimplemented!()
	}
	fn postgres_decode(
		type_: &::postgres::types::Type, buf: Option<&[u8]>,
	) -> Result<Self, Box<std::error::Error + Sync + Send>> {
		unimplemented!()
	}

	fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		// self.serialize(serializer)
		unimplemented!()
	}
	fn serde_deserialize<'de, D>(
		deserializer: D, schema: Option<SchemaIncomplete>,
	) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		// Self::deserialize(deserializer)
		unimplemented!()
	}

	fn parquet_parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::ParquetSchema), ParquetError> {
		<parquet::record::types::Map<
			crate::source::parquet::Record<K>,
			crate::source::parquet::Record<V>,
		> as parquet::record::Record>::parse(schema, repetition)
	}
	fn parquet_reader(
		schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::ParquetReader {
		MapReader::new(
			<parquet::record::types::Map<
				crate::source::parquet::Record<K>,
				crate::source::parquet::Record<V>,
			> as parquet::record::Record>::reader(
				schema, path, def_level, rep_level, paths, batch_size
			),
			|map| {
				Ok(unsafe {
					transmute::<
						parquet::record::types::Map<
							crate::source::parquet::Record<K>,
							crate::source::parquet::Record<V>,
						>,
						parquet::record::types::Map<K, V>,
					>(map)
					.into()
				})
			},
		)
	}
}
// impl From<Map> for parquet::record::types::Map {
//     fn from(map: Map) -> Self {
//         unimplemented!()
//     }
// }
impl<K, V, K1, V1> From<parquet::record::types::Map<K1, V1>> for Map<K, V>
where
	K: Hash + Eq,
	K1: Hash + Eq + Into<K>,
	V1: Into<V>,
{
	fn from(map: parquet::record::types::Map<K1, V1>) -> Self {
		<_ as Into<HashMap<K1, V1>>>::into(map)
			.into_iter()
			.map(|(k, v)| (k.into(), v.into()))
			.collect::<HashMap<_, _>>()
			.into()
	}
}
impl<K, V> From<HashMap<K, V>> for Map<K, V>
where
	K: Hash + Eq,
{
	fn from(hashmap: HashMap<K, V>) -> Self {
		Map(hashmap)
	}
}
impl<K, V> Into<HashMap<K, V>> for Map<K, V>
where
	K: Hash + Eq,
{
	fn into(self) -> HashMap<K, V> {
		self.0
	}
}
impl<K, V, V1> PartialEq<Map<K, V1>> for Map<K, V>
where
	K: Eq + Hash,
	V: PartialEq<V1>,
{
	fn eq(&self, other: &Map<K, V1>) -> bool {
		if self.0.len() != other.0.len() {
			return false;
		}

		self.0
			.iter()
			.all(|(key, value)| other.0.get(key).map_or(false, |v| *value == *v))
	}
}
impl<K, V> Debug for Map<K, V>
where
	K: Hash + Eq + Debug,
	V: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_map().entries(self.iter()).finish()
	}
}
