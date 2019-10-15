//! Implement [`Record`] for [`Map`].

use serde::{Deserialize, Serialize};
use std::{
	borrow::Borrow, cmp::Ordering, collections::{hash_map, HashMap}, fmt::{self, Debug}, hash::Hash
};

// use internal::{
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
pub struct Map<K: Hash + Eq, V>(HashMap<K, V>);

impl<K, V> Map<K, V>
where
	K: Hash + Eq,
{
	pub fn len(&self) -> usize {
		self.0.len()
	}
	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

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
}
impl<K, V> IntoIterator for Map<K, V>
where
	K: Hash + Eq,
{
	type Item = (K, V);
	type IntoIter = hash_map::IntoIter<K, V>;

	/// Creates an iterator over the `(key, value)` pairs of the Map.
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}
// impl From<Map> for internal::record::types::Map {
//     fn from(map: Map) -> Self {
//         unimplemented!()
//     }
// }
impl<K, V> From<HashMap<K, V>> for Map<K, V>
where
	K: Hash + Eq,
{
	fn from(hashmap: HashMap<K, V>) -> Self {
		Self(hashmap)
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
impl<K, V, V1> PartialOrd<Map<K, V1>> for Map<K, V>
where
	K: Eq + Hash,
	V: PartialOrd<V1>,
{
	fn partial_cmp(&self, other: &Map<K, V1>) -> Option<Ordering> {
		if self.0.len() != other.0.len() {
			return None;
		}
		None
		// TODO
		// self.0
		// 	.iter()
		// 	.all(|(key, value)| other.0.get(key).map_or(false, |v| *value.partial_cmp(*v)))
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
