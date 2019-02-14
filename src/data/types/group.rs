//! Implement [`Record`] for [`Group`] aka [`Row`].

use fxhash::FxBuildHasher;
use linked_hash_map::LinkedHashMap;
use serde::{
	de::{MapAccess, SeqAccess, Visitor}, ser::{SerializeStruct, SerializeTupleStruct}, Deserialize, Deserializer, Serialize, Serializer
};
use std::{
	cmp, collections::HashMap, fmt::{self, Debug}, ops::Index, slice::SliceIndex, str, sync::Arc, vec
};

use super::{
	super::{Data, SerdeDeserialize, SerdeSerialize}, IntoReader, Schema, Value
};
use parquet::{
	basic::Repetition, column::reader::ColumnReader, errors::ParquetError, schema::types::{ColumnPath, Type}
};
// use parquet::{
//     basic::Repetition,
//     column::reader::ColumnReader,
//     errors::{ParquetError, Result},
//     record::{
//         reader::GroupReader,
//         schemas::{GroupSchema, ValueSchema},
//         types::Value,
//         Record,
//     },
//     schema::types::{ColumnPath, Type},
// };

/// A Rust type corresponding to Parquet groups of fields.
#[derive(Clone, PartialEq)]
pub struct Group(
	pub(crate) Vec<Value>,
	pub(crate) Option<Arc<LinkedHashMap<String, usize, FxBuildHasher>>>,
);

impl Group {
	#[doc(hidden)]
	pub fn new(
		fields: Vec<Value>, field_names: Option<Arc<LinkedHashMap<String, usize, FxBuildHasher>>>,
	) -> Self {
		Group(fields, field_names)
	}
	/// Get a reference to the value belonging to a particular field name. Returns `None`
	/// if the field name doesn't exist.
	pub fn get(&self, k: &str) -> Option<&Value> {
		self.1.as_ref()?.get(k).map(|&offset| &self.0[offset])
	}
	#[doc(hidden)]
	pub fn into_fields(self) -> Vec<Value> {
		self.0
	}
	#[doc(hidden)]
	pub fn field_names(&self) -> Option<Arc<LinkedHashMap<String, usize, FxBuildHasher>>> {
		self.1.clone()
	}
}

impl Serialize for Group {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		Self::serde_serialize(self, serializer)
	}
}
impl<'de> Deserialize<'de> for Group {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		Self::serde_deserialize(deserializer, None)
	}
}

impl Data for Group {
	type ParquetSchema = <parquet::record::types::Group as parquet::record::Record>::Schema;
	type ParquetReader =
		IntoReader<<parquet::record::types::Group as parquet::record::Record>::Reader, Self>;

	fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		if let Some(field_names) = self.1.as_ref() {
			let mut struct_serializer = serializer.serialize_struct("Group", self.0.len())?;
			for (name, field) in field_names
				.iter()
				.map(|(name, _index)| name)
				.zip(self.0.iter())
			{
				struct_serializer.serialize_field(
					Box::leak(name.clone().into_boxed_str()),
					&SerdeSerialize(field),
				)?; // TODO!! static hashmap caching strings? aka string interning
			}
			struct_serializer.end()
		} else {
			let mut tuple_serializer = serializer.serialize_tuple_struct("Group", self.0.len())?;
			for field in self.0.iter() {
				tuple_serializer.serialize_field(&SerdeSerialize(field))?;
			}
			tuple_serializer.end()
		}
	}
	fn serde_deserialize<'de, D>(deserializer: D, schema: Option<Schema>) -> Result<Self, D::Error>
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
					keys.insert(key, values.len());
					values.push(value.0);
				}

				Ok(Group::new(values, Some(Arc::new(keys))))
			}
		}

		deserializer.deserialize_struct("Group", &[], GroupVisitor)
	}

	fn parquet_parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::ParquetSchema), ParquetError> {
		<parquet::record::types::Group as parquet::record::Record>::parse(schema, repetition)
	}
	fn parquet_reader(
		schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::ParquetReader {
		IntoReader::new(
			<parquet::record::types::Group as parquet::record::Record>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			),
		)
	}
}
// impl From<Group> for parquet::record::types::Group {
// 	fn from(group: Group) -> Self {
// 		let field_names = group.field_names();
// 		Self::new(group.into_fields().into_iter().map(Into::into).collect(), field_names)
// 	}
// }
impl From<parquet::record::types::Group> for Group {
	fn from(group: parquet::record::types::Group) -> Self {
		let field_names = group.field_names();
		Self::new(
			group.into_fields().into_iter().map(Into::into).collect(),
			Some(field_names),
		)
	}
}
impl<I> Index<I> for Group
where
	I: SliceIndex<[Value]>,
{
	type Output = <I as SliceIndex<[Value]>>::Output;

	fn index(&self, index: I) -> &Self::Output {
		self.0.index(index)
	}
}
impl PartialOrd for Group {
	fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
		if match (self.1.as_ref(), other.1.as_ref()) {
			(Some(a), Some(b)) => a == b,
			_ => self.0.len() == other.0.len(),
		} {
			self.0.partial_cmp(&other.0)
		} else {
			None
		}
	}
}

impl Debug for Group {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		if let Some(field_names) = self.1.as_ref() {
			let mut printer = f.debug_struct("Group");
			for (name, field) in field_names
				.iter()
				.map(|(name, _index)| name)
				.zip(self.0.iter())
			{
				printer.field(name, field);
			}
			printer.finish()
		} else {
			let mut printer = f.debug_tuple("Group");
			for field in self.0.iter() {
				printer.field(field);
			}
			printer.finish()
		}
	}
}
impl From<LinkedHashMap<String, Value, FxBuildHasher>> for Group {
	fn from(hashmap: LinkedHashMap<String, Value, FxBuildHasher>) -> Self {
		let mut keys = LinkedHashMap::with_capacity_and_hasher(hashmap.len(), Default::default());
		Self::new(
			hashmap
				.into_iter()
				.map(|(key, value)| {
					keys.insert(key, keys.len());
					value
				})
				.collect(),
			Some(Arc::new(keys)),
		)
	}
}
// impl From<Group> for LinkedHashMap<String, Value, FxBuildHasher> {
// 	fn from(group: Group) -> Self {
// 		group
// 			.1
// 			.iter()
// 			.map(|(name, _index)| name.clone())
// 			.zip(group.0)
// 			.collect()
// 	}
// }
