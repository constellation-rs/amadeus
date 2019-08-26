//! Implement [`Record`] for [`List`].

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
	collections::HashMap, fmt::{self, Debug}, mem::transmute, ops::Index, slice::{self, SliceIndex}, vec
};

use super::{super::Data, MapReader, SchemaIncomplete};
use amadeus_parquet::{
	basic::Repetition, column::reader::ColumnReader, errors::ParquetError, schema::types::{ColumnPath, Type}
};
// use amadeus_parquet::{
//     basic::{LogicalType, Repetition},
//     column::reader::ColumnReader,
//     errors::{ParquetError, Result},
//     record::{
//         reader::{MapReader, RepeatedReader},
//         schemas::{ListSchema, ListSchemaType},
//         Reader, Record,
//     },
//     schema::types::{ColumnPath, Type},
// };

/// [`List<T>`](List) corresponds to the [List logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists).
#[derive(Clone, Hash, Eq, PartialOrd, Serialize, Deserialize)]
pub struct List<T>(pub(in super::super) Vec<T>);

impl<T> List<T> {
	/// Returns an iterator over references to the elements of the List.
	pub fn iter(&self) -> slice::Iter<'_, T> {
		self.0.iter()
	}
}
impl<T> IntoIterator for List<T> {
	type Item = T;
	type IntoIter = vec::IntoIter<T>;

	/// Creates an iterator over the elements of the List.
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}
impl<T> Data for List<T>
where
	T: Data,
{
	type ParquetSchema = <amadeus_parquet::record::types::List<crate::source::parquet::Record<T>> as amadeus_parquet::record::Record>::Schema;
	type ParquetReader = impl amadeus_parquet::record::Reader<Item = Self>;
	// type ParquetReader =
	//     IntoReader<<amadeus_parquet::record::types::List<crate::source::parquet::Record<T>> as amadeus_parquet::record::Record>::Reader, Self>;

	fn postgres_query(
		_f: &mut fmt::Formatter, _name: Option<&crate::source::postgres::Names<'_>>,
	) -> fmt::Result {
		unimplemented!()
	}
	fn postgres_decode(
		_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		unimplemented!()
	}

	fn serde_serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		// self.serialize(serializer)
		unimplemented!()
	}
	fn serde_deserialize<'de, D>(
		_deserializer: D, _schema: Option<SchemaIncomplete>,
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
		<amadeus_parquet::record::types::List<crate::source::parquet::Record<T>> as amadeus_parquet::record::Record>::parse(schema, repetition)
	}
	fn parquet_reader(
		schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::ParquetReader {
		MapReader::new(
            <amadeus_parquet::record::types::List<crate::source::parquet::Record<T>> as amadeus_parquet::record::Record>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            ),
            |list| Ok(unsafe{transmute::<amadeus_parquet::record::types::List<crate::source::parquet::Record<T>>,amadeus_parquet::record::types::List<T>>(list).into()})
        )
	}
}
// impl From<List> for amadeus_parquet::record::types::List {
//     fn from(list: List) -> Self {
//         unimplemented!()
//     }
// }
impl<T, U> From<amadeus_parquet::record::types::List<U>> for List<T>
where
	T: Data,
	U: Into<T>,
{
	fn from(list: amadeus_parquet::record::types::List<U>) -> Self {
		<_ as Into<Vec<U>>>::into(list)
			.into_iter()
			.map(Into::into)
			.collect::<Vec<_>>()
			.into()
	}
}

impl<T> From<Vec<T>> for List<T> {
	fn from(vec: Vec<T>) -> Self {
		Self(vec)
	}
}
impl<T> Into<Vec<T>> for List<T> {
	fn into(self) -> Vec<T> {
		self.0
	}
}
impl<T, U> PartialEq<List<U>> for List<T>
where
	T: PartialEq<U>,
{
	fn eq(&self, other: &List<U>) -> bool {
		self.0 == other.0
	}
}
impl<T, I> Index<I> for List<T>
where
	I: SliceIndex<[T]>,
{
	type Output = <I as SliceIndex<[T]>>::Output;

	fn index(&self, index: I) -> &Self::Output {
		self.0.index(index)
	}
}
impl<T> Debug for List<T>
where
	T: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_list().entries(self.iter()).finish()
	}
}
