//! Implement [`Record`] for [`List`].

use serde::{Deserialize, Serialize};
use std::{
	fmt::{self, Debug}, ops::Index, slice::{self, SliceIndex}, vec
};

// use parchet::{
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
pub struct List<T>(pub(crate) Vec<T>);

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
// impl From<List> for parchet::record::types::List {
//     fn from(list: List) -> Self {
//         unimplemented!()
//     }
// }

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
