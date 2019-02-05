mod cloudfront;
mod common_crawl;
pub mod parquet;
pub use self::{cloudfront::*, common_crawl::*, parquet::Parquet};

pub use serde as _serde;

mod data {
	use parquet::{
		basic::Repetition, column::reader::ColumnReader, errors::ParquetError, record::{Reader, Record, Schema}, schema::types::{ColumnPath, Type}
	};
	use serde::{Deserialize, Deserializer, Serialize, Serializer};
	use std::{collections::HashMap, fmt::Debug};

	pub trait Data: Clone + PartialEq + Debug + PartialOrd + 'static {
		type ParquetSchema: Schema;
		type ParquetReader: Reader<Item = Self>;

		// fn for_each(impl FnMut(&str,

		fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
		where
			S: Serializer;
		fn serde_deserialize<'de, D>(deserializer: D) -> Result<Self, D::Error>
		where
			D: Deserializer<'de>;

		fn parquet_parse(
			schema: &Type, repetition: Option<Repetition>,
		) -> Result<(String, Self::ParquetSchema), ParquetError>;
		fn parquet_reader(
			schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
			paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
		) -> Self::ParquetReader;
	}

	macro_rules! impl_data_for_record {
		($($t:ty)*) => (
			$(impl Data for $t {
				type ParquetSchema = <$t as Record>::Schema;
				type ParquetReader = <$t as Record>::Reader;

				fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
				where
					S: Serializer {
					self.serialize(serializer)
				}
				fn serde_deserialize<'de,D>(deserializer: D) -> Result<Self, D::Error>
				where
					D: Deserializer<'de> {
					Self::deserialize(deserializer)
				}

				fn parquet_parse(
					schema: &Type, repetition: Option<Repetition>,
				) -> Result<(String, Self::ParquetSchema), ParquetError> {
					<$t as Record>::parse(schema, repetition)
				}
				fn parquet_reader(
					schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
					paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
				) -> Self::ParquetReader {
					<$t as Record>::reader(schema, path, def_level, rep_level, paths, batch_size)
				}
			})*
		);
	}
	impl_data_for_record!(bool u8 i8 u16 i16 u32 i32 u64 i64 f32 f64);

	impl<T> Data for Option<T>
	where
		T: Data,
	{
		type ParquetSchema = <Option<super::parquet::Record<T>> as Record>::Schema;
		type ParquetReader = XxxReader<<Option<super::parquet::Record<T>> as Record>::Reader>;

		fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
		where
			S: Serializer,
		{
			self.as_ref().map(SerdeSerialize).serialize(serializer)
		}
		fn serde_deserialize<'de, D>(deserializer: D) -> Result<Self, D::Error>
		where
			D: Deserializer<'de>,
		{
			<Option<SerdeDeserialize<T>>>::deserialize(deserializer).map(|x| x.map(|x| x.0))
		}

		fn parquet_parse(
			schema: &Type, repetition: Option<Repetition>,
		) -> Result<(String, Self::ParquetSchema), ParquetError> {
			<Option<super::parquet::Record<T>> as Record>::parse(schema, repetition)
		}
		fn parquet_reader(
			schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
			paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
		) -> Self::ParquetReader {
			XxxReader(<Option<super::parquet::Record<T>> as Record>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			))
		}
	}

	struct SerdeSerialize<'a, T: Data>(&'a T);
	impl<'a, T> Serialize for SerdeSerialize<'a, T>
	where
		T: Data,
	{
		fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
		where
			S: Serializer,
		{
			self.0.serde_serialize(serializer)
		}
	}

	struct SerdeDeserialize<T: Data>(T);
	impl<'de, T> Deserialize<'de> for SerdeDeserialize<T>
	where
		T: Data,
	{
		fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
		where
			D: Deserializer<'de>,
		{
			T::serde_deserialize(deserializer).map(SerdeDeserialize)
		}
	}

	/// A Reader that wraps a Reader, wrapping the read value in a `Record`.
	pub struct XxxReader<T>(T);
	impl<T, U> parquet::record::Reader for XxxReader<T>
	where
		T: parquet::record::Reader<Item = Option<super::parquet::Record<U>>>,
		U: Data,
	{
		type Item = Option<U>;

		#[inline]
		fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item, ParquetError> {
			self.0.read(def_level, rep_level).map(|x| x.map(|x| x.0))
		}

		#[inline]
		fn advance_columns(&mut self) -> Result<(), ParquetError> {
			self.0.advance_columns()
		}

		#[inline]
		fn has_next(&self) -> bool {
			self.0.has_next()
		}

		#[inline]
		fn current_def_level(&self) -> i16 {
			self.0.current_def_level()
		}

		#[inline]
		fn current_rep_level(&self) -> i16 {
			self.0.current_rep_level()
		}
	}
}
pub use amadeus_derive::Data;
pub use data::Data;

pub mod serde_data {
	use super::Data;
	use serde::{Deserialize, Deserializer, Serialize, Serializer};

	pub fn serialize<T, S>(self_: &T, serializer: S) -> Result<S::Ok, S::Error>
	where
		T: Data + ?Sized,
		S: Serializer,
	{
		self_.serde_serialize(serializer)
	}
	pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
	where
		T: Data,
		D: Deserializer<'de>,
	{
		T::serde_deserialize(deserializer)
	}
}

// impl<T: ?Sized> Data for T where T: PartialEq + Eq + Clone + 'static {}

// pub trait DataSource: crate::dist_iter::DistributedIterator<Item = <Self as DataSource>::Itemm> {
// 	type Itemm: Data;
// }

// impl<T: ?Sized> DataSource for T where T: crate::dist_iter::DistributedIterator, <T as crate::dist_iter::DistributedIterator>::Item: Data {
// 	type Itemm = <T as crate::dist_iter::DistributedIterator>::Item;
// }

pub trait DataSource
where
	Self: crate::dist_iter::DistributedIterator<Item = <Self as DataSource>::Item>,
	<Self as crate::dist_iter::DistributedIterator>::Item: Data,
{
	type Item;
}

// impl<T: ?Sized> DataSource for T where T: crate::dist_iter::DistributedIterator, <T as crate::dist_iter::DistributedIterator>::Item: Data {
// 	// type Itemm = <T as crate::dist_iter::DistributedIterator>::Item;
// 	type Item = <T as crate::dist_iter::DistributedIterator>::Item;
// }

struct ResultExpand<T, E>(Result<T, E>);
impl<T, E> IntoIterator for ResultExpand<T, E>
where
	T: IntoIterator,
{
	type Item = Result<T::Item, E>;
	type IntoIter = ResultExpandIter<T::IntoIter, E>;
	fn into_iter(self) -> Self::IntoIter {
		ResultExpandIter(self.0.map(IntoIterator::into_iter).map_err(Some))
	}
}
struct ResultExpandIter<T, E>(Result<T, Option<E>>);
impl<T, E> Iterator for ResultExpandIter<T, E>
where
	T: Iterator,
{
	type Item = Result<T::Item, E>;
	fn next(&mut self) -> Option<Self::Item> {
		transpose(self.0.as_mut().map(Iterator::next).map_err(Option::take))
	}
}
fn transpose<T, E>(result: Result<Option<T>, Option<E>>) -> Option<Result<T, E>> {
	match result {
		Ok(Some(x)) => Some(Ok(x)),
		Err(Some(e)) => Some(Err(e)),
		Ok(None) | Err(None) => None,
	}
}
