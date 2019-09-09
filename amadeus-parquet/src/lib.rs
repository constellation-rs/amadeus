#![feature(specialization)]
#![feature(type_alias_impl_trait)]
#![feature(read_initializer)]
#![feature(bufreader_seek_relative)]

mod impls;

pub mod internal;

use internal::{
	basic::Repetition, column::reader::ColumnReader, errors::ParquetError as InternalParquetError, file::reader::{FileReader, SerializedFileReader}, record::{Reader as ParquetReader, RowIter, Schema as ParquetSchema}, schema::types::{ColumnPath, Type}
};
use serde::{Deserialize, Serialize};
use serde_closure::*;
use std::{
	collections::HashMap, error, fmt::{self, Debug, Display}, io, iter, marker::PhantomData, ops::FnMut, vec
};

use amadeus_core::{
	dist_iter::DistributedIterator, file::{Directory, File, Page, Partition, PathBuf}, into_dist_iter::IntoDistributedIterator, util::ResultExpand, Source
};

type Closure<Env, Args, Output> =
	serde_closure::FnMut<Env, for<'r> fn(&'r mut Env, Args) -> Output>;

pub trait ParquetData
where
	Self: Clone + PartialEq + Debug + 'static,
{
	type Schema: ParquetSchema;
	type Reader: ParquetReader<Item = Self>;

	fn parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema), InternalParquetError>;
	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader;
}

pub type ParquetInner<F, Row> = amadeus_core::dist_iter::FlatMap<
	amadeus_core::into_dist_iter::IterIter<vec::IntoIter<<F as File>::Partition>>,
	Closure<
		(),
		(<F as File>::Partition,),
		iter::Map<
			iter::FlatMap<
				amadeus_core::util::ResultExpandIter<
					vec::IntoIter<<<F as File>::Partition as Partition>::Page>,
					ParquetError<F>,
				>,
				ResultExpand<
					iter::Map<
						RowIter<
							SerializedFileReader<
								ParquetReaderWrap<<<F as File>::Partition as Partition>::Page>,
							>,
							Record<Row>,
						>,
						Closure<
							(),
							(Result<Record<Row>, InternalParquetError>,),
							Result<Row, InternalParquetError>,
						>,
					>,
					ParquetError<F>,
				>,
				Closure<
					(),
					(Result<<<F as File>::Partition as Partition>::Page, ParquetError<F>>,),
					ResultExpand<
						iter::Map<
							RowIter<
								SerializedFileReader<
									ParquetReaderWrap<<<F as File>::Partition as Partition>::Page>,
								>,
								Record<Row>,
							>,
							Closure<
								(),
								(Result<Record<Row>, InternalParquetError>,),
								Result<Row, InternalParquetError>,
							>,
						>,
						ParquetError<F>,
					>,
				>,
			>,
			Closure<
				(),
				(Result<Result<Row, InternalParquetError>, ParquetError<F>>,),
				Result<Row, ParquetError<F>>,
			>,
		>,
	>,
>;

mod wrap {
	use super::{internal, ParquetData};
	use internal::{
		basic::Repetition, column::reader::ColumnReader, errors::Result, schema::types::{ColumnPath, Type}
	};
	use std::collections::HashMap;

	#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
	#[repr(transparent)]
	pub struct Record<T>(pub T)
	where
		T: ParquetData;
	impl<T> internal::record::Record for Record<T>
	where
		T: ParquetData,
	{
		type Reader = RecordReader<T::Reader>;
		type Schema = T::Schema;

		#[inline]
		fn parse(schema: &Type, repetition: Option<Repetition>) -> Result<(String, Self::Schema)> {
			T::parse(schema, repetition)
		}

		#[inline]
		fn reader(
			schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
			paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
		) -> Self::Reader {
			RecordReader(T::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			))
		}
	}

	/// A Reader that wraps a Reader, wrapping the read value in a `Record`.
	pub struct RecordReader<T>(T);
	impl<T> internal::record::Reader for RecordReader<T>
	where
		T: internal::record::Reader,
		T::Item: ParquetData,
	{
		type Item = Record<T::Item>;

		#[inline]
		fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item> {
			self.0.read(def_level, rep_level).map(Record)
		}

		#[inline]
		fn advance_columns(&mut self) -> Result<()> {
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
pub use wrap::Record;

pub struct Parquet<File, Row>
where
	File: amadeus_core::file::File,
	Row: ParquetData,
{
	partitions: Vec<File::Partition>,
	marker: PhantomData<fn() -> Row>,
}
impl<F, Row> Parquet<F, Row>
where
	F: File,
	Row: ParquetData,
{
	pub fn new(file: F) -> Result<Self, ParquetError<F>> {
		Ok(Self {
			partitions: file.partitions().map_err(ParquetError::<F>::File)?,
			marker: PhantomData,
		})
	}
}
impl<F, Row> Source for Parquet<F, Row>
where
	F: File,
	Row: ParquetData,
{
	type Item = Row;
	type Error = ParquetError<F>;

	// type DistIter = impl DistributedIterator<Item = Result<Row, Self::Error>>; //, <Self as super::super::DistributedIterator>::Task: Serialize + for<'de> Deserialize<'de>
	type DistIter = ParquetInner<F, Row>;
	type Iter = iter::Empty<Result<Row, Self::Error>>;

	fn dist_iter(self) -> Self::DistIter {
		self.partitions
			.into_dist_iter()
			.flat_map(FnMut!(|partition: F::Partition| {
				ResultExpand(partition.pages().map_err(ParquetError::<F>::Partition))
					.into_iter()
					.flat_map(FnMut!(|page: Result<_, _>| {
						ResultExpand(page.and_then(
							|page: <<F as File>::Partition as Partition>::Page| {
								Ok(SerializedFileReader::new(ParquetReaderWrap(Page::reader(
									page,
								)))?
								.get_row_iter(None)?
								.map(FnMut!(|x: Result<
										Record<Row>,
										InternalParquetError,
									>|
									 -> Result<Row, InternalParquetError> {
										Ok(x?.0)
									})))
							},
						))
					}))
					.map(FnMut!(|row: Result<Result<Row, _>, _>| Ok(row??)))
			}))
	}
	fn iter(self) -> Self::Iter {
		iter::empty()
	}
}

pub struct ParquetReaderWrap<P>(amadeus_core::file::Reader<P>)
where
	P: Page;
impl<P> io::Read for ParquetReaderWrap<P>
where
	P: Page,
{
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		self.0.read(buf)
	}
	unsafe fn initializer(&self) -> io::Initializer {
		io::Initializer::nop()
	}
}
impl<P> io::Seek for ParquetReaderWrap<P>
where
	P: Page,
{
	fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
		self.0.seek(pos)
	}
}
impl<P> internal::file::reader::ParquetReader for ParquetReaderWrap<P>
where
	P: Page,
{
	fn len(&self) -> u64 {
		self.0.len()
	}
}

#[derive(Serialize, Deserialize)]
pub struct ParquetDirectory<D> {
	directory: D,
}
impl<D> ParquetDirectory<D> {
	pub fn new(directory: D) -> Self {
		Self { directory }
	}
}
impl<D> File for ParquetDirectory<D>
where
	D: Directory,
	D::Partition: Debug,
{
	type Partition = D::Partition;
	type Error = D::Error;

	fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		self.partitions_filter(|_| true)
	}
}
impl<D> Directory for ParquetDirectory<D>
where
	D: Directory,
	D::Partition: Debug,
{
	fn partitions_filter<F>(self, mut f: F) -> Result<Vec<Self::Partition>, Self::Error>
	where
		F: FnMut(&PathBuf) -> bool,
	{
		// "Logic" interpreted from https://github.com/apache/arrow/blob/927cfeff875e557e28649891ea20ca38cb9d1536/python/pyarrow/parquet.py#L705-L829
		// and https://github.com/apache/spark/blob/5a7403623d0525c23ab8ae575e9d1383e3e10635/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/InMemoryFileIndex.scala#L348-L359
		// and https://github.com/apache/spark/blob/5a7403623d0525c23ab8ae575e9d1383e3e10635/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetPartitionDiscoverySuite.scala
		self.directory.partitions_filter(|path| {
			let skip;
			if !path.is_file() {
				let dir_name = path.last().unwrap();
				skip = dir_name.starts_with('.') // Hidden files
						|| (dir_name.starts_with('_') && !dir_name.contains('=')) // ARROW-1079: Filter out "private" directories starting with underscore;
			} else {
				let file_name = path.file_name().unwrap();
				let extension = file_name.rfind('.').map(|offset| &file_name[offset + 1..]);
				skip = file_name.starts_with('.') // Hidden files
							|| file_name == "_metadata" || file_name == "_common_metadata" // Summary metadata
							|| file_name == "_SUCCESS" // Spark success marker
							|| extension == Some("_COPYING_") // File copy in progress; TODO: Should we error on this?
							|| extension == Some("crc") // Checksums
							|| file_name.ends_with("_$folder$"); // This is created by Apache tools on S3
			}
			!skip && f(path)
		})
	}
}

mod misc_serde {
	use super::internal;
	use internal::errors::ParquetError;
	use serde::{Deserialize, Deserializer, Serialize, Serializer};

	pub struct Serde<T>(T);

	impl Serialize for Serde<&ParquetError> {
		fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
		where
			S: Serializer,
		{
			<(usize, &str)>::serialize(
				&match self.0 {
					ParquetError::General(message) => (0, message),
					ParquetError::EOF(message) => (1, message),
					_ => unimplemented!(),
				},
				serializer,
			)
		}
	}
	impl<'de> Deserialize<'de> for Serde<ParquetError> {
		fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
		where
			D: Deserializer<'de>,
		{
			<(usize, String)>::deserialize(deserializer)
				.map(|(kind, message)| match kind {
					1 => ParquetError::EOF(message),
					_ => ParquetError::General(message),
				})
				.map(Self)
		}
	}

	pub fn serialize<T, S>(t: &T, serializer: S) -> Result<S::Ok, S::Error>
	where
		for<'a> Serde<&'a T>: Serialize,
		S: Serializer,
	{
		Serde(t).serialize(serializer)
	}
	pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
	where
		Serde<T>: Deserialize<'de>,
		D: Deserializer<'de>,
	{
		Serde::<T>::deserialize(deserializer).map(|x| x.0)
	}
}

pub type ParquetError<F> = ParquetErrorInternal<
	<F as File>::Error,
	<<F as File>::Partition as Partition>::Error,
	<<<F as File>::Partition as Partition>::Page as Page>::Error,
>;

#[derive(Serialize, Deserialize, Debug)]
pub enum ParquetErrorInternal<A, B, C> {
	File(A),
	Partition(B),
	Page(C),
	Parquet(#[serde(with = "misc_serde")] InternalParquetError),
}
impl<A, B, C> PartialEq for ParquetErrorInternal<A, B, C>
where
	A: PartialEq,
	B: PartialEq,
	C: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Self::File(a), Self::File(b)) => a == b,
			(Self::Partition(a), Self::Partition(b)) => a == b,
			(Self::Page(a), Self::Page(b)) => a == b,
			(Self::Parquet(a), Self::Parquet(b)) => a == b,
			_ => false,
		}
	}
}
impl<A, B, C> error::Error for ParquetErrorInternal<A, B, C>
where
	A: error::Error,
	B: error::Error,
	C: error::Error,
{
}
impl<A, B, C> Display for ParquetErrorInternal<A, B, C>
where
	A: Display,
	B: Display,
	C: Display,
{
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::File(err) => Display::fmt(err, f),
			Self::Partition(err) => Display::fmt(err, f),
			Self::Page(err) => Display::fmt(err, f),
			Self::Parquet(err) => Display::fmt(err, f),
		}
	}
}
impl<A, B, C> From<InternalParquetError> for ParquetErrorInternal<A, B, C> {
	fn from(err: InternalParquetError) -> Self {
		Self::Parquet(err)
	}
}
