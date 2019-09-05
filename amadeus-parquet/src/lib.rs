#![feature(specialization)]
#![feature(type_alias_impl_trait)]

mod impls;

pub use parchet as _internal;
use parchet::{
	basic::Repetition, column::reader::ColumnReader, errors::ParquetError, file::reader::{FileReader, SerializedFileReader}, record::{Reader as ParquetReader, RowIter, Schema as ParquetSchema}, schema::types::{ColumnPath, Type}
};
use serde::{Deserialize, Serialize};
use serde_closure::*;
use std::{
	collections::HashMap, error, fmt::{self, Debug, Display}, fs::File, io, iter, marker::PhantomData, ops::FnMut, path::PathBuf, vec
};
use walkdir::WalkDir;

use amadeus_core::{
	dist_iter::{Consumer, DistributedIterator}, into_dist_iter::IntoDistributedIterator, util::{IoError, ResultExpand}
};

type Closure<Env, Args, Output> =
	serde_closure::FnMut<Env, for<'r> fn(&'r mut Env, Args) -> Output>;

// trait Abc where Self: DistributedIterator, <Self as DistributedIterator>::Task: serde::Serialize {}
// impl<T:?Sized> Abc for T where T: DistributedIterator, <T as DistributedIterator>::Task: serde::Serialize {}

// existential type ParquetInner: Abc;

pub trait ParquetData
where
	Self: Clone + PartialEq + Debug + 'static,
{
	type Schema: ParquetSchema;
	type Reader: ParquetReader<Item = Self>;

	fn parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema), ParquetError>;
	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader;
}

type ParquetInner<Row> = amadeus_core::dist_iter::FlatMap<
	amadeus_core::into_dist_iter::IterIter<vec::IntoIter<PathBuf>>,
	Closure<
		(),
		(PathBuf,),
		iter::Map<
			iter::FlatMap<
				sum::Sum2<
					iter::Once<Result<PathBuf, io::Error>>,
					vec::IntoIter<Result<PathBuf, io::Error>>,
				>,
				ResultExpand<
					iter::Map<
						RowIter<SerializedFileReader<File>, Record<Row>>,
						Closure<
							(),
							(Result<Record<Row>, ParquetError>,),
							Result<Row, ParquetError>,
						>,
					>,
					io::Error,
				>,
				Closure<
					(),
					(Result<PathBuf, io::Error>,),
					ResultExpand<
						iter::Map<
							RowIter<SerializedFileReader<File>, Record<Row>>,
							Closure<
								(),
								(Result<Record<Row>, ParquetError>,),
								Result<Row, ParquetError>,
							>,
						>,
						io::Error,
					>,
				>,
			>,
			Closure<(), (Result<Result<Row, ParquetError>, io::Error>,), Result<Row, Error>>,
		>,
	>,
>;

mod wrap {
	use super::ParquetData;
	use parchet::{
		basic::Repetition, column::reader::ColumnReader, errors::Result, schema::types::{ColumnPath, Type}
	};
	use std::collections::HashMap;

	#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
	#[repr(transparent)]
	pub struct Record<T>(pub T)
	where
		T: ParquetData;
	impl<T> parchet::record::Record for Record<T>
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
	impl<T> parchet::record::Reader for RecordReader<T>
	where
		T: parchet::record::Reader,
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

pub struct Parquet<Row>
where
	Row: ParquetData,
{
	i: ParquetInner<Row>,
}
impl<Row> Parquet<Row>
where
	Row: ParquetData,
{
	pub fn new<I>(files: I) -> Result<Self, ()>
	where
		I: IntoIterator<Item = PathBuf>,
	{
		let i = files
			.into_iter()
			.collect::<Vec<_>>()
			.into_dist_iter()
			.flat_map(FnMut!(|file: PathBuf| {
				let files = if !file.is_dir() {
					sum::Sum2::A(iter::once(Ok(file)))
				} else {
					sum::Sum2::B(get_parquet_partitions(file))
				};
				files
					.flat_map(FnMut!(|file: Result<PathBuf, _>| ResultExpand(
						file.and_then(|file| Ok(File::open(file)?))
							.and_then(|file| Ok(SerializedFileReader::new(file)?
								.get_row_iter(None)?
								.map(FnMut!(|x: Result<Record<Row>, ParquetError>| Ok(x?.0)))))
					)))
					.map(FnMut!(|row: Result<Result<Row, _>, _>| Ok(row??)))
			}));
		Ok(Self { i })
	}
}

/// "Logic" interpreted from https://github.com/apache/arrow/blob/927cfeff875e557e28649891ea20ca38cb9d1536/python/pyarrow/parquet.py#L705-L829
/// and https://github.com/apache/spark/blob/5a7403623d0525c23ab8ae575e9d1383e3e10635/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/InMemoryFileIndex.scala#L348-L359
/// and https://github.com/apache/spark/blob/5a7403623d0525c23ab8ae575e9d1383e3e10635/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetPartitionDiscoverySuite.scala
fn get_parquet_partitions(dir: PathBuf) -> vec::IntoIter<Result<PathBuf, io::Error>> {
	WalkDir::new(dir)
		.follow_links(true)
		.sort_by(|a, b| a.file_name().cmp(b.file_name()))
		.into_iter()
		.filter_entry(|e| {
			let is_dir = e.file_type().is_dir();
			let path = e.path();
			let extension = path.extension();
			let file_name = path.file_name().unwrap().to_string_lossy();
			let skip = file_name.starts_with('.')
				|| if is_dir {
					file_name.starts_with('_') && !file_name.contains('=') // ARROW-1079: Filter out "private" directories starting with underscore
				} else {
					file_name == "_metadata" // Summary metadata
						|| file_name == "_common_metadata" // Summary metadata
						// || (extension.is_some() && extension.unwrap() == "_COPYING_") // File copy in progress; TODO: Emit error on this.
						|| (extension.is_some() && extension.unwrap() == "crc") // Checksums
						|| file_name == "_SUCCESS" // Spark success marker
				};
			!skip
		})
		.filter_map(|e| match e {
			Ok(ref e) if e.file_type().is_dir() => {
				let path = e.path();
				let directory_name = path.file_name().unwrap();
				let valid = directory_name.to_string_lossy().contains('=');
				if !valid {
					Some(Err(io::Error::new(
						io::ErrorKind::Other,
						format!(
							"Invalid directory name \"{}\"",
							directory_name.to_string_lossy()
						),
					)))
				} else {
					None
				}
			}
			Ok(e) => Some(Ok(e.into_path())),
			Err(e) => Some(Err(e.into())),
		})
		.collect::<Vec<_>>()
		.into_iter()
}

mod misc_serde {
	use parchet::errors::ParquetError;
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

#[derive(Serialize, Deserialize, Debug)]
pub enum Error {
	Io(IoError),
	Parquet(#[serde(with = "misc_serde")] ParquetError),
}
impl PartialEq for Error {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Self::Io(a), Self::Io(b)) => a.to_string() == b.to_string(),
			(Self::Parquet(a), Self::Parquet(b)) => a == b,
			_ => false,
		}
	}
}
impl error::Error for Error {}
impl Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Io(err) => Display::fmt(err, f),
			Self::Parquet(err) => Display::fmt(err, f),
		}
	}
}
impl From<io::Error> for Error {
	fn from(err: io::Error) -> Self {
		Self::Io(err.into())
	}
}
impl From<ParquetError> for Error {
	fn from(err: ParquetError) -> Self {
		Self::Parquet(err)
	}
}

impl<Row> DistributedIterator for Parquet<Row>
where
	Row: ParquetData,
{
	type Item = Result<Row, Error>;
	type Task = ParquetConsumer<Row>;

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.i.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.i.next_task().map(|task| ParquetConsumer::<Row> {
			task,
			marker: PhantomData,
		})
	}
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ParquetConsumer<Row>
where
	Row: ParquetData,
{
	task: <ParquetInner<Row> as DistributedIterator>::Task,
	marker: PhantomData<fn() -> Row>,
}

impl<Row> Consumer for ParquetConsumer<Row>
where
	Row: ParquetData,
{
	type Item = Result<Row, Error>;

	fn run(self, i: &mut impl FnMut(Self::Item) -> bool) -> bool {
		self.task.run(i)
	}
}
