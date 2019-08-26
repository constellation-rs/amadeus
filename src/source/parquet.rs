use super::ResultExpand;
use crate::{
	data::Data, dist_iter::Consumer, into_dist_iter::IntoDistributedIterator, DistributedIterator
};
use amadeus_parquet::{
	errors::ParquetError, file::reader::{FileReader, SerializedFileReader}, record::RowIter
};
use std::{
	error, fmt::{self, Display}, fs::File, io, iter, marker::PhantomData, path::PathBuf, sync::Arc, vec
};
use walkdir::WalkDir;

pub use amadeus_parquet as _internal;

type Closure<Env, Args, Output> =
	serde_closure::FnMut<Env, for<'r> fn(&'r mut Env, Args) -> Output>;

// trait Abc where Self: DistributedIterator, <Self as DistributedIterator>::Task: serde::Serialize {}
// impl<T:?Sized> Abc for T where T: DistributedIterator, <T as DistributedIterator>::Task: serde::Serialize {}

// existential type ParquetInner: Abc;

use serde::Serialize;

type ParquetInner<Row> = crate::dist_iter::FlatMap<
	crate::into_dist_iter::IterIter<vec::IntoIter<PathBuf>>,
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
	use crate::data::Data;
	use amadeus_parquet::{
		basic::Repetition, column::reader::ColumnReader, errors::Result, schema::types::{ColumnPath, Type}
	};
	use std::collections::HashMap;

	#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
	#[repr(transparent)]
	pub struct Record<T>(pub T)
	where
		T: Data;
	impl<T> amadeus_parquet::record::Record for Record<T>
	where
		T: Data,
	{
		type Reader = RecordReader<T::ParquetReader>;
		type Schema = T::ParquetSchema;

		#[inline]
		fn parse(schema: &Type, repetition: Option<Repetition>) -> Result<(String, Self::Schema)> {
			T::parquet_parse(schema, repetition)
		}

		#[inline]
		fn reader(
			schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
			paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
		) -> Self::Reader {
			RecordReader(T::parquet_reader(
				schema, path, def_level, rep_level, paths, batch_size,
			))
		}
	}

	/// A Reader that wraps a Reader, wrapping the read value in a `Record`.
	pub struct RecordReader<T>(T);
	impl<T> amadeus_parquet::record::Reader for RecordReader<T>
	where
		T: amadeus_parquet::record::Reader,
		T::Item: Data,
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
	Row: Data,
{
	i: ParquetInner<Row>,
}
impl<Row> Parquet<Row>
where
	Row: Data,
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
			Ok(e) if e.file_type().is_dir() => {
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
	use amadeus_parquet::errors::ParquetError;
	use serde::{Deserialize, Deserializer, Serialize, Serializer};
	use std::{io, sync::Arc};

	pub struct Serde<T>(T);

	impl Serialize for Serde<&io::ErrorKind> {
		fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
		where
			S: Serializer,
		{
			usize::serialize(
				&match self.0 {
					io::ErrorKind::NotFound => 0,
					io::ErrorKind::PermissionDenied => 1,
					io::ErrorKind::ConnectionRefused => 2,
					io::ErrorKind::ConnectionReset => 3,
					io::ErrorKind::ConnectionAborted => 4,
					io::ErrorKind::NotConnected => 5,
					io::ErrorKind::AddrInUse => 6,
					io::ErrorKind::AddrNotAvailable => 7,
					io::ErrorKind::BrokenPipe => 8,
					io::ErrorKind::AlreadyExists => 9,
					io::ErrorKind::WouldBlock => 10,
					io::ErrorKind::InvalidInput => 11,
					io::ErrorKind::InvalidData => 12,
					io::ErrorKind::TimedOut => 13,
					io::ErrorKind::WriteZero => 14,
					io::ErrorKind::Interrupted => 15,
					io::ErrorKind::UnexpectedEof => 17,
					_ => 16,
				},
				serializer,
			)
		}
	}
	impl<'de> Deserialize<'de> for Serde<io::ErrorKind> {
		fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
		where
			D: Deserializer<'de>,
		{
			usize::deserialize(deserializer)
				.map(|kind| match kind {
					0 => io::ErrorKind::NotFound,
					1 => io::ErrorKind::PermissionDenied,
					2 => io::ErrorKind::ConnectionRefused,
					3 => io::ErrorKind::ConnectionReset,
					4 => io::ErrorKind::ConnectionAborted,
					5 => io::ErrorKind::NotConnected,
					6 => io::ErrorKind::AddrInUse,
					7 => io::ErrorKind::AddrNotAvailable,
					8 => io::ErrorKind::BrokenPipe,
					9 => io::ErrorKind::AlreadyExists,
					10 => io::ErrorKind::WouldBlock,
					11 => io::ErrorKind::InvalidInput,
					12 => io::ErrorKind::InvalidData,
					13 => io::ErrorKind::TimedOut,
					14 => io::ErrorKind::WriteZero,
					15 => io::ErrorKind::Interrupted,
					17 => io::ErrorKind::UnexpectedEof,
					_ => io::ErrorKind::Other,
				})
				.map(Self)
		}
	}

	impl Serialize for Serde<&Arc<io::Error>> {
		fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
		where
			S: Serializer,
		{
			<(Serde<&io::ErrorKind>, String)>::serialize(
				&(Serde(&self.0.kind()), self.0.to_string()),
				serializer,
			)
		}
	}
	impl<'de> Deserialize<'de> for Serde<Arc<io::Error>> {
		fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
		where
			D: Deserializer<'de>,
		{
			<(Serde<io::ErrorKind>, String)>::deserialize(deserializer)
				.map(|(kind, message)| Arc::new(io::Error::new(kind.0, message)))
				.map(Self)
		}
	}

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
	Io(#[serde(with = "misc_serde")] Arc<io::Error>),
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
			Self::Io(err) => err.fmt(f),
			Self::Parquet(err) => err.fmt(f),
		}
	}
}
impl From<io::Error> for Error {
	fn from(err: io::Error) -> Self {
		Self::Io(Arc::new(err))
	}
}
impl From<ParquetError> for Error {
	fn from(err: ParquetError) -> Self {
		Self::Parquet(err)
	}
}

impl<Row> DistributedIterator for Parquet<Row>
where
	Row: Data,
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
	Row: Data,
{
	task: <ParquetInner<Row> as DistributedIterator>::Task,
	marker: PhantomData<fn() -> Row>,
}

impl<Row> Consumer for ParquetConsumer<Row>
where
	Row: Data,
{
	type Item = Result<Row, Error>;

	fn run(self, i: &mut impl FnMut(Self::Item) -> bool) -> bool {
		self.task.run(i)
	}
}
