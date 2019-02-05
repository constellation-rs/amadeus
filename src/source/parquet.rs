use super::{Data, ResultExpand};
use crate::{
	dist_iter::Consumer, into_dist_iter::IntoDistributedIterator, DistributedIterator, IteratorExt
};
use parquet::{
	errors::ParquetError, file::reader::{FileReader, SerializedFileReader}, record::RowIter
};
use std::{
	borrow::Cow, convert::identity, error, fmt::{self, Display}, fs::File, io::{self, BufRead, BufReader}, iter, marker::PhantomData, path::PathBuf, sync::Arc, time, vec
};
use walkdir::WalkDir;

pub use parquet as _internal;

type Closure<Env, Args, Output> =
	serde_closure::FnMut<Env, for<'r> fn(&'r mut Env, Args) -> Output>;

// trait Abc where Self: DistributedIterator, <Self as DistributedIterator>::Task: serde::Serialize {}
// impl<T:?Sized> Abc for T where T: DistributedIterator, <T as DistributedIterator>::Task: serde::Serialize {}

// existential type ParquetInner: Abc;

use serde::{de::DeserializeOwned, Serialize};

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
	use super::super::Data;
	use parquet::{
		basic::Repetition, column::reader::ColumnReader, errors::Result, schema::types::{ColumnPath, Type}
	};
	use std::collections::HashMap;

	pub struct Record<T>(pub T)
	where
		T: Data;
	impl<T> parquet::record::Record for Record<T>
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
	impl<T> parquet::record::Reader for RecordReader<T>
	where
		T: parquet::record::Reader,
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
		I: iter::IntoIterator<Item = PathBuf>,
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
		Ok(Parquet { i })
	}
}

/// "Logic" interpreted from https://github.com/apache/arrow/blob/927cfeff875e557e28649891ea20ca38cb9d1536/python/pyarrow/parquet.py#L705-L829
fn get_parquet_partitions(dir: PathBuf) -> vec::IntoIter<Result<PathBuf, io::Error>> {
	WalkDir::new(dir)
		.follow_links(true)
		.sort_by(|a, b| a.file_name().cmp(b.file_name()))
		.into_iter()
		.filter_entry(|e| {
			let is_dir = e.file_type().is_dir();
			let path = e.path();
			let extension = path.extension();
			let file_name = path
				.file_name()
				.map(|file_name| file_name.to_string_lossy())
				.unwrap_or(Cow::from(""));
			let skip = file_name.starts_with('.')
				|| match is_dir {
					true => {
						file_name.starts_with('_') && !file_name.contains('=') // ARROW-1079: Filter out "private" directories starting with underscore
					}
					false => {
						file_name.ends_with("_metadata") // Summary metadata
						|| (extension.is_some() && extension.unwrap() == "crc") // Checksums
						|| file_name == "_SUCCESS" // Spark success marker
					}
				};
			skip
		})
		.filter_map(|e| match e {
			Ok(e) if e.file_type().is_dir() => {
				let path = e.path();
				let file_name = path
					.file_name()
					.map(|file_name| file_name.to_string_lossy())
					.unwrap_or(Cow::from(""));
				if !file_name.contains('=') {
					Some(Err(io::Error::new(
						io::ErrorKind::Other,
						format!("Invalid directory name \"{}\"", file_name),
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

#[derive(Debug)]
pub enum Error {
	Io(Arc<io::Error>),
	Parquet(ParquetError),
}
impl error::Error for Error {}
impl Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Error::Io(err) => err.fmt(f),
			Error::Parquet(err) => err.fmt(f),
		}
	}
}
impl From<io::Error> for Error {
	fn from(err: io::Error) -> Self {
		Error::Io(Arc::new(err))
	}
}
impl From<ParquetError> for Error {
	fn from(err: ParquetError) -> Self {
		Error::Parquet(err)
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
