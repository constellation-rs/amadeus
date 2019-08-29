use super::SerdeData;
use amadeus_core::{
	dist_iter::{Consumer, DistributedIterator}, into_dist_iter::IntoDistributedIterator, util::ResultExpand
};
use serde::{Deserialize, Serialize};
use std::{
	error, fmt::{self, Display}, fs::File, io, iter, marker::PhantomData, path::PathBuf, sync::Arc, vec
};
use walkdir::WalkDir;

use super::SerdeDeserializeGroup;
use csv::Error as CsvError;
use serde_closure::*;
use std::ops::FnMut;

type Closure<Env, Args, Output> =
	serde_closure::FnMut<Env, for<'r> fn(&'r mut Env, Args) -> Output>;

type CsvInner<Row> = amadeus_core::dist_iter::FlatMap<
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
						csv::DeserializeRecordsIntoIter<File, SerdeDeserializeGroup<Row>>,
						Closure<
							(),
							(Result<SerdeDeserializeGroup<Row>, CsvError>,),
							Result<Row, CsvError>,
						>,
					>,
					io::Error,
				>,
				Closure<
					(),
					(Result<PathBuf, io::Error>,),
					ResultExpand<
						iter::Map<
							csv::DeserializeRecordsIntoIter<File, SerdeDeserializeGroup<Row>>,
							Closure<
								(),
								(Result<SerdeDeserializeGroup<Row>, CsvError>,),
								Result<Row, CsvError>,
							>,
						>,
						io::Error,
					>,
				>,
			>,
			Closure<(), (Result<Result<Row, CsvError>, io::Error>,), Result<Row, Error>>,
		>,
	>,
>;

pub struct Csv<Row>
where
	Row: SerdeData,
{
	i: CsvInner<Row>,
}
impl<Row> Csv<Row>
where
	Row: SerdeData,
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
					sum::Sum2::B(get_csv_partitions(file))
				};
				files
					.flat_map(FnMut!(|file: Result<PathBuf, _>| ResultExpand(
						file.and_then(|file| Ok(File::open(file)?)).map(|file| {
							csv::ReaderBuilder::new()
								.has_headers(false)
								.from_reader(file)
								.into_deserialize()
								.map(FnMut!(
									|x: Result<SerdeDeserializeGroup<Row>, CsvError>| Ok(x?.0)
								))
						})
					)))
					.map(FnMut!(|row: Result<Result<Row, _>, _>| Ok(row??)))
			}));
		Ok(Self { i })
	}
}

/// "Logic" interpreted from https://github.com/apache/arrow/blob/927cfeff875e557e28649891ea20ca38cb9d1536/python/pyarrow/parquet.py#L705-L829
/// and https://github.com/apache/spark/blob/5a7403623d0525c23ab8ae575e9d1383e3e10635/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/InMemoryFileIndex.scala#L348-L359
fn get_csv_partitions(dir: PathBuf) -> vec::IntoIter<Result<PathBuf, io::Error>> {
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
					// || (extension.is_some() && extension.unwrap() == "_COPYING_") // File copy in progress; TODO: Emit error on this.
					(extension.is_some() && extension.unwrap() == "crc") // Checksums
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

// mod misc_serde {
// 	use csv::Error as CsvError;
// 	use serde::{Deserialize, Deserializer, Serialize, Serializer};
// 	use std::{io, sync::Arc};

// 	pub struct Serde<T>(T);

// 	impl Serialize for Serde<&io::ErrorKind> {
// 		fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
// 		where
// 			S: Serializer,
// 		{
// 			usize::serialize(
// 				&match self.0 {
// 					io::ErrorKind::NotFound => 0,
// 					io::ErrorKind::PermissionDenied => 1,
// 					io::ErrorKind::ConnectionRefused => 2,
// 					io::ErrorKind::ConnectionReset => 3,
// 					io::ErrorKind::ConnectionAborted => 4,
// 					io::ErrorKind::NotConnected => 5,
// 					io::ErrorKind::AddrInUse => 6,
// 					io::ErrorKind::AddrNotAvailable => 7,
// 					io::ErrorKind::BrokenPipe => 8,
// 					io::ErrorKind::AlreadyExists => 9,
// 					io::ErrorKind::WouldBlock => 10,
// 					io::ErrorKind::InvalidInput => 11,
// 					io::ErrorKind::InvalidData => 12,
// 					io::ErrorKind::TimedOut => 13,
// 					io::ErrorKind::WriteZero => 14,
// 					io::ErrorKind::Interrupted => 15,
// 					io::ErrorKind::UnexpectedEof => 17,
// 					_ => 16,
// 				},
// 				serializer,
// 			)
// 		}
// 	}
// 	impl<'de> Deserialize<'de> for Serde<io::ErrorKind> {
// 		fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
// 		where
// 			D: Deserializer<'de>,
// 		{
// 			usize::deserialize(deserializer)
// 				.map(|kind| match kind {
// 					0 => io::ErrorKind::NotFound,
// 					1 => io::ErrorKind::PermissionDenied,
// 					2 => io::ErrorKind::ConnectionRefused,
// 					3 => io::ErrorKind::ConnectionReset,
// 					4 => io::ErrorKind::ConnectionAborted,
// 					5 => io::ErrorKind::NotConnected,
// 					6 => io::ErrorKind::AddrInUse,
// 					7 => io::ErrorKind::AddrNotAvailable,
// 					8 => io::ErrorKind::BrokenPipe,
// 					9 => io::ErrorKind::AlreadyExists,
// 					10 => io::ErrorKind::WouldBlock,
// 					11 => io::ErrorKind::InvalidInput,
// 					12 => io::ErrorKind::InvalidData,
// 					13 => io::ErrorKind::TimedOut,
// 					14 => io::ErrorKind::WriteZero,
// 					15 => io::ErrorKind::Interrupted,
// 					17 => io::ErrorKind::UnexpectedEof,
// 					_ => io::ErrorKind::Other,
// 				})
// 				.map(Self)
// 		}
// 	}

// 	impl Serialize for Serde<&Arc<io::Error>> {
// 		fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
// 		where
// 			S: Serializer,
// 		{
// 			<(Serde<&io::ErrorKind>, String)>::serialize(
// 				&(Serde(&self.0.kind()), self.0.to_string()),
// 				serializer,
// 			)
// 		}
// 	}
// 	impl<'de> Deserialize<'de> for Serde<Arc<io::Error>> {
// 		fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
// 		where
// 			D: Deserializer<'de>,
// 		{
// 			<(Serde<io::ErrorKind>, String)>::deserialize(deserializer)
// 				.map(|(kind, message)| Arc::new(io::Error::new(kind.0, message)))
// 				.map(Self)
// 		}
// 	}

// 	impl Serialize for Serde<&CsvError> {
// 		fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
// 		where
// 			S: Serializer,
// 		{
// 			panic!()
// 		}
// 	}
// 	impl<'de> Deserialize<'de> for Serde<CsvError> {
// 		fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
// 		where
// 			D: Deserializer<'de>,
// 		{
// 			panic!()
// 		}
// 	}

// 	pub fn serialize<T, S>(t: &T, serializer: S) -> Result<S::Ok, S::Error>
// 		S: Serializer,
// 	{
// 		Serde(t).serialize(serializer)
// 	}
// 	pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
// 	where
// 		Serde<T>: Deserialize<'de>,
// 		D: Deserializer<'de>,
// 	{
// 		Serde::<T>::deserialize(deserializer).map(|x| x.0)
// 	}
// }

mod csverror {
	use serde::{Deserializer, Serializer};

	pub fn serialize<T, S>(_t: &T, _serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		unimplemented!()
	}
	pub fn deserialize<'de, T, D>(_deserializer: D) -> Result<T, D::Error>
	where
		D: Deserializer<'de>,
	{
		unimplemented!()
	}

	// TODO
	// #[derive(Serialize, Deserialize)]
	// #[serde(remote = "csv::Error")]
	// struct Error(Box<ErrorKind>);
	// #[derive(Serialize, Deserialize)]
	// #[serde(remote = "csv::ErrorKind")]
	// enum ErrorKind {
	// 	Io(io::Error),
	// 	Utf8 {
	// 		pos: Option<Position>,
	// 		err: Utf8Error,
	// 	},
	// 	UnequalLengths {
	// 		pos: Option<Position>,
	// 		expected_len: u64,
	// 		len: u64,
	// 	},
	// 	Seek,
	// 	Serialize(String),
	// 	Deserialize {
	// 		pos: Option<Position>,
	// 		err: DeserializeError,
	// 	},
	// }
	// #[derive(Serialize, Deserialize)]
	// #[serde(remote = "csv::Position")]
	// struct Position {
	// 	byte: u64,
	// 	line: u64,
	// 	record: u64,
	// }
	// #[derive(Serialize, Deserialize)]
	// #[serde(remote = "csv::Utf8Error")]
	// struct Utf8Error {
	// 	field: usize,
	// 	valid_up_to: usize,
	// }
	// #[derive(Serialize, Deserialize)]
	// #[serde(remote = "csv::DeserializeError")]
	// struct DeserializeError {
	// 	field: Option<u64>,
	// 	kind: DeserializeErrorKind,
	// }
	// #[derive(Serialize, Deserialize)]
	// #[serde(remote = "csv::DeserializeErrorKind")]
	// enum DeserializeErrorKind {
	// 	Message(String),
	// 	Unsupported(String),
	// 	UnexpectedEndOfRow,
	// 	InvalidUtf8(str::Utf8Error),
	// 	ParseBool(str::ParseBoolError),
	// 	ParseInt(num::ParseIntError),
	// 	ParseFloat(num::ParseFloatError),
	// }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Error {
	Io(#[serde(with = "amadeus_core::misc_serde")] Arc<io::Error>),
	Csv(#[serde(with = "csverror")] CsvError),
}
impl PartialEq for Error {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Self::Io(a), Self::Io(b)) => a.to_string() == b.to_string(),
			(Self::Csv(a), Self::Csv(b)) => a.to_string() == b.to_string(),
			_ => false,
		}
	}
}
impl error::Error for Error {}
impl Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Io(err) => err.fmt(f),
			Self::Csv(err) => err.fmt(f),
		}
	}
}
impl From<io::Error> for Error {
	fn from(err: io::Error) -> Self {
		Self::Io(Arc::new(err))
	}
}
impl From<CsvError> for Error {
	fn from(err: CsvError) -> Self {
		Self::Csv(err)
	}
}

impl<Row> DistributedIterator for Csv<Row>
where
	Row: SerdeData,
{
	type Item = Result<Row, Error>;
	type Task = CsvConsumer<Row>;

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.i.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.i.next_task().map(|task| CsvConsumer::<Row> {
			task,
			marker: PhantomData,
		})
	}
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct CsvConsumer<Row>
where
	Row: SerdeData,
{
	task: <CsvInner<Row> as DistributedIterator>::Task,
	marker: PhantomData<fn() -> Row>,
}

impl<Row> Consumer for CsvConsumer<Row>
where
	Row: SerdeData,
{
	type Item = Result<Row, Error>;

	fn run(self, i: &mut impl FnMut(Self::Item) -> bool) -> bool {
		self.task.run(i)
	}
}
