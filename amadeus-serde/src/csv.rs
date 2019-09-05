use super::SerdeData;
use amadeus_core::{
	dist_iter::DistributedIterator, into_dist_iter::IntoDistributedIterator, util::ResultExpand
};
use serde::{Deserialize, Serialize};
use std::{
	error, fmt::{self, Display}, fs::File, io, iter, marker::PhantomData, path::PathBuf, sync::Arc, vec
};
use walkdir::WalkDir;

use super::SerdeDeserializeGroup;
use csv::Error as CsvError;
use serde_closure::*;

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

// #[doc(inline)]
// pub type Trim = csv::Trim;
// #[doc(inline)]
// pub type Terminator = csv::Terminator;

// open (assume exists,

// open: append/overwrite (fail on not existing)
// create: append/overwrite/fail on existing

// create, fail if exists
// create, overwrite if exists
// create, append if exists

// open, fail if not exists
// open, overwrite if exists
// open, append if exists

// open fail on exist/fail on not exist/no fail overwrite/append

// fail on exist
// fail on not exist; overwrite/append
// no fail; overwrite/append

// open: must exist; append
// create: overwrite

// trait File

#[derive(Clone)]
pub struct Csv<Row>
where
	Row: SerdeData,
{
	// delimiter: u8,
	// has_headers: bool,
	// flexible: bool,
	// trim: Trim,
	// terminator: Terminator,
	// quote: u8,
	// escape: Option<u8>,
	// double_quote: bool,
	// quoting: bool,
	// comment: Option<u8>,
	files: Vec<PathBuf>,
	marker: PhantomData<fn() -> Row>,
}
impl<Row> Csv<Row>
where
	Row: SerdeData,
{
	pub fn new(files: Vec<PathBuf>) -> Self {
		Self {
			files,
			marker: PhantomData,
		}
	}
	// pub fn open<Row>(files: Vec<PathBuf>) -> Csv<Row> {}
	// pub fn create<Row>(files: Vec<PathBuf>) -> Csv<Row> {}
}
impl<Row> amadeus_core::Source for Csv<Row>
where
	Row: SerdeData,
{
	type Item = Row;
	type Error = Error;

	// existential type DistIter: super::super::DistributedIterator<Item = Result<Row, Error>>;//, <Self as super::super::DistributedIterator>::Task: Serialize + for<'de> Deserialize<'de>
	type DistIter = CsvInner<Row>;
	type Iter = iter::Empty<Result<Row, Error>>;

	fn dist_iter(self) -> Self::DistIter {
		self.files
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
			}))
	}
	fn iter(self) -> Self::Iter {
		iter::empty()
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
