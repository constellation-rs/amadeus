use csv::Error as SerdeCsvError;
use serde::{Deserialize, Serialize};
use serde_closure::*;
use std::{
	error, fmt::{self, Display}, iter, marker::PhantomData
};

use amadeus_core::{
	dist_iter::DistributedIterator, file::{File, Page, Partition}, into_dist_iter::IntoDistributedIterator, util::ResultExpand, Source
};

use super::{SerdeData, SerdeDeserializeGroup};

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
pub struct Csv<File, Row>
where
	File: amadeus_core::file::File,
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
	partitions: Vec<File::Partition>,
	marker: PhantomData<fn() -> Row>,
}
impl<F, Row> Csv<F, Row>
where
	F: File,
	Row: SerdeData,
{
	pub fn new(file: F) -> Result<Self, <Self as Source>::Error> {
		Ok(Self {
			partitions: file.partitions().map_err(CsvError::File)?,
			marker: PhantomData,
		})
	}
	// pub fn open<Row>(files: Vec<PathBuf>) -> Csv<Row> {}
	// pub fn create<Row>(files: Vec<PathBuf>) -> Csv<Row> {}
}
impl<F, Row> Source for Csv<F, Row>
where
	F: File,
	Row: SerdeData,
{
	type Item = Row;
	#[allow(clippy::type_complexity)]
	type Error = CsvError<
		<F as File>::Error,
		<<F as File>::Partition as Partition>::Error,
		<<<F as File>::Partition as Partition>::Page as Page>::Error,
	>;

	#[cfg(not(feature = "doc"))]
	type DistIter = impl DistributedIterator<Item = Result<Self::Item, Self::Error>>;
	#[cfg(feature = "doc")]
	type DistIter = amadeus_core::util::ImplDistributedIterator<Result<Self::Item, Self::Error>>;
	type Iter = iter::Empty<Result<Self::Item, Self::Error>>;

	#[allow(clippy::let_and_return)]
	fn dist_iter(self) -> Self::DistIter {
		let ret = self
			.partitions
			.into_dist_iter()
			.flat_map(FnMut!(|partition: F::Partition| {
				ResultExpand(partition.pages().map_err(CsvError::Partition))
					.into_iter()
					.flat_map(|page: Result<_, _>| {
						ResultExpand(page.map(|page| {
							csv::ReaderBuilder::new()
								.has_headers(false)
								.from_reader(Page::reader(page))
								.into_deserialize()
								.map(
									|x: Result<SerdeDeserializeGroup<Row>, SerdeCsvError>| Ok(x?.0),
								)
						}))
					})
					.map(|row: Result<Result<Row, SerdeCsvError>, Self::Error>| Ok(row??))
			}));
		#[cfg(feature = "doc")]
		let ret = amadeus_core::util::ImplDistributedIterator::new(ret);
		ret
	}
	fn iter(self) -> Self::Iter {
		iter::empty()
	}
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
pub enum CsvError<A, B, C> {
	File(A),
	Partition(B),
	Page(C),
	Csv(#[serde(with = "csverror")] SerdeCsvError),
}
impl<A, B, C> Clone for CsvError<A, B, C>
where
	A: Clone,
	B: Clone,
	C: Clone,
{
	fn clone(&self) -> Self {
		match self {
			Self::File(err) => Self::File(err.clone()),
			Self::Partition(err) => Self::Partition(err.clone()),
			Self::Page(err) => Self::Page(err.clone()),
			Self::Csv(err) => Self::Csv(serde::ser::Error::custom(err)),
		}
	}
}
impl<A, B, C> PartialEq for CsvError<A, B, C>
where
	A: PartialEq,
	B: PartialEq,
	C: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Self::File(a), Self::File(b)) => a.eq(b),
			(Self::Partition(a), Self::Partition(b)) => a.eq(b),
			(Self::Page(a), Self::Page(b)) => a.eq(b),
			(Self::Csv(a), Self::Csv(b)) => a.to_string() == b.to_string(),
			_ => false,
		}
	}
}
impl<A, B, C> error::Error for CsvError<A, B, C>
where
	A: error::Error,
	B: error::Error,
	C: error::Error,
{
}
impl<A, B, C> Display for CsvError<A, B, C>
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
			Self::Csv(err) => Display::fmt(err, f),
		}
	}
}
impl<A, B, C> From<SerdeCsvError> for CsvError<A, B, C> {
	fn from(err: SerdeCsvError) -> Self {
		Self::Csv(err)
	}
}
