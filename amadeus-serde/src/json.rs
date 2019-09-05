use serde::{Deserialize, Serialize};
use serde_closure::*;
use serde_json::Error as SerdeJsonError;
use std::{
	error, fmt::{self, Debug, Display}, io::{self, BufReader}, iter, marker::PhantomData, path::PathBuf, vec
};
use walkdir::WalkDir;

use amadeus_core::{
	dist_iter::DistributedIterator, file::{File, Page, Partition, Reader}, into_dist_iter::IntoDistributedIterator, util::ResultExpand, Source
};

use super::{SerdeData, SerdeDeserialize};

type Closure<Env, Args, Output> =
	serde_closure::FnMut<Env, for<'r> fn(&'r mut Env, Args) -> Output>;

pub type JsonInner<F, Row> = amadeus_core::dist_iter::FlatMap<
	amadeus_core::into_dist_iter::IterIter<vec::IntoIter<<F as File>::Partition>>,
	Closure<
		(),
		(<F as File>::Partition,),
		iter::Map<
			iter::FlatMap<
				amadeus_core::util::ResultExpandIter<
					vec::IntoIter<<<F as File>::Partition as Partition>::Page>,
					JsonError<F>,
				>,
				ResultExpand<
					iter::Map<
						serde_json::StreamDeserializer<
							'static,
							serde_json::de::IoRead<
								BufReader<Reader<<<F as File>::Partition as Partition>::Page>>,
							>,
							SerdeDeserialize<Row>,
						>,
						Closure<
							(),
							(Result<SerdeDeserialize<Row>, SerdeJsonError>,),
							Result<Row, SerdeJsonError>,
						>,
					>,
					JsonError<F>,
				>,
				Closure<
					(),
					(Result<<<F as File>::Partition as Partition>::Page, JsonError<F>>,),
					ResultExpand<
						iter::Map<
							serde_json::StreamDeserializer<
								'static,
								serde_json::de::IoRead<
									BufReader<Reader<<<F as File>::Partition as Partition>::Page>>,
								>,
								SerdeDeserialize<Row>,
							>,
							Closure<
								(),
								(Result<SerdeDeserialize<Row>, SerdeJsonError>,),
								Result<Row, SerdeJsonError>,
							>,
						>,
						JsonError<F>,
					>,
				>,
			>,
			Closure<
				(),
				(Result<Result<Row, SerdeJsonError>, JsonError<F>>,),
				Result<Row, JsonError<F>>,
			>,
		>,
	>,
>;

#[derive(Clone)]
pub struct Json<File, Row>
where
	File: amadeus_core::file::File,
	Row: SerdeData,
{
	partitions: Vec<File::Partition>,
	marker: PhantomData<fn() -> Row>,
}
impl<F, Row> Json<F, Row>
where
	F: File,
	Row: SerdeData,
{
	pub fn new(file: F) -> Result<Self, JsonError<F>> {
		Ok(Self {
			partitions: file.partitions().map_err(JsonError::<F>::File)?,
			marker: PhantomData,
		})
	}
}
impl<F, Row> Source for Json<F, Row>
where
	F: File,
	Row: SerdeData,
{
	type Item = Row;
	type Error = JsonError<F>;

	// type DistIter = impl DistributedIterator<Item = Result<Row, Self::Error>>; //, <Self as super::super::DistributedIterator>::Task: Serialize + for<'de> Deserialize<'de>
	type DistIter = JsonInner<F, Row>;
	type Iter = iter::Empty<Result<Row, Self::Error>>;

	fn dist_iter(self) -> Self::DistIter {
		self.partitions
			.into_dist_iter()
			.flat_map(FnMut!(|partition: F::Partition| {
				ResultExpand(partition.pages().map_err(JsonError::<F>::Partition))
					.into_iter()
					.flat_map(FnMut!(|page: Result<_, _>| ResultExpand(page.map(
						|page| {
							let reader = BufReader::new(Page::reader(page));
							serde_json::Deserializer::from_reader(reader)
								.into_iter()
								.map(FnMut!(|x: Result<
									SerdeDeserialize<Row>,
									SerdeJsonError,
								>| Ok(x?.0)))
						}
					))))
					.map(FnMut!(|row: Result<
						Result<Row, SerdeJsonError>,
						Self::Error,
					>| Ok(row??)))
			}))
	}
	fn iter(self) -> Self::Iter {
		iter::empty()
		// self.files
		// 	.into_iter()
		// 	.flat_map(|file: PathBuf| {
		// 		let files = if !file.is_dir() {
		// 			sum::Sum2::A(iter::once(Ok(file)))
		// 		} else {
		// 			sum::Sum2::B(get_json_partitions(file))
		// 		};
		// 		files
		// 			.flat_map(|file: Result<PathBuf, _>| ResultExpand(
		// 				file.and_then(|file| Ok(fs::File::open(file)?)).map(|file| {
		// 					serde_json::Deserializer::from_reader(file)
		// 						.into_iter()
		// 						.map(FnMut!(|x: Result<SerdeDeserialize<Row>, SerdeJsonError>| Ok(
		// 							x?.0
		// 						)))
		// 				})
		// 			))
		// 			.map(|row: Result<Result<Row, SerdeJsonError>, io::Error>| Ok(row??))
		// 	})
	}
}

/// "Logic" interpreted from https://github.com/apache/arrow/blob/927cfeff875e557e28649891ea20ca38cb9d1536/python/pyarrow/parquet.py#L705-L829
/// and https://github.com/apache/spark/blob/5a7403623d0525c23ab8ae575e9d1383e3e10635/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/InMemoryFileIndex.scala#L348-L359
fn _get_json_partitions(dir: PathBuf) -> vec::IntoIter<Result<PathBuf, io::Error>> {
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

mod jsonerror {
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
}

pub type JsonError<F> = JsonErrorInternal<
	<F as File>::Error,
	<<F as File>::Partition as Partition>::Error,
	<<<F as File>::Partition as Partition>::Page as Page>::Error,
>;

#[derive(Serialize, Deserialize, Debug)]
pub enum JsonErrorInternal<A, B, C> {
	File(A),
	Partition(B),
	Page(C),
	Json(#[serde(with = "jsonerror")] SerdeJsonError),
}
impl<A, B, C> Clone for JsonErrorInternal<A, B, C>
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
			Self::Json(err) => Self::Json(serde::de::Error::custom(err)),
		}
	}
}
impl<A, B, C> PartialEq for JsonErrorInternal<A, B, C>
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
			(Self::Json(a), Self::Json(b)) => a.to_string() == b.to_string(),
			_ => false,
		}
	}
}
impl<A, B, C> error::Error for JsonErrorInternal<A, B, C>
where
	A: error::Error,
	B: error::Error,
	C: error::Error,
{
}
impl<A, B, C> Display for JsonErrorInternal<A, B, C>
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
			Self::Json(err) => Display::fmt(err, f),
		}
	}
}
impl<A, B, C> From<SerdeJsonError> for JsonErrorInternal<A, B, C> {
	fn from(err: SerdeJsonError) -> Self {
		Self::Json(err)
	}
}
