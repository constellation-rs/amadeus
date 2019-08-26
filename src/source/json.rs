use super::ResultExpand;
use crate::{data::Data, into_dist_iter::IntoDistributedIterator, DistributedIterator};
use serde::{Deserialize, Serialize};
use std::{
	error, fmt::{self, Display}, fs::File, io, iter, marker::PhantomData, path::PathBuf, sync::Arc, vec
};
use walkdir::WalkDir;

use serde_json::Error as JsonError;

use crate::data::SerdeDeserialize;

type Closure<Env, Args, Output> =
	serde_closure::FnMut<Env, for<'r> fn(&'r mut Env, Args) -> Output>;

mod private {
	use super::*;
	pub type JsonInner<Row> = crate::dist_iter::FlatMap<
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
							serde_json::StreamDeserializer<
								'static,
								serde_json::de::IoRead<File>,
								SerdeDeserialize<Row>,
							>,
							Closure<
								(),
								(Result<SerdeDeserialize<Row>, JsonError>,),
								Result<Row, JsonError>,
							>,
						>,
						io::Error,
					>,
					Closure<
						(),
						(Result<PathBuf, io::Error>,),
						ResultExpand<
							iter::Map<
								serde_json::StreamDeserializer<
									'static,
									serde_json::de::IoRead<File>,
									SerdeDeserialize<Row>,
								>,
								Closure<
									(),
									(Result<SerdeDeserialize<Row>, JsonError>,),
									Result<Row, JsonError>,
								>,
							>,
							io::Error,
						>,
					>,
				>,
				Closure<(), (Result<Result<Row, JsonError>, io::Error>,), Result<Row, Error>>,
			>,
		>,
	>;
}
use private::JsonInner;

pub struct Json<Row>
where
	Row: Data,
{
	files: Vec<PathBuf>,
	marker: PhantomData<fn() -> Row>,
}
impl<Row> Json<Row>
where
	Row: Data,
{
	pub fn new(files: Vec<PathBuf>) -> Self {
		Self {
			files,
			marker: PhantomData,
		}
	}
}
impl<Row> super::Source for Json<Row>
where
	Row: Data,
{
	type Item = Row;
	type Error = Error;

	// existential type DistIter: super::super::DistributedIterator<Item = Result<Row, Error>>;//, <Self as super::super::DistributedIterator>::Task: Serialize + for<'de> Deserialize<'de>
	type DistIter = JsonInner<Row>;
	type Iter = iter::Empty<Result<Row, Error>>;

	fn dist_iter(self) -> Self::DistIter {
		self.files
			.into_dist_iter()
			.flat_map(FnMut!(|file: PathBuf| {
				let files = if !file.is_dir() {
					sum::Sum2::A(iter::once(Ok(file)))
				} else {
					sum::Sum2::B(get_json_partitions(file))
				};
				files
					.flat_map(FnMut!(|file: Result<PathBuf, _>| ResultExpand(
						file.and_then(|file| Ok(File::open(file)?)).map(|file| {
							serde_json::Deserializer::from_reader(file)
								.into_iter()
								.map(FnMut!(|x: Result<SerdeDeserialize<Row>, JsonError>| Ok(
									x?.0
								)))
						})
					)))
					.map(FnMut!(|row: Result<Result<Row, JsonError>, io::Error>| Ok(
						row??
					)))
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
		// 				file.and_then(|file| Ok(File::open(file)?)).map(|file| {
		// 					serde_json::Deserializer::from_reader(file)
		// 						.into_iter()
		// 						.map(FnMut!(|x: Result<SerdeDeserialize<Row>, JsonError>| Ok(
		// 							x?.0
		// 						)))
		// 				})
		// 			))
		// 			.map(|row: Result<Result<Row, JsonError>, io::Error>| Ok(row??))
		// 	})
	}
}

/// "Logic" interpreted from https://github.com/apache/arrow/blob/927cfeff875e557e28649891ea20ca38cb9d1536/python/pyarrow/parquet.py#L705-L829
/// and https://github.com/apache/spark/blob/5a7403623d0525c23ab8ae575e9d1383e3e10635/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/InMemoryFileIndex.scala#L348-L359
fn get_json_partitions(dir: PathBuf) -> vec::IntoIter<Result<PathBuf, io::Error>> {
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

#[derive(Serialize, Deserialize, Debug)]
pub enum Error {
	Io(#[serde(with = "super::misc_serde")] Arc<io::Error>),
	Json(#[serde(with = "super::misc_serde")] JsonError),
}
impl PartialEq for Error {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Self::Io(a), Self::Io(b)) => a.to_string() == b.to_string(),
			(Self::Json(a), Self::Json(b)) => a.to_string() == b.to_string(),
			_ => false,
		}
	}
}
impl error::Error for Error {}
impl Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Io(err) => err.fmt(f),
			Self::Json(err) => err.fmt(f),
		}
	}
}
impl From<io::Error> for Error {
	fn from(err: io::Error) -> Self {
		Self::Io(Arc::new(err))
	}
}
impl From<JsonError> for Error {
	fn from(err: JsonError) -> Self {
		Self::Json(err)
	}
}
