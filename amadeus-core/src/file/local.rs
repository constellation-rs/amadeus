#![allow(clippy::needless_lifetimes)]

use async_trait::async_trait;
use futures::{future::BoxFuture, stream, FutureExt, StreamExt, TryStreamExt};
use std::{
	convert::TryFrom, ffi::{OsStr, OsString}, fs, io::{self, Seek, SeekFrom}, path::{Path, PathBuf}, sync::{
		atomic::{AtomicU64, Ordering}, Arc
	}
};
use walkdir::WalkDir;

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

use super::{Directory, File, Page, Partition};
use crate::util::{IoError, ResultExpand};

#[async_trait(?Send)]
impl<F> File for Vec<F>
where
	F: File,
{
	type Partition = F::Partition;
	type Error = F::Error;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		stream::iter(self.into_iter())
			.flat_map(|file| {
				async { stream::iter(ResultExpand(file.partitions().await)) }.flatten_stream()
			})
			.try_collect()
			.await
	}
}
#[async_trait(?Send)]
impl<F> File for &[F]
where
	F: File + Clone,
{
	type Partition = F::Partition;
	type Error = F::Error;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		stream::iter(self.iter().cloned())
			.flat_map(|file| {
				async { stream::iter(ResultExpand(file.partitions().await)) }.flatten_stream()
			})
			.try_collect()
			.await
	}
}
#[async_trait(?Send)]
impl File for PathBuf {
	type Partition = Self;
	type Error = IoError;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		Ok(vec![self])
	}
}
#[async_trait(?Send)]
impl Directory for PathBuf {
	async fn partitions_filter<F>(
		self, f: F,
	) -> Result<Vec<<Self as File>::Partition>, <Self as File>::Error>
	where
		F: FnMut(&super::PathBuf) -> bool,
	{
		(*self).partitions_filter(f).await
	}
}
#[async_trait(?Send)]
impl Partition for PathBuf {
	type Page = LocalFile;
	type Error = IoError;

	async fn pages(self) -> Result<Vec<Self::Page>, Self::Error> {
		Ok(vec![LocalFile::open(self)?])
	}
}
#[async_trait(?Send)]
impl Directory for &Path {
	async fn partitions_filter<F>(
		self, mut f: F,
	) -> Result<Vec<<Self as File>::Partition>, <Self as File>::Error>
	where
		F: FnMut(&super::PathBuf) -> bool,
	{
		WalkDir::new(self)
			.follow_links(true)
			.sort_by(|a, b| a.file_name().cmp(b.file_name()))
			.into_iter()
			.filter_entry(|e| {
				let is_dir = e.file_type().is_dir();
				let path = e.path();
				if path == self {
					return true;
				}
				let mut path = path.strip_prefix(self).unwrap();
				let mut path_buf = if !cfg!(windows) {
					super::PathBuf::new()
				} else {
					super::PathBuf::new_wide()
				};
				let mut file_name = None;
				#[cfg(unix)]
				let into = |osstr: &OsStr| -> Vec<u8> {
					std::os::unix::ffi::OsStrExt::as_bytes(osstr).to_owned()
				};
				#[cfg(windows)]
				let into = |osstr: &OsStr| -> Vec<u8> {
					std::os::windows::ffi::OsStrExt::encode_wide(osstr)
						.flat_map(|char| {
							let char = char.to_be();
							[
								u8::try_from(char >> 8).unwrap(),
								u8::try_from(char & 0xff).unwrap(),
							]
							.iter()
							.copied()
						})
						.collect()
				};
				if !is_dir {
					file_name = Some(path.file_name().unwrap());
					path = path.parent().unwrap();
				}
				for component in path {
					path_buf.push(into(component));
				}
				path_buf.set_file_name(file_name.map(into));
				f(&path_buf)
			})
			.filter_map(|e| match e {
				Ok(ref e) if e.file_type().is_dir() => None,
				Ok(e) => Some(Ok(e.into_path())),
				Err(e) => Some(Err(if e.io_error().is_some() {
					e.into_io_error().unwrap()
				} else {
					io::Error::new(io::ErrorKind::Other, e)
				}
				.into())),
			})
			.collect()
	}
}
#[async_trait(?Send)]
impl File for &Path {
	type Partition = PathBuf;
	type Error = IoError;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		PathBuf::partitions(self.into()).await
	}
}
#[async_trait(?Send)]
impl File for String {
	type Partition = PathBuf;
	type Error = IoError;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		PathBuf::partitions(self.into()).await
	}
}
#[async_trait(?Send)]
impl File for &str {
	type Partition = PathBuf;
	type Error = IoError;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		PathBuf::partitions(self.into()).await
	}
}
#[async_trait(?Send)]
impl File for OsString {
	type Partition = PathBuf;
	type Error = IoError;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		PathBuf::partitions(self.into()).await
	}
}
#[async_trait(?Send)]
impl File for &OsStr {
	type Partition = PathBuf;
	type Error = IoError;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		PathBuf::partitions(self.into()).await
	}
}
// impl File for fs::File {
// 	type Partition = Self;
// 	type Error = IoError;

// 	fn partitions(self) -> Result<Vec<Self::Partition>,Self::Error> {
// 		Ok(vec![self])
// 	}
// }
// impl Partition for fs::File {
// 	type Page = LocalFile;
// 	type Error = IoError;

// 	fn pages(self) -> Result<Vec<Self::Page>,Self::Error> {
// 		Ok(vec![LocalFile::from_file(self)?])
// 	}
// }

// https://github.com/vasi/positioned-io/blob/a03c792f5b6f99cb4f72e146befdfc8b1f6e1d28/src/raf.rs

struct LocalFileInner {
	file: fs::File,
	len: AtomicU64,
	#[cfg(windows)]
	pos: u64,
}
pub struct LocalFile {
	inner: Arc<LocalFileInner>,
}
impl LocalFile {
	/// [Opens](https://doc.rust-lang.org/std/fs/struct.File.html#method.open)
	/// a file for random access.
	pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
		Self::from_file(fs::File::open(path)?)
	}

	#[cfg(unix)]
	fn from_file(mut file: fs::File) -> io::Result<Self> {
		let old_pos = file.seek(SeekFrom::Current(0))?;
		let len = file.seek(SeekFrom::End(0))?;
		if old_pos != len {
			file.seek(SeekFrom::Start(old_pos))?;
		}
		let len = AtomicU64::new(len);
		let inner = Arc::new(LocalFileInner { file, len });
		Ok(Self { inner })
	}

	#[cfg(windows)]
	fn from_file(mut file: fs::File) -> io::Result<Self> {
		let pos = file.seek(SeekFrom::Current(0))?;
		let len = file.seek(SeekFrom::End(0))?;
		let len = AtomicU64::new(len);
		let inner = Arc::new(LocalFileInner { file, len, pos });
		Ok(Self { inner })
	}

	// #[cfg(unix)]
	// fn into_file(self) -> io::Result<fs::File> {
	// 	Ok(self.file)
	// }

	// #[cfg(windows)]
	// fn into_file(mut self) -> io::Result<fs::File> {
	// 	self.file.seek(SeekFrom::Start(self.pos)).map(|_| self.file)
	// }
}

impl From<fs::File> for LocalFile {
	fn from(file: fs::File) -> Self {
		Self::from_file(file).unwrap()
	}
}
// impl From<LocalFile> for fs::File {
// 	fn from(file: LocalFile) -> Self {
// 		file.into_file().unwrap()
// 	}
// }

#[cfg(unix)]
impl LocalFile {
	#[inline]
	fn read_at(&self, pos: u64, buf: &mut [u8]) -> io::Result<usize> {
		FileExt::read_at(&self.inner.file, buf, pos)
	}
	#[inline]
	fn write_at(&self, pos: u64, buf: &[u8]) -> io::Result<usize> {
		FileExt::write_at(&self.inner.file, buf, pos)
	}
}

#[cfg(windows)]
impl LocalFile {
	#[inline]
	fn read_at(&self, pos: u64, buf: &mut [u8]) -> io::Result<usize> {
		FileExt::seek_read(&self.inner.file, buf, pos)
	}
	#[inline]
	fn write_at(&self, pos: u64, buf: &[u8]) -> io::Result<usize> {
		FileExt::seek_write(&self.inner.file, buf, pos)
	}
}

#[async_trait]
impl Page for LocalFile {
	type Error = IoError;

	fn len(&self) -> u64 {
		self.inner.len.load(Ordering::Relaxed)
	}
	fn set_len(&self, len: u64) -> Result<(), Self::Error> {
		self.inner.file.set_len(len)?;
		self.inner.len.store(len, Ordering::Relaxed);
		Ok(())
	}
	fn read(
		&self, mut offset: u64, len: usize,
	) -> BoxFuture<'static, Result<Box<[u8]>, Self::Error>> {
		let self_ = LocalFile {
			inner: self.inner.clone(),
		};
		Box::pin(async move {
			let mut buf_ = vec![0; len].into_boxed_slice();
			let mut buf = &mut *buf_;
			while !buf.is_empty() {
				match self_.read_at(offset, buf) {
					Ok(0) => break,
					Ok(n) => {
						let tmp = buf;
						buf = &mut tmp[n..];
						offset += n as u64;
					}
					Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
					Err(e) => return Err(e.into()),
				}
			}
			if !buf.is_empty() {
				Err(
					io::Error::new(io::ErrorKind::UnexpectedEof, "failed to fill whole buffer")
						.into(),
				)
			} else {
				Ok(buf_)
			}
		})
	}
	fn write(
		&self, mut offset: u64, buf: Box<[u8]>,
	) -> BoxFuture<'static, Result<(), Self::Error>> {
		let self_ = LocalFile {
			inner: self.inner.clone(),
		};
		Box::pin(async move {
			let mut buf = &*buf;
			while !buf.is_empty() {
				match self_.write_at(offset, buf) {
					Ok(0) => {
						return Err(io::Error::new(
							io::ErrorKind::WriteZero,
							"failed to write whole buffer",
						)
						.into())
					}
					Ok(n) => {
						let _ = self_
							.inner
							.len
							.fetch_max(offset + u64::try_from(n).unwrap(), Ordering::Relaxed);
						buf = &buf[n..];
						offset += n as u64
					}
					Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
					Err(e) => return Err(e.into()),
				}
			}
			Ok(())
		})
	}
}
