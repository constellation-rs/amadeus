use std::{
	convert::TryFrom, ffi::{OsStr, OsString}, fs, future::Future, io::{self, Seek, SeekFrom}, path::{Path, PathBuf}, pin::Pin, sync::atomic::{AtomicU64, Ordering}
};

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

use super::{File, Page, Partition};
use crate::util::{IoError, ResultExpand};

impl<F> File for Vec<F>
where
	F: File,
{
	type Partition = F::Partition;
	type Error = F::Error;

	fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		self.into_iter()
			.flat_map(|file| ResultExpand(file.partitions()))
			.collect()
	}
}
impl<F> File for &[F]
where
	F: File + Clone,
{
	type Partition = F::Partition;
	type Error = F::Error;

	fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		self.iter()
			.cloned()
			.flat_map(|file| ResultExpand(file.partitions()))
			.collect()
	}
}
impl File for PathBuf {
	type Partition = Self;
	type Error = IoError;

	fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		Ok(vec![self])
	}
}
impl Partition for PathBuf {
	type Page = LocalFile;
	type Error = IoError;

	fn pages(self) -> Result<Vec<Self::Page>, Self::Error> {
		Ok(vec![LocalFile::open(self)?])
	}
}
impl File for &Path {
	type Partition = PathBuf;
	type Error = IoError;

	fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		Ok(vec![self.to_owned()])
	}
}
impl File for String {
	type Partition = Self;
	type Error = IoError;

	fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		Ok(vec![self])
	}
}
impl Partition for String {
	type Page = LocalFile;
	type Error = IoError;

	fn pages(self) -> Result<Vec<Self::Page>, Self::Error> {
		Ok(vec![LocalFile::open(self)?])
	}
}
impl File for &str {
	type Partition = String;
	type Error = IoError;

	fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		Ok(vec![self.to_owned()])
	}
}
impl File for OsString {
	type Partition = Self;
	type Error = IoError;

	fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		Ok(vec![self])
	}
}
impl Partition for OsString {
	type Page = LocalFile;
	type Error = IoError;

	fn pages(self) -> Result<Vec<Self::Page>, Self::Error> {
		Ok(vec![LocalFile::open(self)?])
	}
}
impl File for &OsStr {
	type Partition = OsString;
	type Error = IoError;

	fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		Ok(vec![self.to_owned()])
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

pub struct LocalFile {
	file: fs::File,
	len: AtomicU64,
	#[cfg(windows)]
	pos: u64,
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
		Ok(Self { file, len })
	}

	#[cfg(windows)]
	fn from_file(mut file: fs::File) -> io::Result<Self> {
		let pos = file.seek(SeekFrom::Current(0))?;
		let len = file.seek(SeekFrom::End(0))?;
		let len = AtomicU64::new(len);
		Ok(Self { file, len, pos })
	}

	#[cfg(unix)]
	fn into_file(self) -> io::Result<fs::File> {
		Ok(self.file)
	}

	#[cfg(windows)]
	fn into_file(mut self) -> io::Result<fs::File> {
		self.file.seek(SeekFrom::Start(self.pos)).map(|_| self.file)
	}
}

impl From<fs::File> for LocalFile {
	fn from(file: fs::File) -> Self {
		Self::from_file(file).unwrap()
	}
}
impl From<LocalFile> for fs::File {
	fn from(file: LocalFile) -> Self {
		file.into_file().unwrap()
	}
}

#[cfg(unix)]
impl LocalFile {
	#[inline]
	fn read_at(&self, pos: u64, buf: &mut [u8]) -> io::Result<usize> {
		FileExt::read_at(&self.file, buf, pos)
	}
	#[inline]
	fn write_at(&self, pos: u64, buf: &[u8]) -> io::Result<usize> {
		FileExt::write_at(&self.file, buf, pos)
	}
}

#[cfg(windows)]
impl LocalFile {
	#[inline]
	fn read_at(&self, pos: u64, buf: &mut [u8]) -> io::Result<usize> {
		FileExt::seek_read(&self.file, buf, pos)
	}
	#[inline]
	fn write_at(&self, pos: u64, buf: &[u8]) -> io::Result<usize> {
		FileExt::seek_write(&self.file, buf, pos)
	}
}

impl Page for LocalFile {
	type Error = IoError;

	fn len(&self) -> u64 {
		self.len.load(Ordering::Relaxed)
	}
	fn set_len(&self, len: u64) -> Result<(), Self::Error> {
		self.file.set_len(len)?;
		self.len.store(len, Ordering::Relaxed);
		Ok(())
	}
	fn read<'a>(
		&'a self, mut offset: u64, mut buf: &'a mut [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
		Box::pin(async move {
			while !buf.is_empty() {
				match self.read_at(offset, buf) {
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
				Ok(())
			}
		})
	}
	fn write<'a>(
		&'a self, mut offset: u64, mut buf: &'a [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
		Box::pin(async move {
			while !buf.is_empty() {
				match self.write_at(offset, buf) {
					Ok(0) => {
						return Err(io::Error::new(
							io::ErrorKind::WriteZero,
							"failed to write whole buffer",
						)
						.into())
					}
					Ok(n) => {
						let _ = self
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
