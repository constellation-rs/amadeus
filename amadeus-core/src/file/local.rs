use std::{
	convert::TryFrom, ffi::{OsStr, OsString}, fs, future::Future, io::{self, Seek, SeekFrom}, path::{Path, PathBuf}, pin::Pin, sync::atomic::{AtomicU64, Ordering}
};
use walkdir::WalkDir;

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

use super::{Directory, File, Page, Partition};
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
impl Directory for PathBuf {
	fn partitions_filter<F>(self, f: F) -> Result<Vec<Self::Partition>, Self::Error>
	where
		F: FnMut(&super::PathBuf) -> bool,
	{
		(*self).partitions_filter(f)
	}
}
impl Partition for PathBuf {
	type Page = LocalFile;
	type Error = IoError;

	fn pages(self) -> Result<Vec<Self::Page>, Self::Error> {
		Ok(vec![LocalFile::open(self)?])
	}
}
impl Directory for &Path {
	fn partitions_filter<F>(self, mut f: F) -> Result<Vec<Self::Partition>, Self::Error>
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
impl File for &Path {
	type Partition = PathBuf;
	type Error = IoError;

	fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		PathBuf::partitions(self.into())
	}
}
impl File for String {
	type Partition = PathBuf;
	type Error = IoError;

	fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		PathBuf::partitions(self.into())
	}
}
impl File for &str {
	type Partition = PathBuf;
	type Error = IoError;

	fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		PathBuf::partitions(self.into())
	}
}
impl File for OsString {
	type Partition = PathBuf;
	type Error = IoError;

	fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		PathBuf::partitions(self.into())
	}
}
impl File for &OsStr {
	type Partition = PathBuf;
	type Error = IoError;

	fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		PathBuf::partitions(self.into())
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
