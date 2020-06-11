#![allow(clippy::type_complexity)]

mod local;

use async_trait::async_trait;
use futures::{future::BoxFuture, ready};
use pin_project::pin_project;
use std::{
	convert::TryFrom, error::Error, fmt, future::Future, io, pin::Pin, sync::Arc, task::{Context, Poll}
};

use crate::pool::ProcessSend;

pub use local::LocalFile;

const PAGE_SIZE: usize = 10 * 1024 * 1024; // `Reader` reads this many bytes at a time

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PathBuf {
	wide: bool, // for if the Vec<u8> is actually utf16
	components: Vec<Vec<u8>>,
	file_name: Option<Vec<u8>>,
}
impl PathBuf {
	pub fn new() -> Self {
		Self {
			wide: false,
			components: Vec::new(),
			file_name: None,
		}
	}
	pub fn new_wide() -> Self {
		Self {
			wide: true,
			components: Vec::new(),
			file_name: None,
		}
	}
	pub fn push<S>(&mut self, component: S)
	where
		S: Into<Vec<u8>>,
	{
		assert!(self.file_name.is_none());
		self.components.push(component.into());
	}
	pub fn pop(&mut self) -> Option<Vec<u8>> {
		assert!(self.file_name.is_none());
		self.components.pop()
	}
	pub fn last(&self) -> Option<String> {
		assert!(self.file_name.is_none());
		self.components
			.last()
			.map(|bytes| String::from_utf8_lossy(bytes).into_owned())
	}
	pub fn set_file_name<S>(&mut self, file_name: Option<S>)
	where
		S: Into<Vec<u8>>,
	{
		self.file_name = file_name.map(Into::into);
	}
	pub fn is_file(&self) -> bool {
		self.file_name.is_some()
	}
	pub fn file_name(&self) -> Option<String> {
		self.file_name
			.as_ref()
			.map(|file_name| String::from_utf8_lossy(file_name).into_owned())
	}
	pub fn depth(&self) -> usize {
		self.components.len()
	}
	pub fn iter<'a>(&'a self) -> impl Iterator<Item = String> + 'a {
		self.components
			.iter()
			.map(|bytes| String::from_utf8_lossy(bytes).into_owned())
	}
}
impl Default for PathBuf {
	fn default() -> Self {
		Self::new()
	}
}
impl fmt::Display for PathBuf {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let mut res: fmt::Result = self
			.iter()
			.map(|component| write!(f, "{}/", component))
			.collect();
		if let Some(file_name) = self.file_name() {
			res = res.and_then(|()| write!(f, "{}", file_name));
		}
		res
	}
}
impl fmt::Debug for PathBuf {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		fmt::Debug::fmt(&self.to_string(), f)
	}
}
impl PartialEq<str> for PathBuf {
	fn eq(&self, other: &str) -> bool {
		let mut vec: Vec<u8> = Vec::new();
		for component in self.components.iter() {
			vec.extend(component);
			vec.push(b'/');
		}
		if let Some(file_name) = &self.file_name {
			vec.extend(file_name);
		}
		&*vec == other.as_bytes()
	}
}

#[async_trait(?Send)]
pub trait Directory: File {
	async fn partitions_filter<F>(
		self, f: F,
	) -> Result<Vec<<Self as File>::Partition>, <Self as File>::Error>
	where
		F: FnMut(&PathBuf) -> bool;
}

#[async_trait(?Send)]
pub trait File {
	type Partition: Partition;
	type Error: Error + Clone + PartialEq + 'static;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error>;
}
#[async_trait(?Send)]
pub trait Partition: ProcessSend {
	type Page: Page;
	type Error: Error + Clone + PartialEq + ProcessSend;

	async fn pages(self) -> Result<Vec<Self::Page>, Self::Error>;
}
#[allow(clippy::len_without_is_empty)]
#[async_trait]
pub trait Page {
	type Error: Error + Clone + PartialEq + Into<io::Error> + ProcessSend;

	fn len(&self) -> u64;
	fn set_len(&self, len: u64) -> Result<(), Self::Error>;
	fn read(&self, offset: u64, len: usize) -> BoxFuture<'static, Result<Box<[u8]>, Self::Error>>;
	fn write(&self, offset: u64, buf: Box<[u8]>) -> BoxFuture<'static, Result<(), Self::Error>>;

	fn reader(self) -> Reader<Self>
	where
		Self: Sized,
	{
		Reader::new(self)
	}
}

#[async_trait]
impl<T: ?Sized> Page for &T
where
	T: Page,
{
	type Error = T::Error;

	fn len(&self) -> u64 {
		(**self).len()
	}
	fn set_len(&self, len: u64) -> Result<(), Self::Error> {
		(**self).set_len(len)
	}
	fn read(&self, offset: u64, len: usize) -> BoxFuture<'static, Result<Box<[u8]>, Self::Error>> {
		(**self).read(offset, len)
	}
	fn write(&self, offset: u64, buf: Box<[u8]>) -> BoxFuture<'static, Result<(), Self::Error>> {
		(**self).write(offset, buf)
	}
}
#[async_trait]
impl<T: ?Sized> Page for Arc<T>
where
	T: Page,
{
	type Error = T::Error;

	fn len(&self) -> u64 {
		(**self).len()
	}
	fn set_len(&self, len: u64) -> Result<(), Self::Error> {
		(**self).set_len(len)
	}
	fn read(&self, offset: u64, len: usize) -> BoxFuture<'static, Result<Box<[u8]>, Self::Error>> {
		(**self).read(offset, len)
	}
	fn write(&self, offset: u64, buf: Box<[u8]>) -> BoxFuture<'static, Result<(), Self::Error>> {
		(**self).write(offset, buf)
	}
}

#[pin_project]
pub struct Reader<P>
where
	P: Page,
{
	#[pin]
	page: P,
	#[pin]
	pending: Option<BoxFuture<'static, Result<Box<[u8]>, P::Error>>>,
	pending_len: Option<usize>,
	offset: u64,
}
#[allow(clippy::len_without_is_empty)]
impl<P> Reader<P>
where
	P: Page,
{
	fn new(page: P) -> Self {
		Self {
			page,
			pending: None,
			pending_len: None,
			offset: 0,
		}
	}
	pub fn len(&self) -> u64 {
		self.page.len()
	}
}
impl<P> futures::io::AsyncRead for Reader<P>
where
	P: Page,
{
	fn poll_read(
		self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8],
	) -> Poll<io::Result<usize>> {
		let mut self_ = self.project();
		if self_.pending.is_none() {
			let start = *self_.offset;
			let len = (self_.page.len() - start).min(buf.len() as u64) as usize;
			let len = len.min(PAGE_SIZE);
			let pending = self_.page.read(start, len);
			*self_.pending = Some(pending);
			*self_.pending_len = Some(len);
		}
		let ret = ready!(self_.pending.as_mut().as_pin_mut().unwrap().poll(cx));
		*self_.pending = None;
		let len = self_.pending_len.take().unwrap();
		let ret = ret
			.map(|buf_| {
				buf[..len].copy_from_slice(&buf_);
				len
			})
			.map_err(Into::into);
		*self_.offset += u64::try_from(len).unwrap();
		Poll::Ready(ret)
	}
}

// impl<P> io::Seek for Reader<P>
// where
// 	P: Page,
// {
// 	fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
// 		let len = self.page.len();
// 		self.offset = match pos {
// 			io::SeekFrom::Start(n) => Some(n),
// 			io::SeekFrom::End(n) if n >= 0 => len.checked_add(u64::try_from(n).unwrap()),
// 			io::SeekFrom::End(n) => {
// 				let n = u64::try_from(-(n + 1)).unwrap() + 1;
// 				len.checked_sub(n)
// 			}
// 			io::SeekFrom::Current(n) if n >= 0 => {
// 				self.offset.checked_add(u64::try_from(n).unwrap())
// 			}
// 			io::SeekFrom::Current(n) => {
// 				let n = u64::try_from(-(n + 1)).unwrap() + 1;
// 				self.offset.checked_sub(n)
// 			}
// 		}
// 		.ok_or_else(|| {
// 			io::Error::new(
// 				io::ErrorKind::InvalidInput,
// 				"invalid seek to a negative or overflowing position",
// 			)
// 		})?;
// 		Ok(self.offset)
// 	}
// }
