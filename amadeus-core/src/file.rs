// TODO: Use WTF-8 rather than UTF-16

#![allow(clippy::type_complexity)]

mod local;

use async_trait::async_trait;
use futures::{future::LocalBoxFuture, ready};
use pin_project::pin_project;
use std::{
	convert::TryFrom, error::Error, ffi, fmt, future::Future, io, pin::Pin, sync::Arc, task::{Context, Poll}
};
use widestring::U16String;

use crate::pool::ProcessSend;

pub use local::LocalFile;

const PAGE_SIZE: usize = 10 * 1024 * 1024; // `Reader` reads this many bytes at a time

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct OsString {
	buf: U16String,
}
impl OsString {
	pub fn new() -> Self {
		Self {
			buf: U16String::new(),
		}
	}
	pub fn to_string_lossy(&self) -> String {
		self.buf.to_string_lossy()
	}
	pub fn display<'a>(&'a self) -> impl fmt::Display + 'a {
		struct Display<'a>(&'a OsString);
		impl<'a> fmt::Display for Display<'a> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				self.0.to_string_lossy().fmt(f)
			}
		}
		Display(self)
	}
}
impl From<Vec<u8>> for OsString {
	fn from(from: Vec<u8>) -> Self {
		Self {
			buf: String::from_utf8(from)
				.expect("Not yet imlemented: Handling non-UTF-8")
				.into(),
		} // TODO
	}
}
impl From<String> for OsString {
	fn from(from: String) -> Self {
		Self { buf: from.into() }
	}
}
impl From<&str> for OsString {
	fn from(from: &str) -> Self {
		Self {
			buf: U16String::from_str(from),
		}
	}
}
impl From<ffi::OsString> for OsString {
	fn from(from: ffi::OsString) -> Self {
		Self {
			buf: U16String::from_os_str(&from),
		}
	}
}
impl From<&ffi::OsStr> for OsString {
	fn from(from: &ffi::OsStr) -> Self {
		Self {
			buf: U16String::from_os_str(from),
		}
	}
}
pub struct InvalidOsString;
impl TryFrom<OsString> for ffi::OsString {
	type Error = InvalidOsString;

	fn try_from(from: OsString) -> Result<Self, Self::Error> {
		Ok(from.buf.to_os_string()) // TODO: this is lossy but it should error
	}
}
impl PartialEq<Vec<u8>> for OsString {
	fn eq(&self, other: &Vec<u8>) -> bool {
		self == &OsString::from(other.clone())
	}
}
impl PartialEq<String> for OsString {
	fn eq(&self, other: &String) -> bool {
		self == &OsString::from(other.clone())
	}
}
impl PartialEq<str> for OsString {
	fn eq(&self, other: &str) -> bool {
		self == &OsString::from(other)
	}
}
impl PartialEq<ffi::OsString> for OsString {
	fn eq(&self, other: &ffi::OsString) -> bool {
		self == &OsString::from(other.clone())
	}
}
impl PartialEq<ffi::OsStr> for OsString {
	fn eq(&self, other: &ffi::OsStr) -> bool {
		self == &OsString::from(other)
	}
}
impl fmt::Debug for OsString {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.display())
	}
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct PathBuf {
	components: Vec<OsString>,
	file_name: Option<OsString>,
}
impl PathBuf {
	pub fn new() -> Self {
		Self {
			components: Vec::new(),
			file_name: None,
		}
	}
	pub fn push<S>(&mut self, component: S)
	where
		S: Into<OsString>,
	{
		assert!(self.file_name.is_none());
		self.components.push(component.into());
	}
	pub fn pop(&mut self) -> Option<OsString> {
		assert!(self.file_name.is_none());
		self.components.pop()
	}
	pub fn last(&self) -> Option<&OsString> {
		assert!(self.file_name.is_none());
		self.components.last()
	}
	pub fn set_file_name<S>(&mut self, file_name: Option<S>)
	where
		S: Into<OsString>,
	{
		self.file_name = file_name.map(Into::into);
	}
	pub fn is_file(&self) -> bool {
		self.file_name.is_some()
	}
	pub fn file_name(&self) -> Option<&OsString> {
		self.file_name.as_ref()
	}
	pub fn depth(&self) -> usize {
		self.components.len()
	}
	pub fn iter<'a>(&'a self) -> impl Iterator<Item = &OsString> + 'a {
		self.components.iter()
	}
	pub fn display<'a>(&'a self) -> impl fmt::Display + 'a {
		struct Display<'a>(&'a PathBuf);
		impl<'a> fmt::Display for Display<'a> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				let mut res: fmt::Result = self
					.0
					.iter()
					.map(|component| write!(f, "{}/", component.to_string_lossy()))
					.collect();
				if let Some(file_name) = self.0.file_name() {
					res = res.and_then(|()| write!(f, "{}", file_name.to_string_lossy()));
				}
				res
			}
		}
		Display(self)
	}
}
impl fmt::Debug for PathBuf {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.display())
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
pub trait Partition: Clone + fmt::Debug + ProcessSend {
	type Page: Page;
	type Error: Error + Clone + PartialEq + ProcessSend;

	async fn pages(self) -> Result<Vec<Self::Page>, Self::Error>;
}
#[allow(clippy::len_without_is_empty)]
pub trait Page {
	type Error: Error + Clone + PartialEq + Into<io::Error> + ProcessSend;

	fn len(&self) -> LocalBoxFuture<'static, Result<u64, Self::Error>>;
	fn read(
		&self, offset: u64, len: usize,
	) -> LocalBoxFuture<'static, Result<Box<[u8]>, Self::Error>>;
	fn write(
		&self, offset: u64, buf: Box<[u8]>,
	) -> LocalBoxFuture<'static, Result<(), Self::Error>>;

	fn reader(self) -> Reader<Self>
	where
		Self: Sized,
	{
		Reader::new(self)
	}
}

impl<T: ?Sized> Page for &T
where
	T: Page,
{
	type Error = T::Error;

	fn len(&self) -> LocalBoxFuture<'static, Result<u64, Self::Error>> {
		(**self).len()
	}
	fn read(
		&self, offset: u64, len: usize,
	) -> LocalBoxFuture<'static, Result<Box<[u8]>, Self::Error>> {
		(**self).read(offset, len)
	}
	fn write(
		&self, offset: u64, buf: Box<[u8]>,
	) -> LocalBoxFuture<'static, Result<(), Self::Error>> {
		(**self).write(offset, buf)
	}
}
impl<T: ?Sized> Page for Arc<T>
where
	T: Page,
{
	type Error = T::Error;

	fn len(&self) -> LocalBoxFuture<'static, Result<u64, Self::Error>> {
		(**self).len()
	}
	fn read(
		&self, offset: u64, len: usize,
	) -> LocalBoxFuture<'static, Result<Box<[u8]>, Self::Error>> {
		(**self).read(offset, len)
	}
	fn write(
		&self, offset: u64, buf: Box<[u8]>,
	) -> LocalBoxFuture<'static, Result<(), Self::Error>> {
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
	pending: Option<LocalBoxFuture<'static, Result<Box<[u8]>, P::Error>>>,
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
			offset: 0,
		}
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
			let len = buf.len();
			let len = len.min(PAGE_SIZE);
			let pending = self_.page.read(start, len);
			*self_.pending = Some(pending);
		}
		let ret = ready!(self_.pending.as_mut().as_pin_mut().unwrap().poll(cx));
		*self_.pending = None;
		let ret = ret
			.map(|buf_| {
				buf[..buf_.len()].copy_from_slice(&buf_);
				buf_.len()
			})
			.map_err(Into::into);
		*self_.offset += u64::try_from(ret.as_ref().ok().cloned().unwrap_or(0)).unwrap();
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
