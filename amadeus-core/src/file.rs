mod local;

use futures::future::FutureExt;
use std::{
	convert::{TryFrom, TryInto}, error::Error, fmt, future::Future, io, pin::Pin, sync::Arc, task::{Context, Poll}
};

use crate::pool::ProcessSend;

pub use local::LocalFile;

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

pub trait Directory: File {
	fn partitions_filter<F>(self, f: F) -> Result<Vec<Self::Partition>, Self::Error>
	where
		F: FnMut(&PathBuf) -> bool;
}

pub trait File {
	type Partition: Partition;
	type Error: Error + Clone + PartialEq + 'static;

	fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error>;
}
pub trait Partition: ProcessSend {
	type Page: Page;
	type Error: Error + Clone + PartialEq + ProcessSend;

	fn pages(self) -> Result<Vec<Self::Page>, Self::Error>;
}
#[allow(clippy::len_without_is_empty)]
pub trait Page {
	type Error: Error + Clone + PartialEq + Into<io::Error> + ProcessSend;

	fn block_on<F>(future: F) -> F::Output
	where
		F: Future + Send,
		F::Output: Send,
	{
		futures::executor::block_on(future)
	}
	fn len(&self) -> u64;
	fn set_len(&self, len: u64) -> Result<(), Self::Error>;
	fn read<'a>(
		&'a self, offset: u64, buf: &'a mut [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>>;
	fn write<'a>(
		&'a self, offset: u64, buf: &'a [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>>;

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

	fn block_on<F>(future: F) -> F::Output
	where
		F: Future + Send,
		F::Output: Send,
	{
		T::block_on(future)
	}
	fn len(&self) -> u64 {
		(**self).len()
	}
	fn set_len(&self, len: u64) -> Result<(), Self::Error> {
		(**self).set_len(len)
	}
	fn read<'a>(
		&'a self, offset: u64, buf: &'a mut [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
		(**self).read(offset, buf)
	}
	fn write<'a>(
		&'a self, offset: u64, buf: &'a [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
		(**self).write(offset, buf)
	}
}

impl<T: ?Sized> Page for Arc<T>
where
	T: Page,
{
	type Error = T::Error;

	fn block_on<F>(future: F) -> F::Output
	where
		F: Future + Send,
		F::Output: Send,
	{
		T::block_on(future)
	}
	fn len(&self) -> u64 {
		(**self).len()
	}
	fn set_len(&self, len: u64) -> Result<(), Self::Error> {
		(**self).set_len(len)
	}
	fn read<'a>(
		&'a self, offset: u64, buf: &'a mut [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
		(**self).read(offset, buf)
	}
	fn write<'a>(
		&'a self, offset: u64, buf: &'a [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
		(**self).write(offset, buf)
	}
}

#[derive(Clone)]
pub struct Reader<P>
where
	P: Page,
{
	page: P,
	offset: u64,
}
#[allow(clippy::len_without_is_empty)]
impl<P> Reader<P>
where
	P: Page,
{
	fn new(page: P) -> Self {
		Self { page, offset: 0 }
	}
	pub fn len(&self) -> u64 {
		self.page.len()
	}
}
impl<P> Unpin for Reader<P> where P: Page {}
impl<P> futures::io::AsyncRead for Reader<P>
where
	P: Page,
{
	fn poll_read(
		self: Pin<&mut Self>, _cx: &mut Context, _buf: &mut [u8],
	) -> Poll<io::Result<usize>> {
		unimplemented!();
		// println!("Reader::poll_read");
		// let rem = self.page.len().saturating_sub(self.offset);
		// let rem: usize = rem.try_into().unwrap();
		// let rem = rem.min(buf.len());
		// buf = &mut buf[..rem];
		// let offset = self.offset;
		// // let self_offset = &mut self.offset;
		// let ret = unsafe { self.map_unchecked_mut(|s| &mut s.page) }
		// 	.read(offset, buf)
		// 	.poll_unpin(cx)
		// 	.map(|x| x.map(|()| {
		// 		println!("/Reader::poll_read");
		// 		// *self_offset += u64::try_from(rem).unwrap();
		// 		rem
		// 	}).map_err(Into::into))
		// 	;
		// println!("{:?}", ret);
		// ret
	}
}
impl<P> io::Read for Reader<P>
where
	P: Page,
{
	fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
		// let future = futures::io::AsyncReadExt::read(self, buf);
		let rem = self.page.len().saturating_sub(self.offset);
		let rem: usize = rem.try_into().unwrap();
		let rem = rem.min(buf.len());
		buf = &mut buf[..rem];
		let offset = self.offset;
		self.offset += u64::try_from(rem).unwrap();
		let future = self
			.page
			.read(offset, buf)
			.map(|x| x.map(|()| rem).map_err(Into::into));
		P::block_on(future)
	}
	unsafe fn initializer(&self) -> io::Initializer {
		io::Initializer::nop()
	}
}
impl<P> io::Seek for Reader<P>
where
	P: Page,
{
	fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
		let len = self.page.len();
		self.offset = match pos {
			io::SeekFrom::Start(n) => Some(n),
			io::SeekFrom::End(n) if n >= 0 => len.checked_add(u64::try_from(n).unwrap()),
			io::SeekFrom::End(n) => {
				let n = u64::try_from(-(n + 1)).unwrap() + 1;
				len.checked_sub(n)
			}
			io::SeekFrom::Current(n) if n >= 0 => {
				self.offset.checked_add(u64::try_from(n).unwrap())
			}
			io::SeekFrom::Current(n) => {
				let n = u64::try_from(-(n + 1)).unwrap() + 1;
				self.offset.checked_sub(n)
			}
		}
		.ok_or_else(|| {
			io::Error::new(
				io::ErrorKind::InvalidInput,
				"invalid seek to a negative or overflowing position",
			)
		})?;
		Ok(self.offset)
	}
}
