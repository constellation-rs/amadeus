mod local;

use futures::future::FutureExt;
use std::{
	convert::{TryFrom, TryInto}, error::Error, future::Future, io, pin::Pin, task::{Context, Poll}
};

use crate::pool::ProcessSend;

pub use local::LocalFile;

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

	fn len(&self) -> u64;
	fn set_len(&self, len: u64) -> Result<(), Self::Error>;
	fn read<'a>(
		&'a self, offset: u64, buf: &'a mut [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>>;
	fn write<'a>(
		&'a self, offset: u64, buf: &'a [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>>;

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

	fn len(&self) -> u64 {
		(**self).len()
	}
	fn set_len(&self, len: u64) -> Result<(), Self::Error> {
		(**self).set_len(len)
	}
	fn read<'a>(
		&'a self, offset: u64, buf: &'a mut [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>> {
		(**self).read(offset, buf)
	}
	fn write<'a>(
		&'a self, offset: u64, buf: &'a [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>> {
		(**self).write(offset, buf)
	}
}

pub struct Reader<P>
where
	P: Page,
{
	page: P,
	offset: u64,
}
impl<P> Reader<P>
where
	P: Page,
{
	fn new(page: P) -> Self {
		Self { page, offset: 0 }
	}
}
impl<P> Unpin for Reader<P> where P: Page {}
impl<P> futures::io::AsyncRead for Reader<P>
where
	P: Page,
{
	fn poll_read(
		mut self: Pin<&mut Self>, cx: &mut Context, mut buf: &mut [u8],
	) -> Poll<io::Result<usize>> {
		let rem = self.page.len().saturating_sub(self.offset);
		let rem: usize = rem.try_into().unwrap();
		let rem = rem.min(buf.len());
		buf = &mut buf[..rem];
		let offset = self.offset;
		self.offset += u64::try_from(rem).unwrap();
		unsafe { self.map_unchecked_mut(|s| &mut s.page) }
			.read(offset, buf)
			.poll_unpin(cx)
			.map(|x| x.map(|()| rem))
			.map_err(Into::into)
	}
}
impl<P> io::Read for Reader<P>
where
	P: Page,
{
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		let future = futures::io::AsyncReadExt::read(self, buf);
		futures::executor::block_on(future)
	}
}
