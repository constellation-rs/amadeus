use futures::{pin_mut, ready, AsyncRead, AsyncReadExt, Stream};
use pin_project::pin_project;
use std::{
	borrow::Cow, future::Future, io::{self, Read}, iter, pin::Pin, task::{Context, Poll}
};
use url::Url;

use amadeus_types::Webpage;

use super::parser;

const BUF: usize = 2 << 26; // 64 MiB
const CHOMP: usize = 2 << 13; // 8 KiB

#[pin_project]
#[derive(Clone, Debug)]
pub struct WarcParser<I> {
	#[pin]
	input: I,
	state: WarcParserState,
	res: Vec<u8>,
	offset: usize,
}
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum WarcParserState {
	Info,
	Request,
	Response,
	Metadata,
	Done,
}
impl<I> WarcParser<I> {
	pub fn new(input: I) -> WarcParser<I> {
		WarcParser {
			input,
			state: WarcParserState::Info,
			res: Vec::with_capacity(BUF),
			offset: 0,
		}
	}
}
impl<I> WarcParser<I>
where
	I: Read,
{
	pub fn next_borrowed(&mut self) -> Result<Option<Webpage<'_>>, io::Error> {
		if let WarcParserState::Done = self.state {
			return Ok(None);
		}
		'chomp: loop {
			assert!(
				self.res.len() < BUF,
				"Individual record > configured BUF {:?}",
				BUF
			);
			let n = io::copy(
				&mut self
					.input
					.by_ref()
					.take(CHOMP.min(BUF - self.res.len()) as u64),
				&mut self.res,
			)?;
			assert_eq!(self.res.capacity(), BUF);
			if n == 0 && self.offset == self.res.len() {
				assert_eq!(self.state, WarcParserState::Request);
				self.state = WarcParserState::Done;
				return Ok(None);
			}

			loop {
				self.res.splice(..self.offset, iter::empty());
				self.offset = 0;
				if self.offset == self.res.len() {
					continue 'chomp;
				}
				let record = match parser::record(&self.res[self.offset..]) {
					Ok((rem, record)) => {
						let record_len = self.res.len() - self.offset - rem.len() + 4; // 4 is \r\n\r\n
						if self.offset + record_len > self.res.len() {
							continue 'chomp;
						}
						self.offset += record_len;
						record
					}
					Err(nom::Err::Incomplete(_)) => {
						continue 'chomp;
					}
					_ => panic!(),
				};
				self.state = match self.state {
					WarcParserState::Info => {
						assert!(record.type_ == parser::RecordType::WARCInfo);
						WarcParserState::Request
					}
					WarcParserState::Request => {
						assert!(record.type_ == parser::RecordType::Request);
						WarcParserState::Response
					}
					WarcParserState::Response => {
						assert!(record.type_ == parser::RecordType::Response);
						self.state = WarcParserState::Metadata;

						let content: *const u8 = record.content.as_ptr();
						let buffer: *const u8 = self.res.as_slice().as_ptr();
						let start = (content as usize) - (buffer as usize);
						let end = start + record.content.len();
						return Ok(Some(Webpage {
							ip: record.ip_address.unwrap().parse().unwrap(),
							url: Url::parse(record.target_uri.unwrap()).unwrap(),
							contents: Cow::Borrowed(&self.res[start..end]),
						}));
					}
					WarcParserState::Metadata => {
						assert!(record.type_ == parser::RecordType::Metadata);
						WarcParserState::Request
					}
					WarcParserState::Done => unreachable!(),
				}
			}
		}
	}
}
impl<I> WarcParser<I>
where
	I: AsyncRead,
{
	pub fn poll_next_borrowed(
		self: Pin<&mut Self>, cx: &mut Context,
	) -> Poll<Result<Option<Webpage<'_>>, io::Error>> {
		let mut self_ = self.project();
		if let WarcParserState::Done = self_.state {
			return Poll::Ready(Ok(None));
		}
		'chomp: loop {
			assert!(
				self_.res.len() < BUF,
				"Individual record > configured BUF {:?}",
				BUF
			);
			let from = (&mut self_.input).take(CHOMP.min(BUF - self_.res.len()) as u64);
			let copy = futures::io::copy(from, self_.res);
			pin_mut!(copy);
			let n = ready!(copy.poll(cx))?;
			assert_eq!(self_.res.capacity(), BUF);
			if n == 0 && *self_.offset == self_.res.len() {
				assert_eq!(*self_.state, WarcParserState::Request);
				*self_.state = WarcParserState::Done;
				return Poll::Ready(Ok(None));
			}

			loop {
				self_.res.splice(..*self_.offset, iter::empty());
				*self_.offset = 0;
				if *self_.offset == self_.res.len() {
					continue 'chomp;
				}
				let record = match parser::record(&self_.res[*self_.offset..]) {
					Ok((rem, record)) => {
						let record_len = self_.res.len() - *self_.offset - rem.len() + 4; // 4 is \r\n\r\n
						if *self_.offset + record_len > self_.res.len() {
							continue 'chomp;
						}
						*self_.offset += record_len;
						record
					}
					Err(nom::Err::Incomplete(_)) => {
						continue 'chomp;
					}
					_ => panic!(),
				};
				*self_.state = match *self_.state {
					WarcParserState::Info => {
						assert!(record.type_ == parser::RecordType::WARCInfo);
						WarcParserState::Request
					}
					WarcParserState::Request => {
						assert!(record.type_ == parser::RecordType::Request);
						WarcParserState::Response
					}
					WarcParserState::Response => {
						assert!(record.type_ == parser::RecordType::Response);
						*self_.state = WarcParserState::Metadata;

						let content: *const u8 = record.content.as_ptr();
						let buffer: *const u8 = self_.res.as_slice().as_ptr();
						let start = (content as usize) - (buffer as usize);
						let end = start + record.content.len();
						return Poll::Ready(Ok(Some(Webpage {
							ip: record.ip_address.unwrap().parse().unwrap(),
							url: Url::parse(record.target_uri.unwrap()).unwrap(),
							contents: Cow::Borrowed(&self_.res[start..end]),
						})));
					}
					WarcParserState::Metadata => {
						assert!(record.type_ == parser::RecordType::Metadata);
						WarcParserState::Request
					}
					WarcParserState::Done => unreachable!(),
				}
			}
		}
	}
}
impl<I> Iterator for WarcParser<I>
where
	I: Read,
{
	type Item = Result<Webpage<'static>, io::Error>;
	fn next(&mut self) -> Option<Self::Item> {
		self.next_borrowed()
			.transpose()
			.map(|x| x.map(|x| x.to_owned()))
	}
}
impl<I> Stream for WarcParser<I>
where
	I: AsyncRead,
{
	type Item = Result<Webpage<'static>, io::Error>;
	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		Poll::Ready(
			ready!(self.poll_next_borrowed(cx))
				.transpose()
				.map(|x| x.map(|x| x.to_owned())),
		)
	}
}
