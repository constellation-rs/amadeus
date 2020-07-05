mod filter;
mod flat_map;

use futures::{pin_mut, Future, Stream};
use pin_project::pin_project;
use std::{
	ops::DerefMut, pin::Pin, task::{Context, Poll}
};

pub use self::{filter::*, flat_map::*};

// Sink takes Source as an input parameter rather than associated type to accept
// for<'a> &'a T, but this might not be necessary in future?
// https://github.com/rust-lang/rust/issues/49601

pub trait StreamExt: Stream {
	fn pipe<P>(self, pipe: P) -> StreamPipe<Self, P>
	where
		P: Pipe<Self::Item>,
		Self: Sized,
	{
		assert_stream(StreamPipe { stream: self, pipe })
	}

	fn sink<S>(self, sink: S) -> StreamSink<Self, S>
	where
		S: Sink<Self::Item>,
		Self: Sized,
	{
		assert_future(StreamSink { stream: self, sink })
	}

	fn flat_map<F, R>(self, f: F) -> FlatMap<Self, F, R>
	where
		F: FnMut(Self::Item) -> R,
		R: Stream,
		Self: Sized,
	{
		assert_stream(FlatMap::new(self, f))
	}

	fn filter<F>(self, f: F) -> Filter<Self, F>
	where
		F: FnMut(&Self::Item) -> bool,
		Self: Sized,
	{
		assert_stream(Filter::new(self, f))
	}
}
impl<S: ?Sized> StreamExt for S where S: Stream {}

pub trait Pipe<Source> {
	type Item;

	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
	) -> Poll<Option<Self::Item>>;

	fn pipe<P>(self, pipe: P) -> PipePipe<Self, P>
	where
		P: Pipe<Self::Item>,
		Self: Sized,
	{
		assert_pipe(PipePipe { a: self, b: pipe })
	}

	fn sink<S>(self, sink: S) -> PipeSink<Self, S>
	where
		S: Sink<Self::Item>,
		Self: Sized,
	{
		assert_sink(PipeSink { pipe: self, sink })
	}

	fn flat_map<F, R>(self, f: F) -> FlatMap<Self, F, R>
	where
		F: FnMut(Self::Item) -> R,
		R: Stream,
		Self: Sized,
	{
		assert_pipe(FlatMap::new(self, f))
	}

	fn filter<F>(self, f: F) -> Filter<Self, F>
	where
		F: FnMut(&Self::Item) -> bool,
		Self: Sized,
	{
		assert_pipe(Filter::new(self, f))
	}
}

pub trait Sink<Source> {
	fn poll_pipe(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
	) -> Poll<()>;
}

fn assert_stream<S>(s: S) -> S
where
	S: Stream,
{
	s
}
fn assert_pipe<P, Source>(p: P) -> P
where
	P: Pipe<Source>,
{
	p
}
fn assert_sink<S, Source>(s: S) -> S
where
	S: Sink<Source>,
{
	s
}
fn assert_future<F>(f: F) -> F
where
	F: Future,
{
	f
}

#[pin_project]
pub struct StreamPipe<S, P> {
	#[pin]
	stream: S,
	#[pin]
	pipe: P,
}

impl<S, P> Stream for StreamPipe<S, P>
where
	S: Stream,
	P: Pipe<S::Item>,
{
	type Item = P::Item;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		let self_ = self.project();
		self_.pipe.poll_next(cx, self_.stream)
	}
}

#[pin_project]
pub struct PipePipe<A, B> {
	#[pin]
	a: A,
	#[pin]
	b: B,
}

impl<A, B, Source> Pipe<Source> for PipePipe<A, B>
where
	A: Pipe<Source>,
	B: Pipe<A::Item>,
{
	type Item = B::Item;

	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
	) -> Poll<Option<Self::Item>> {
		let self_ = self.project();
		let stream = stream.pipe(self_.a);
		pin_mut!(stream);
		self_.b.poll_next(cx, stream)
	}
}

#[pin_project]
pub struct PipeSink<P, S> {
	#[pin]
	pipe: P,
	#[pin]
	sink: S,
}

impl<P, S, Source> Sink<Source> for PipeSink<P, S>
where
	P: Pipe<Source>,
	S: Sink<P::Item>,
{
	fn poll_pipe(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
	) -> Poll<()> {
		let self_ = self.project();
		let stream = stream.pipe(self_.pipe);
		pin_mut!(stream);
		self_.sink.poll_pipe(cx, stream)
	}
}

#[pin_project]
pub struct StreamSink<A, B> {
	#[pin]
	stream: A,
	#[pin]
	sink: B,
}

impl<A, B> Future for StreamSink<A, B>
where
	A: Stream,
	B: Sink<A::Item>,
{
	type Output = ();

	fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
		let self_ = self.project();
		self_.sink.poll_pipe(cx, self_.stream)
	}
}

impl<P, Source> Pipe<Source> for Pin<P>
where
	P: DerefMut + Unpin,
	P::Target: Pipe<Source>,
{
	type Item = <P::Target as Pipe<Source>>::Item;

	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
	) -> Poll<Option<Self::Item>> {
		self.get_mut().as_mut().poll_next(cx, stream)
	}
}

impl<T: ?Sized, Source> Pipe<Source> for &mut T
where
	T: Pipe<Source> + Unpin,
{
	type Item = T::Item;

	fn poll_next(
		mut self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
	) -> Poll<Option<Self::Item>> {
		Pin::new(&mut **self).poll_next(cx, stream)
	}
}

impl<P, Source> Sink<Source> for Pin<P>
where
	P: DerefMut + Unpin,
	P::Target: Sink<Source>,
{
	fn poll_pipe(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
	) -> Poll<()> {
		self.get_mut().as_mut().poll_pipe(cx, stream)
	}
}

impl<T: ?Sized, Source> Sink<Source> for &mut T
where
	T: Sink<Source> + Unpin,
{
	fn poll_pipe(
		mut self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
	) -> Poll<()> {
		Pin::new(&mut **self).poll_pipe(cx, stream)
	}
}
