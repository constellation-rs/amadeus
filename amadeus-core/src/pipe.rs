mod filter;
mod flat_map;
mod flatten;
mod map;

use derive_new::new;
use futures::{pin_mut, stream, Future, Stream};
use pin_project::pin_project;
use std::{
	marker::PhantomData, mem, ops::DerefMut, pin::Pin, task::{Context, Poll}
};

pub use self::{filter::*, flat_map::*, flatten::*, map::*};

// Sink takes Input as an input parameter rather than associated type to accept
// for<'a> &'a T, but this might not be necessary in future?
// https://github.com/rust-lang/rust/issues/49601

pub trait StreamExt: Stream {
	#[inline(always)]
	fn pipe<P>(self, pipe: P) -> StreamPipe<Self, P>
	where
		P: Pipe<Self::Item>,
		Self: Sized,
	{
		assert_stream(StreamPipe { stream: self, pipe })
	}

	#[inline(always)]
	fn sink<S>(self, sink: S) -> StreamSink<Self, S>
	where
		S: Sink<Self::Item>,
		Self: Sized,
	{
		assert_future(StreamSink { stream: self, sink })
	}
}
impl<S: ?Sized> StreamExt for S where S: Stream {}

pub trait Pipe<Input> {
	type Output;

	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Input>>,
	) -> Poll<Option<Self::Output>>;

	#[inline(always)]
	fn pipe<P>(self, pipe: P) -> PipePipe<Self, P>
	where
		P: Pipe<Self::Output>,
		Self: Sized,
	{
		assert_pipe(PipePipe { a: self, b: pipe })
	}

	#[inline(always)]
	fn sink<S>(self, sink: S) -> PipeSink<Self, S>
	where
		S: Sink<Self::Output>,
		Self: Sized,
	{
		assert_sink(PipeSink { pipe: self, sink })
	}

	#[inline(always)]
	fn filter<F>(self, f: F) -> Filter<Self, F>
	where
		F: FnMut(&Self::Output) -> bool,
		Self: Sized,
	{
		assert_pipe(Filter::new(self, f))
	}

	#[inline(always)]
	fn flat_map<F, R>(self, f: F) -> FlatMap<Self, F, R>
	where
		F: FnMut(Self::Output) -> R,
		R: Stream,
		Self: Sized,
	{
		assert_pipe(FlatMap::new(self, f))
	}

	#[inline(always)]
	fn flatten(self) -> Flatten<Self, Self::Output>
	where
		Self::Output: Stream,
		Self: Sized,
	{
		assert_pipe(Flatten::new(self))
	}

	#[inline(always)]
	fn map<F, R>(self, f: F) -> Map<Self, F>
	where
		F: FnMut(Self::Output) -> R,
		Self: Sized,
	{
		assert_pipe(Map::new(self, f))
	}
}

pub trait Sink<Item> {
	type Done;

	/// Returns `Poll::Ready` when a) it can't accept any more elements from `stream` and b) all
	/// accepted elements have been fully processed. By convention, `stream` yielding `None`
	/// typically triggers (a).
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<Self::Done>;

	#[inline(always)]
	fn send(&mut self, item: Item) -> Send<'_, Self, Item>
	where
		Self: Unpin,
	{
		assert_future(Send::new(self, Poll::Ready(item)))
	}

	#[inline(always)]
	fn send_all<'a, S: ?Sized>(&'a mut self, items: &'a mut S) -> SendAll<'a, Self, S>
	where
		S: Stream<Item = Item> + Unpin,
		Self: Unpin,
	{
		assert_future(SendAll::new(self, items))
	}

	#[inline(always)]
	fn done(&mut self) -> Done<'_, Self, Item>
	where
		Self: Unpin,
	{
		assert_future(Done::new(self))
	}
}

#[inline(always)]
fn assert_stream<S>(s: S) -> S
where
	S: Stream,
{
	s
}
#[inline(always)]
fn assert_pipe<P, Input>(p: P) -> P
where
	P: Pipe<Input>,
{
	p
}
#[inline(always)]
fn assert_sink<S, Input>(s: S) -> S
where
	S: Sink<Input>,
{
	s
}
#[inline(always)]
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
	type Item = P::Output;

	#[inline(always)]
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

impl<A, B, Input> Pipe<Input> for PipePipe<A, B>
where
	A: Pipe<Input>,
	B: Pipe<A::Output>,
{
	type Output = B::Output;

	#[inline(always)]
	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Input>>,
	) -> Poll<Option<Self::Output>> {
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

impl<P, S, Input> Sink<Input> for PipeSink<P, S>
where
	P: Pipe<Input>,
	S: Sink<P::Output>,
{
	type Done = S::Done;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Input>>,
	) -> Poll<Self::Done> {
		let self_ = self.project();
		let stream = stream.pipe(self_.pipe);
		pin_mut!(stream);
		self_.sink.poll_forward(cx, stream)
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
	type Output = B::Done;

	#[inline(always)]
	fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		let self_ = self.project();
		self_.sink.poll_forward(cx, self_.stream)
	}
}

#[pin_project]
#[derive(new)]
pub struct Send<'a, S: ?Sized, Item> {
	sink: &'a mut S,
	item: Poll<Item>,
}
impl<S: ?Sized + Sink<Item> + Unpin, Item> Future for Send<'_, S, Item> {
	type Output = Option<S::Done>;

	#[inline(always)]
	fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		let self_ = self.project();
		let item = self_.item;
		let stream = stream::poll_fn(|_| mem::replace(item, Poll::Pending).map(Some));
		pin_mut!(stream);
		if let Poll::Ready(done) = Pin::new(self_.sink).poll_forward(cx, stream) {
			return Poll::Ready(Some(done));
		}
		if item.is_pending() {
			Poll::Ready(None)
		} else {
			Poll::Pending
		}
	}
}

#[pin_project]
#[derive(new)]
pub struct SendAll<'a, S: ?Sized, St: ?Sized> {
	sink: &'a mut S,
	items: &'a mut St,
}
impl<S: ?Sized + Sink<Item> + Unpin, St: ?Sized + Stream<Item = Item> + Unpin, Item> Future
	for SendAll<'_, S, St>
{
	type Output = Option<S::Done>;

	#[inline(always)]
	fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		let self_ = self.project();
		let items = &mut **self_.items;
		let mut given_all = false;
		let stream = stream::poll_fn(|cx| match Pin::new(&mut *items).poll_next(cx) {
			x @ Poll::Ready(Some(_)) | x @ Poll::Pending => x,
			Poll::Ready(None) => {
				given_all = true;
				Poll::Pending
			}
		});
		pin_mut!(stream);
		if let Poll::Ready(done) = Pin::new(self_.sink).poll_forward(cx, stream) {
			return Poll::Ready(Some(done));
		}
		if given_all {
			Poll::Ready(None)
		} else {
			Poll::Pending
		}
	}
}

#[pin_project]
#[derive(new)]
pub struct Done<'a, S: ?Sized, Item: ?Sized> {
	sink: &'a mut S,
	marker: PhantomData<fn() -> Item>,
}
impl<S: ?Sized + Sink<Item> + Unpin, Item> Future for Done<'_, S, Item> {
	type Output = S::Done;

	#[inline(always)]
	fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		let stream = stream::empty();
		pin_mut!(stream);
		Pin::new(&mut self.sink).poll_forward(cx, stream)
	}
}

impl<P, Input> Pipe<Input> for Pin<P>
where
	P: DerefMut + Unpin,
	P::Target: Pipe<Input>,
{
	type Output = <P::Target as Pipe<Input>>::Output;

	#[inline(always)]
	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Input>>,
	) -> Poll<Option<Self::Output>> {
		self.get_mut().as_mut().poll_next(cx, stream)
	}
}

impl<T: ?Sized, Input> Pipe<Input> for &mut T
where
	T: Pipe<Input> + Unpin,
{
	type Output = T::Output;

	#[inline(always)]
	fn poll_next(
		mut self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Input>>,
	) -> Poll<Option<Self::Output>> {
		Pin::new(&mut **self).poll_next(cx, stream)
	}
}

impl<P, Input> Sink<Input> for Pin<P>
where
	P: DerefMut + Unpin,
	P::Target: Sink<Input>,
{
	type Done = <P::Target as Sink<Input>>::Done;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Input>>,
	) -> Poll<Self::Done> {
		self.get_mut().as_mut().poll_forward(cx, stream)
	}
}

impl<T: ?Sized, Input> Sink<Input> for &mut T
where
	T: Sink<Input> + Unpin,
{
	type Done = T::Done;

	#[inline(always)]
	fn poll_forward(
		mut self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Input>>,
	) -> Poll<Self::Done> {
		Pin::new(&mut **self).poll_forward(cx, stream)
	}
}
