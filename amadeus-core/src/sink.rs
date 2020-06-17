#![allow(clippy::type_complexity)]

use derive_new::new;
use futures::{pin_mut, ready, stream, Future, Stream, StreamExt};
use pin_project::pin_project;
use std::{
	marker::PhantomData, ops::DerefMut, pin::Pin, task::{Context, Poll}
};

// Sink could take Item as an input parameter to accept for<'a> &'a T, but this might be fixed anyway? https://github.com/rust-lang/rust/issues/49601

pub trait Sink {
	type Item;

	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()>;
}

impl<P> Sink for Pin<P>
where
	P: DerefMut + Unpin,
	P::Target: Sink,
{
	type Item = <P::Target as Sink>::Item;

	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		self.get_mut().as_mut().poll_forward(cx, stream)
	}
}

impl<T: ?Sized> Sink for &mut T
where
	T: Sink + Unpin,
{
	type Item = T::Item;

	fn poll_forward(
		mut self: Pin<&mut Self>, cx: &mut Context,
		stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		Pin::new(&mut **self).poll_forward(cx, stream)
	}
}

#[pin_project]
#[derive(new)]
pub struct SinkMap<F, I, R, Item> {
	#[pin]
	i: I,
	f: F,
	marker: PhantomData<fn() -> (R, Item)>,
}
impl<F, I, R, Item> Sink for SinkMap<F, I, R, Item>
where
	F: FnMut(Item) -> R,
	I: Sink<Item = R>,
{
	type Item = Item;

	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		let (f, i) = (self_.f, self_.i);
		let stream = stream.map(f);
		pin_mut!(stream);
		i.poll_forward(cx, stream)
	}
}

#[pin_project(project = SinkFilterStateProj, project_replace = SinkFilterStateProjOwn)]
pub enum SinkFilterState<T, Fut> {
	Some(T, #[pin] Fut),
	None,
}
impl<T, Fut> SinkFilterState<T, Fut> {
	fn fut(self: Pin<&mut Self>) -> Option<Pin<&mut Fut>> {
		match self.project() {
			SinkFilterStateProj::Some(_, fut) => Some(fut),
			SinkFilterStateProj::None => None,
		}
	}
	fn take(self: Pin<&mut Self>) -> Option<T> {
		match self.project_replace(SinkFilterState::None) {
			SinkFilterStateProjOwn::Some(t, _) => Some(t),
			SinkFilterStateProjOwn::None => None,
		}
	}
}
#[pin_project]
#[derive(new)]
pub struct SinkFilter<'a, F, I, T, Fut> {
	fut: Pin<&'a mut SinkFilterState<T, Fut>>,
	#[pin]
	i: I,
	f: F,
}
impl<'a, F, I, Fut, Item> Sink for SinkFilter<'a, F, I, Item, Fut>
where
	F: FnMut(&Item) -> Fut,
	Fut: Future<Output = bool>,
	I: Sink<Item = Item>,
{
	type Item = Item;

	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		let f = self_.f;
		let fut = self_.fut;
		let stream = stream::poll_fn(move |cx| loop {
			if fut.as_mut().fut().is_none() {
				let item = ready!(stream.as_mut().poll_next(cx));
				if item.is_none() {
					break Poll::Ready(None);
				}
				let item = item.unwrap();
				let fut_ = f(&item);
				fut.set(SinkFilterState::Some(item, fut_));
			}
			let filter = ready!(fut.as_mut().fut().unwrap().poll(cx));
			let item = fut.as_mut().take().unwrap();
			if filter {
				break Poll::Ready(Some(item));
			}
		});
		pin_mut!(stream);
		(self_.i).poll_forward(cx, stream)
	}
}

#[pin_project]
#[derive(new)]
pub struct SinkThen<'a, F, I, Fut, R, Item> {
	fut: Pin<&'a mut Option<Fut>>,
	#[pin]
	i: I,
	f: F,
	marker: PhantomData<fn(R, Item)>,
}
impl<'a, F, I, R, Fut, Item> Sink for SinkThen<'a, F, I, Fut, R, Item>
where
	F: FnMut(Item) -> Fut,
	Fut: Future<Output = R>,
	I: Sink<Item = R>,
{
	type Item = Item;

	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		let f = self_.f;
		let fut = self_.fut;
		let stream = stream::poll_fn(|cx| {
			if fut.is_none() {
				let item = ready!(stream.as_mut().poll_next(cx));
				if item.is_none() {
					return Poll::Ready(None);
				}
				fut.set(Some(f(item.unwrap())));
			}
			let item = ready!(fut.as_mut().as_pin_mut().unwrap().poll(cx));
			fut.set(None);
			Poll::Ready(Some(item))
		});
		pin_mut!(stream);
		(self_.i).poll_forward(cx, stream)
	}
}

#[pin_project]
#[derive(new)]
pub struct SinkFlatMap<'a, F, I, Fut, R, Item> {
	fut: Pin<&'a mut Option<Fut>>,
	#[pin]
	i: I,
	f: F,
	marker: PhantomData<fn() -> (Fut, R, Item)>,
}
impl<'a, F, I, R, Fut, Item> Sink for SinkFlatMap<'a, F, I, Fut, R, Item>
where
	F: FnMut(Item) -> Fut,
	Fut: Stream<Item = R>,
	I: Sink<Item = R>,
{
	type Item = Item;

	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		let f = self_.f;
		let fut = self_.fut;
		let stream = stream::poll_fn(|cx| loop {
			if fut.is_none() {
				let item = ready!(stream.as_mut().poll_next(cx));
				if item.is_none() {
					return Poll::Ready(None);
				}
				fut.set(Some(f(item.unwrap())));
			}
			let item = ready!(fut.as_mut().as_pin_mut().unwrap().poll_next(cx));
			if let Some(item) = item {
				break Poll::Ready(Some(item));
			}
			fut.set(None);
		});
		pin_mut!(stream);
		(self_.i).poll_forward(cx, stream)
	}
}
