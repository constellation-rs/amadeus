use futures::{pin_mut, ready, stream, Future, Stream, StreamExt};
use pin_project::{pin_project, project, project_replace};
use std::{
	any::type_name, marker::PhantomData, pin::Pin, task::{Context, Poll}
};

pub trait Sink<Item> {
	/// returns 'false' when no more data should be passed
	fn poll_sink(
		&mut self, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<bool>;
}

impl<Item, T> Sink<Item> for &mut T
where
	T: Sink<Item>,
{
	fn poll_sink(
		&mut self, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<bool> {
		(**self).poll_sink(cx, stream)
	}
}

pub struct SinkMap<F, I, R>(F, I, PhantomData<(R,)>);
impl<F, I, R> SinkMap<F, I, R> {
	pub fn new(i: I, f: F) -> Self {
		Self(f, i, PhantomData)
	}
}
impl<F, I, R, Item> Sink<Item> for SinkMap<F, I, R>
where
	F: FnMut(Item) -> R,
	I: Sink<R>,
{
	fn poll_sink(
		&mut self, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<bool> {
		let (f, i) = (&mut self.0, &mut self.1);
		let stream = stream.map(f);
		pin_mut!(stream);
		i.poll_sink(cx, stream)
	}
}
#[pin_project(Replace)]
pub enum SinkFilterState<T, Fut> {
	Some(T, #[pin] Fut),
	None,
}
impl<T, Fut> SinkFilterState<T, Fut> {
	#[project]
	fn fut(self: Pin<&mut Self>) -> Option<Pin<&mut Fut>> {
		#[project]
		match self.project() {
			SinkFilterState::Some(_, fut) => Some(fut),
			SinkFilterState::None => None,
		}
	}
	#[project_replace]
	fn take(self: Pin<&mut Self>) -> Option<T> {
		#[project_replace]
		match self.project_replace(SinkFilterState::None) {
			SinkFilterState::Some(t, _) => Some(t),
			SinkFilterState::None => None,
		}
	}
}
pub struct SinkFilter<'a, F, I, T, Fut>(
	F,
	I,
	Pin<&'a mut SinkFilterState<T, Fut>>,
	PhantomData<()>,
);
impl<'a, F, I, T, Fut> SinkFilter<'a, F, I, T, Fut> {
	pub fn new(fut: Pin<&'a mut SinkFilterState<T, Fut>>, i: I, f: F) -> Self {
		Self(f, i, fut, PhantomData)
	}
}
impl<'a, F, I, Fut, Item> Sink<Item> for SinkFilter<'a, F, I, Item, Fut>
where
	F: FnMut(&Item) -> Fut,
	Fut: Future<Output = bool>,
	I: Sink<Item>,
{
	#[project_replace]
	fn poll_sink(
		&mut self, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<bool> {
		let f = &mut self.0;
		let fut = &mut self.2;
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
		(self.1).poll_sink(cx, stream)
	}
}
pub struct SinkBuffer<'a, F, Item>(F, &'a mut Option<Option<Item>>, PhantomData<()>);
impl<'a, F, Item> SinkBuffer<'a, F, Item> {
	pub fn new(buffer: &'a mut Option<Option<Item>>, f: F) -> Self {
		Self(f, buffer, PhantomData)
	}
}
impl<'a, F, Item> Sink<Item> for SinkBuffer<'a, F, Item>
where
	F: FnMut(&mut Context, &mut Option<Item>) -> Poll<bool>,
{
	fn poll_sink(
		&mut self, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<bool> {
		if self.1.is_none() {
			pin_mut!(stream);
			*self.1 = Some(ready!(stream.poll_next(cx)));
		}
		let ret = (self.0)(cx, self.1.as_mut().unwrap());
		assert!(
			!ret.is_ready() || self.1.as_ref().unwrap().is_none(),
			"{}",
			type_name::<F>()
		);
		*self.1 = None;
		ret
	}
}
pub struct SinkThen<'a, F, I, Fut, R>(F, I, Pin<&'a mut Option<Fut>>, PhantomData<R>);
impl<'a, F, I, Fut, R> SinkThen<'a, F, I, Fut, R> {
	pub fn new(fut: Pin<&'a mut Option<Fut>>, i: I, f: F) -> Self {
		Self(f, i, fut, PhantomData)
	}
}
impl<'a, F, I, R, Fut, Item> Sink<Item> for SinkThen<'a, F, I, Fut, R>
where
	F: FnMut(Item) -> Fut,
	Fut: Future<Output = R>,
	I: Sink<R>,
{
	fn poll_sink(
		&mut self, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<bool> {
		let f = &mut self.0;
		let fut = &mut self.2;
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
		(self.1).poll_sink(cx, stream)
	}
}
pub struct SinkFlatMap<'a, F, I, Fut, R>(F, I, Pin<&'a mut Option<Fut>>, PhantomData<(Fut, R)>);
impl<'a, F, I, Fut, R> SinkFlatMap<'a, F, I, Fut, R> {
	pub fn new(fut: Pin<&'a mut Option<Fut>>, i: I, f: F) -> Self {
		Self(f, i, fut, PhantomData)
	}
}
impl<'a, F, I, R, Fut, Item> Sink<Item> for SinkFlatMap<'a, F, I, Fut, R>
where
	F: FnMut(Item) -> Fut,
	Fut: Stream<Item = R>,
	I: Sink<R>,
{
	fn poll_sink(
		&mut self, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<bool> {
		let f = &mut self.0;
		let fut = &mut self.2;
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
		(self.1).poll_sink(cx, stream)
	}
}
