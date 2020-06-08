use futures::{pin_mut, ready, stream, Future, Stream, StreamExt};
use pin_project::{pin_project, project, project_replace};
use std::{
	marker::PhantomData, ops::DerefMut, pin::Pin, task::{Context, Poll}
};

pub trait Sink<Item> {
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<()>;
}

impl<P, Item> Sink<Item> for Pin<P>
where
	P: DerefMut + Unpin,
	P::Target: Sink<Item>,
{
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<()> {
		self.get_mut().as_mut().poll_forward(cx, stream)
	}
}

impl<T: ?Sized, Item> Sink<Item> for &mut T
where
	T: Sink<Item> + Unpin,
{
	fn poll_forward(
		mut self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<()> {
		Pin::new(&mut **self).poll_forward(cx, stream)
	}
}

#[pin_project]
pub struct SinkMap<F, I, R>(F, #[pin] I, PhantomData<(R,)>);
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
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		let (f, i) = (self_.0, self_.1);
		let stream = stream.map(f);
		pin_mut!(stream);
		i.poll_forward(cx, stream)
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
#[pin_project]
pub struct SinkFilter<'a, F, I, T, Fut>(
	F,
	#[pin] I,
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
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		let f = self_.0;
		let fut = self_.2;
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
		(self_.1).poll_forward(cx, stream)
	}
}

#[pin_project]
pub struct SinkThen<'a, F, I, Fut, R>(F, #[pin] I, Pin<&'a mut Option<Fut>>, PhantomData<R>);
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
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		let f = self_.0;
		let fut = self_.2;
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
		(self_.1).poll_forward(cx, stream)
	}
}

#[pin_project]
pub struct SinkFlatMap<'a, F, I, Fut, R>(
	F,
	#[pin] I,
	Pin<&'a mut Option<Fut>>,
	PhantomData<(Fut, R)>,
);
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
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		let f = self_.0;
		let fut = self_.2;
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
		(self_.1).poll_forward(cx, stream)
	}
}
