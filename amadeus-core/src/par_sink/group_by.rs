#![allow(clippy::type_complexity)]

use derive_new::new;
use educe::Educe;
use futures::{pin_mut, ready, stream, Stream, StreamExt};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	collections::HashMap, hash::Hash, marker::PhantomData, mem, pin::Pin, task::{Context, Poll}
};
use sum::Sum2;

use super::{
	DistributedPipe, DistributedSink, ParallelPipe, ParallelSink, PipeTask, Reducer, ReducerProcessSend, ReducerSend
};
use crate::{
	pipe::{Pipe, Sink, StreamExt as _}, pool::ProcessSend
};

#[derive(new)]
#[must_use]
pub struct GroupBy<A, B> {
	a: A,
	b: B,
}

impl<A: ParallelPipe<Input, Output = (T, U)>, B: ParallelSink<U>, Input, T, U> ParallelSink<Input>
	for GroupBy<A, B>
where
	T: Eq + Hash + Send + 'static,
	<B::Pipe as ParallelPipe<U>>::Task: Clone + Send + 'static,
	B::ReduceA: Clone + Send + 'static,
	B::ReduceC: Clone,
	B::Done: Send + 'static,
{
	type Done = HashMap<T, B::Done>;
	type Pipe = A;
	type ReduceA = GroupByReducerA<<B::Pipe as ParallelPipe<U>>::Task, B::ReduceA, T, U>;
	type ReduceC = GroupByReducerB<
		B::ReduceC,
		T,
		<B::ReduceA as ReducerSend<<B::Pipe as ParallelPipe<U>>::Output>>::Done,
	>;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceC) {
		let (a, b, c) = self.b.reducers();
		(
			self.a,
			GroupByReducerA::new(a.task(), b),
			GroupByReducerB::new(c),
		)
	}
}

impl<A: DistributedPipe<Input, Output = (T, U)>, B: DistributedSink<U>, Input, T, U>
	DistributedSink<Input> for GroupBy<A, B>
where
	T: Eq + Hash + ProcessSend + 'static,
	<B::Pipe as DistributedPipe<U>>::Task: Clone + ProcessSend + 'static,
	B::ReduceA: Clone + ProcessSend + 'static,
	B::ReduceB: Clone,
	B::ReduceC: Clone,
	B::Done: ProcessSend + 'static,
{
	type Done = HashMap<T, B::Done>;
	type Pipe = A;
	type ReduceA = GroupByReducerA<<B::Pipe as DistributedPipe<U>>::Task, B::ReduceA, T, U>;
	type ReduceB = GroupByReducerB<
		B::ReduceB,
		T,
		<B::ReduceA as ReducerSend<<B::Pipe as DistributedPipe<U>>::Output>>::Done,
	>;
	type ReduceC = GroupByReducerB<
		B::ReduceC,
		T,
		<B::ReduceB as ReducerProcessSend<
			<B::ReduceA as Reducer<<B::Pipe as DistributedPipe<U>>::Output>>::Done,
		>>::Done,
	>;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		let (a, b, c, d) = self.b.reducers();
		(
			self.a,
			GroupByReducerA::new(a.task(), b),
			GroupByReducerB::new(c),
			GroupByReducerB::new(d),
		)
	}
}

#[derive(Educe, Serialize, Deserialize, new)]
#[educe(Clone(bound = "P: Clone, R: Clone"))]
#[serde(
	bound(serialize = "P: Serialize, R: Serialize"),
	bound(deserialize = "P: Deserialize<'de>, R: Deserialize<'de>")
)]
pub struct GroupByReducerA<P, R, T, U>(P, R, PhantomData<fn() -> (R, T, U)>);

impl<P, R, T, U> Reducer<(T, U)> for GroupByReducerA<P, R, T, U>
where
	P: PipeTask<U>,
	R: Reducer<P::Output> + Clone,
	T: Eq + Hash,
{
	type Done = HashMap<T, R::Done>;
	type Async = GroupByReducerAAsync<P::Async, R, T, U>;

	fn into_async(self) -> Self::Async {
		GroupByReducerAAsync::new(self.0.into_async(), self.1)
	}
}
impl<P, R, T, U> ReducerProcessSend<(T, U)> for GroupByReducerA<P, R, T, U>
where
	P: PipeTask<U>,
	R: Reducer<P::Output> + Clone,
	T: Eq + Hash + ProcessSend + 'static,
	R::Done: ProcessSend + 'static,
{
	type Done = HashMap<T, R::Done>;
}
impl<P, R, T, U> ReducerSend<(T, U)> for GroupByReducerA<P, R, T, U>
where
	P: PipeTask<U>,
	R: Reducer<P::Output> + Clone,
	T: Eq + Hash + Send + 'static,
	R::Done: Send + 'static,
{
	type Done = HashMap<T, R::Done>;
}

#[pin_project]
#[derive(new)]
pub struct GroupByReducerAAsync<P, R, T, U>
where
	P: Pipe<U>,
	R: Reducer<P::Output>,
{
	#[pin]
	pipe: P,
	factory: R,
	#[new(default)]
	pending: Option<Sum2<(T, Option<U>, Option<Pin<Box<R::Async>>>), Vec<Option<R::Done>>>>,
	#[new(default)]
	map: HashMap<T, Pin<Box<R::Async>>>,
	marker: PhantomData<fn() -> (R, U)>,
}

impl<P, R, T, U> Sink<(T, U)> for GroupByReducerAAsync<P, R, T, U>
where
	P: Pipe<U>,
	R: Reducer<P::Output> + Clone,
	T: Eq + Hash,
{
	type Done = HashMap<T, R::Done>;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = (T, U)>>,
	) -> Poll<Self::Done> {
		let mut self_ = self.project();
		loop {
			if !self_.pending.is_some() {
				*self_.pending = Some(
					ready!(stream.as_mut().poll_next(cx))
						.map(|(k, u)| {
							let r = if !self_.map.contains_key(&k) {
								Some(Box::pin(self_.factory.clone().into_async()))
							} else {
								None
							};
							(k, Some(u), r)
						})
						.map_or_else(
							|| Sum2::B((0..self_.map.len()).map(|_| None).collect()),
							Sum2::A,
						),
				);
			}
			match self_.pending.as_mut().unwrap() {
				Sum2::A((k, u, r)) => {
					let waker = cx.waker();
					let stream = stream::poll_fn(|cx| {
						if let Some(u) = u.take() {
							Poll::Ready(Some(u))
						} else {
							let waker_ = cx.waker();
							if !waker.will_wake(waker_) {
								waker_.wake_by_ref();
							}
							Poll::Pending
						}
					})
					.fuse()
					.pipe(self_.pipe.as_mut());
					pin_mut!(stream);
					let map = &mut *self_.map;
					let r_ = r.as_mut().unwrap_or_else(|| map.get_mut(&k).unwrap());
					if r_.as_mut().poll_forward(cx, stream).is_ready() {
						let _ = u.take();
					}
					if u.is_some() {
						return Poll::Pending;
					}
					let (k, _u, r) = self_.pending.take().unwrap().a().unwrap();
					if let Some(r) = r {
						let _ = self_.map.insert(k, r);
					}
				}
				Sum2::B(done) => {
					let mut done_ = true;
					self_
						.map
						.values_mut()
						.zip(done.iter_mut())
						.for_each(|(r, done)| {
							if done.is_none() {
								let stream = stream::empty();
								pin_mut!(stream);
								if let Poll::Ready(done_) = r.as_mut().poll_forward(cx, stream) {
									*done = Some(done_);
								} else {
									done_ = false;
								}
							}
						});
					if !done_ {
						return Poll::Pending;
					}
					let ret = self_
						.map
						.drain()
						.zip(done.iter_mut())
						.map(|((k, _), v)| (k, v.take().unwrap()))
						.collect();
					return Poll::Ready(ret);
				}
			}
		}
	}
}

#[derive(Educe, Serialize, Deserialize, new)]
#[educe(Clone(bound = "R: Clone"))]
#[serde(
	bound(serialize = "R: Serialize"),
	bound(deserialize = "R: Deserialize<'de>")
)]
pub struct GroupByReducerB<R, T, U>(R, PhantomData<fn() -> (T, U)>);

impl<R, T, U> Reducer<HashMap<T, U>> for GroupByReducerB<R, T, U>
where
	R: Reducer<U> + Clone,
	T: Eq + Hash,
{
	type Done = HashMap<T, R::Done>;
	type Async = GroupByReducerBAsync<R, T, U>;

	fn into_async(self) -> Self::Async {
		GroupByReducerBAsync::new(self.0)
	}
}
impl<R, T, U> ReducerProcessSend<HashMap<T, U>> for GroupByReducerB<R, T, U>
where
	R: Reducer<U> + Clone,
	T: Eq + Hash + ProcessSend + 'static,
	R::Done: ProcessSend + 'static,
{
	type Done = HashMap<T, R::Done>;
}
impl<R, T, U> ReducerSend<HashMap<T, U>> for GroupByReducerB<R, T, U>
where
	R: Reducer<U> + Clone,
	T: Eq + Hash + Send + 'static,
	R::Done: Send + 'static,
{
	type Done = HashMap<T, R::Done>;
}

#[pin_project]
#[derive(new)]
pub struct GroupByReducerBAsync<R, T, U>
where
	R: Reducer<U>,
{
	f: R,
	#[new(default)]
	pending: Option<Sum2<HashMap<T, (U, Option<Pin<Box<R::Async>>>)>, Vec<Option<R::Done>>>>,
	#[new(default)]
	map: HashMap<T, Pin<Box<R::Async>>>,
	marker: PhantomData<fn() -> (T, U)>,
}

impl<R, T, U> Sink<HashMap<T, U>> for GroupByReducerBAsync<R, T, U>
where
	R: Reducer<U> + Clone,
	T: Eq + Hash,
{
	type Done = HashMap<T, R::Done>;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = HashMap<T, U>>>,
	) -> Poll<Self::Done> {
		let self_ = self.project();
		loop {
			if self_.pending.is_none() {
				*self_.pending = Some(
					ready!(stream.as_mut().poll_next(cx))
						.map(|item| {
							item.into_iter()
								.map(|(k, v)| {
									let r = if !self_.map.contains_key(&k) {
										Some(Box::pin(self_.f.clone().into_async()))
									} else {
										None
									};
									(k, (v, r))
								})
								.collect()
						})
						.map_or_else(
							|| Sum2::B((0..self_.map.len()).map(|_| None).collect()),
							Sum2::A,
						),
				);
			}
			match self_.pending.as_mut().unwrap() {
				Sum2::A(pending) => {
					while let Some((k, (v, mut r))) = pop(pending) {
						let mut v = Some(v);
						let waker = cx.waker();
						let stream = stream::poll_fn(|cx| {
							if let Some(v) = v.take() {
								Poll::Ready(Some(v))
							} else {
								let waker_ = cx.waker();
								if !waker.will_wake(waker_) {
									waker_.wake_by_ref();
								}
								Poll::Pending
							}
						})
						.fuse();
						pin_mut!(stream);
						let map = &mut *self_.map;
						let r_ = r.as_mut().unwrap_or_else(|| map.get_mut(&k).unwrap());
						if r_.as_mut().poll_forward(cx, stream).is_ready() {
							let _ = v.take();
						}
						if let Some(v) = v {
							let _ = pending.insert(k, (v, r));
							return Poll::Pending;
						}
						if let Some(r) = r {
							let _ = self_.map.insert(k, r);
						}
					}
					*self_.pending = None;
				}
				Sum2::B(done) => {
					let mut done_ = true;
					self_
						.map
						.values_mut()
						.zip(done.iter_mut())
						.for_each(|(r, done)| {
							if done.is_none() {
								let stream = stream::empty();
								pin_mut!(stream);
								if let Poll::Ready(done_) = r.as_mut().poll_forward(cx, stream) {
									*done = Some(done_);
								} else {
									done_ = false;
								}
							}
						});
					if !done_ {
						return Poll::Pending;
					}
					let ret = self_
						.map
						.drain()
						.zip(done.iter_mut())
						.map(|((k, _), v)| (k, v.take().unwrap()))
						.collect();
					return Poll::Ready(ret);
				}
			}
		}
	}
}

// https://github.com/rust-lang/rfcs/issues/1800#issuecomment-653757340
#[allow(clippy::unnecessary_filter_map)]
fn pop<K, V>(map: &mut HashMap<K, V>) -> Option<(K, V)>
where
	K: Eq + Hash,
{
	let mut first = None;
	*map = mem::take(map)
		.into_iter()
		.filter_map(|el| {
			if first.is_none() {
				first = Some(el);
				None
			} else {
				Some(el)
			}
		})
		.collect();
	first
}
