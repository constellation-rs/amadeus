#![allow(clippy::type_complexity)]

use derive_new::new;
use educe::Educe;
use futures::{pin_mut, ready, stream, Stream, StreamExt};
use indexmap::IndexMap;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	hash::Hash, marker::PhantomData, mem, pin::Pin, task::{Context, Poll}
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

impl<A: ParallelPipe<Item, Output = (T, U)>, B: ParallelSink<U>, Item, T, U> ParallelSink<Item>
	for GroupBy<A, B>
where
	T: Eq + Hash + Send,
	<B::Pipe as ParallelPipe<U>>::Task: Clone + Send,
	B::ReduceA: Clone + Send,
	B::ReduceC: Clone,
	B::Done: Send,
{
	type Done = IndexMap<T, B::Done>;
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

impl<A: DistributedPipe<Item, Output = (T, U)>, B: DistributedSink<U>, Item, T, U>
	DistributedSink<Item> for GroupBy<A, B>
where
	T: Eq + Hash + ProcessSend,
	<B::Pipe as DistributedPipe<U>>::Task: Clone + ProcessSend,
	B::ReduceA: Clone + ProcessSend,
	B::ReduceB: Clone,
	B::ReduceC: Clone,
	B::Done: ProcessSend,
{
	type Done = IndexMap<T, B::Done>;
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
pub struct GroupByReducerA<P, R, T, U>(P, R, PhantomData<fn() -> (T, U)>);

impl<P, R, T, U> Reducer<(T, U)> for GroupByReducerA<P, R, T, U>
where
	P: PipeTask<U>,
	R: Reducer<P::Output> + Clone,
	T: Eq + Hash,
{
	type Done = IndexMap<T, R::Done>;
	type Async = GroupByReducerAAsync<P::Async, R, T, U>;

	fn into_async(self) -> Self::Async {
		GroupByReducerAAsync::new(self.0.into_async(), self.1)
	}
}
impl<P, R, T, U> ReducerProcessSend<(T, U)> for GroupByReducerA<P, R, T, U>
where
	P: PipeTask<U>,
	R: Reducer<P::Output> + Clone,
	T: Eq + Hash + ProcessSend,
	R::Done: ProcessSend,
{
	type Done = IndexMap<T, R::Done>;
}
impl<P, R, T, U> ReducerSend<(T, U)> for GroupByReducerA<P, R, T, U>
where
	P: PipeTask<U>,
	R: Reducer<P::Output> + Clone,
	T: Eq + Hash + Send,
	R::Done: Send,
{
	type Done = IndexMap<T, R::Done>;
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
	map: IndexMap<T, Pin<Box<R::Async>>>,
}

impl<P, R, T, U> Sink<(T, U)> for GroupByReducerAAsync<P, R, T, U>
where
	P: Pipe<U>,
	R: Reducer<P::Output> + Clone,
	T: Eq + Hash,
{
	type Done = IndexMap<T, R::Done>;

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
						u.take().map_or_else(
							|| {
								let waker_ = cx.waker();
								if !waker.will_wake(waker_) {
									waker_.wake_by_ref();
								}
								Poll::Pending
							},
							|u| Poll::Ready(Some(u)),
						)
					})
					.fuse()
					.pipe(self_.pipe.as_mut());
					pin_mut!(stream);
					let map = &mut *self_.map;
					let r_ = r.as_mut().unwrap_or_else(|| map.get_mut(k).unwrap());
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
					let ret = mem::take(self_.map)
						.into_iter()
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

impl<R, T, U> Reducer<IndexMap<T, U>> for GroupByReducerB<R, T, U>
where
	R: Reducer<U> + Clone,
	T: Eq + Hash,
{
	type Done = IndexMap<T, R::Done>;
	type Async = GroupByReducerBAsync<R, T, U>;

	fn into_async(self) -> Self::Async {
		GroupByReducerBAsync::new(self.0)
	}
}
impl<R, T, U> ReducerProcessSend<IndexMap<T, U>> for GroupByReducerB<R, T, U>
where
	R: Reducer<U> + Clone,
	T: Eq + Hash + ProcessSend,
	R::Done: ProcessSend,
{
	type Done = IndexMap<T, R::Done>;
}
impl<R, T, U> ReducerSend<IndexMap<T, U>> for GroupByReducerB<R, T, U>
where
	R: Reducer<U> + Clone,
	T: Eq + Hash + Send,
	R::Done: Send,
{
	type Done = IndexMap<T, R::Done>;
}

#[pin_project]
#[derive(new)]
pub struct GroupByReducerBAsync<R, T, U>
where
	R: Reducer<U>,
{
	f: R,
	#[new(default)]
	pending: Option<Sum2<IndexMap<T, (U, Option<Pin<Box<R::Async>>>)>, Vec<Option<R::Done>>>>,
	#[new(default)]
	map: IndexMap<T, Pin<Box<R::Async>>>,
}

impl<R, T, U> Sink<IndexMap<T, U>> for GroupByReducerBAsync<R, T, U>
where
	R: Reducer<U> + Clone,
	T: Eq + Hash,
{
	type Done = IndexMap<T, R::Done>;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = IndexMap<T, U>>>,
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
					while let Some((k, (v, mut r))) = pending.pop() {
						let mut v = Some(v);
						let waker = cx.waker();
						let stream = stream::poll_fn(|cx| {
							v.take().map_or_else(
								|| {
									let waker_ = cx.waker();
									if !waker.will_wake(waker_) {
										waker_.wake_by_ref();
									}
									Poll::Pending
								},
								|v| Poll::Ready(Some(v)),
							)
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
					let ret = mem::take(self_.map)
						.into_iter()
						.zip(done.iter_mut())
						.map(|((k, _), v)| (k, v.take().unwrap()))
						.collect();
					return Poll::Ready(ret);
				}
			}
		}
	}
}
