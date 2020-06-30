#![allow(clippy::type_complexity)]

use derive_new::new;
use educe::Educe;
use futures::{pin_mut, ready, stream, Stream, StreamExt};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	collections::HashMap, hash::Hash, marker::PhantomData, mem, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedPipe, DistributedSink, ParallelPipe, ParallelSink, Reducer, ReducerAsync, ReducerProcessSend, ReducerSend
};
use crate::{pool::ProcessSend, util::type_coerce};

#[derive(new)]
#[must_use]
pub struct GroupBy<A, B> {
	a: A,
	b: B,
}

impl<A: ParallelPipe<Source, Item = (T, U)>, B: ParallelSink<U>, Source, T, U> ParallelSink<Source>
	for GroupBy<A, B>
where
	T: Eq + Hash + Send + 'static,
	B::Pipe: Clone + Send + 'static,
	B::ReduceA: Clone + Send + 'static,
	B::ReduceC: Clone,
	B::Output: Send + 'static,
{
	type Output = HashMap<T, B::Output>;
	type Pipe = A;
	type ReduceA = GroupByReducerA<B::Pipe, B::ReduceA, T, U>;
	type ReduceC = GroupByReducerB<B::ReduceC, T, <B::ReduceA as Reducer>::Output>;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceC) {
		let (a, b, c) = self.b.reducers();
		(self.a, GroupByReducerA::new(a, b), GroupByReducerB::new(c))
	}
}

impl<A: DistributedPipe<Source, Item = (T, U)>, B: DistributedSink<U>, Source, T, U>
	DistributedSink<Source> for GroupBy<A, B>
where
	T: Eq + Hash + ProcessSend + 'static,
	B::Pipe: Clone + ProcessSend + 'static,
	B::ReduceA: Clone + ProcessSend + 'static,
	B::ReduceB: Clone,
	B::ReduceC: Clone,
	B::Output: ProcessSend + 'static,
{
	type Output = HashMap<T, B::Output>;
	type Pipe = A;
	type ReduceA = GroupByReducerA<B::Pipe, B::ReduceA, T, U>;
	type ReduceB = GroupByReducerB<B::ReduceB, T, <B::ReduceA as Reducer>::Output>;
	type ReduceC = GroupByReducerB<B::ReduceC, T, <B::ReduceB as Reducer>::Output>;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		let (a, b, c, d) = self.b.reducers();
		(
			self.a,
			GroupByReducerA::new(a, b),
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

impl<P, R, T, U> Reducer for GroupByReducerA<P, R, T, U>
where
	R: Reducer + Clone,
	T: Eq + Hash,
{
	type Item = (T, U);
	type Output = HashMap<T, R::Output>;
	type Async = GroupByReducerAAsync<P, R, R::Async, T, U>;

	fn into_async(self) -> Self::Async {
		GroupByReducerAAsync::new(self.0, self.1)
	}
}
impl<P, R, T, U> ReducerProcessSend for GroupByReducerA<P, R, T, U>
where
	R: Reducer + Clone,
	T: Eq + Hash + ProcessSend + 'static,
	R::Output: ProcessSend + 'static,
{
	type Output = HashMap<T, R::Output>;
}
impl<P, R, T, U> ReducerSend for GroupByReducerA<P, R, T, U>
where
	R: Reducer + Clone,
	T: Eq + Hash + Send + 'static,
	R::Output: Send + 'static,
{
	type Output = HashMap<T, R::Output>;
}

#[pin_project]
#[derive(new)]
pub struct GroupByReducerAAsync<P, R, RA, T, U> {
	pipe: P,
	factory: R,
	#[new(default)]
	pending: Option<Option<(T, Option<U>, Option<Pin<Box<RA>>>)>>,
	#[new(default)]
	map: HashMap<T, Pin<Box<RA>>>,
	marker: PhantomData<fn() -> (R, U)>,
}

impl<P, R, T, U> ReducerAsync for GroupByReducerAAsync<P, R, R::Async, T, U>
where
	R: Reducer + Clone,
	T: Eq + Hash,
{
	type Item = (T, U);
	type Output = HashMap<T, R::Output>;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		loop {
			if !self_.pending.is_some() {
				*self_.pending = Some(ready!(stream.as_mut().poll_next(cx)).map(|(t, u)| {
					let r = if !self_.map.contains_key(&t) {
						Some(Box::pin(self_.factory.clone().into_async()))
					} else {
						None
					};
					(t, Some(u), r)
				}));
			}
			if let Some((t, u, r)) = self_.pending.as_mut().unwrap() {
				let stream = stream::poll_fn(|_cx| {
					if let Some(u) = u.take() {
						Poll::Ready(Some(type_coerce(u)))
					} else {
						Poll::Pending
					}
				})
				.fuse();
				pin_mut!(stream);
				let map = &mut *self_.map;
				let r_ = r.as_mut().unwrap_or_else(|| map.get_mut(&t).unwrap());
				let _ = r_.as_mut().poll_forward(cx, stream);
				if u.is_some() {
					return Poll::Pending;
				}
				let (t, _u, r) = self_.pending.take().unwrap().unwrap();
				if let Some(r) = r {
					let _ = self_.map.insert(t, r);
				}
			} else {
				for r in self_.map.values_mut() {
					let stream = stream::empty();
					pin_mut!(stream);
					ready!(r.as_mut().poll_forward(cx, stream));
				}
				return Poll::Ready(());
			}
		}
	}
	fn poll_output(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		let self_ = self.project();
		Poll::Ready(
			mem::take(self_.map)
				.into_iter()
				.map(|(k, mut v)| {
					(
						k,
						if let Poll::Ready(x) = v.as_mut().poll_output(cx) {
							x
						} else {
							todo!()
						},
					)
				})
				.collect(),
		)
	}
}

#[derive(Educe, Serialize, Deserialize, new)]
#[educe(Clone(bound = "R: Clone"))]
#[serde(
	bound(serialize = "R: Serialize"),
	bound(deserialize = "R: Deserialize<'de>")
)]
pub struct GroupByReducerB<R, T, U>(R, PhantomData<fn() -> (T, U)>);

impl<R, T, U> Reducer for GroupByReducerB<R, T, U>
where
	R: Reducer + Clone,
	T: Eq + Hash,
{
	type Item = HashMap<T, U>;
	type Output = HashMap<T, R::Output>;
	type Async = GroupByReducerBAsync<R, R::Async, T, U>;

	fn into_async(self) -> Self::Async {
		GroupByReducerBAsync::new(self.0)
	}
}
impl<R, T, U> ReducerProcessSend for GroupByReducerB<R, T, U>
where
	R: Reducer + Clone,
	T: Eq + Hash + ProcessSend + 'static,
	R::Output: ProcessSend + 'static,
{
	type Output = HashMap<T, R::Output>;
}
impl<R, T, U> ReducerSend for GroupByReducerB<R, T, U>
where
	R: Reducer + Clone,
	T: Eq + Hash + Send + 'static,
	R::Output: Send + 'static,
{
	type Output = HashMap<T, R::Output>;
}

#[pin_project]
#[derive(new)]
pub struct GroupByReducerBAsync<R, RA, T, U> {
	f: R,
	#[new(default)]
	pending: Option<Option<HashMap<T, (U, Option<Pin<Box<RA>>>)>>>,
	#[new(default)]
	map: HashMap<T, Pin<Box<RA>>>,
	marker: PhantomData<fn() -> (T, U)>,
}

impl<R, T, U> ReducerAsync for GroupByReducerBAsync<R, R::Async, T, U>
where
	R: Reducer + Clone,
	R::Async: ReducerAsync,
	T: Eq + Hash,
{
	type Item = HashMap<T, U>;
	type Output = HashMap<T, R::Output>;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		loop {
			if self_.pending.is_none() {
				*self_.pending = Some(ready!(stream.as_mut().poll_next(cx)).map(|item| {
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
				}));
			}
			let pending = self_.pending.as_mut().unwrap();
			if let Some(pending) = pending {
				while let Some((k, (v, mut r))) = remove_arbitrary(pending) {
					let mut v = Some(v);
					let stream = stream::poll_fn(|_cx| {
						if let Some(v) = v.take() {
							Poll::Ready(Some(type_coerce::<U, R::Item>(v)))
						} else {
							Poll::Pending
						}
					});
					pin_mut!(stream);
					let map = &mut *self_.map;
					let r_ = r.as_mut().unwrap_or_else(|| map.get_mut(&k).unwrap());
					let _ = r_.as_mut().poll_forward(cx, stream);
					if let Some(v) = v {
						let _ = pending.insert(k, (v, r));
						return Poll::Pending;
					}
					if let Some(r) = r {
						let _ = self_.map.insert(k, r);
					}
				}
				*self_.pending = None;
			} else {
				for r in self_.map.values_mut() {
					let stream = stream::empty();
					pin_mut!(stream);
					ready!(r.as_mut().poll_forward(cx, stream));
				}
				return Poll::Ready(());
			}
		}
	}
	fn poll_output(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		let self_ = self.project();
		Poll::Ready(
			mem::take(self_.map)
				.into_iter()
				.map(|(k, mut v)| {
					(
						k,
						if let Poll::Ready(x) = ReducerAsync::poll_output(v.as_mut(), cx) {
							x
						} else {
							todo!()
						},
					)
				})
				.collect(),
		)
	}
}

#[allow(clippy::unnecessary_filter_map)]
fn remove_arbitrary<K, V>(map: &mut HashMap<K, V>) -> Option<(K, V)>
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
