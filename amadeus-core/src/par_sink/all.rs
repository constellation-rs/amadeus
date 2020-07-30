use derive_new::new;
use educe::Educe;
use futures::{ready, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use serde_closure::traits::FnMut;
use std::{
	marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedPipe, DistributedSink, ParallelPipe, ParallelSink, Reducer, ReducerProcessSend, ReducerSend
};
use crate::{pipe::Sink, pool::ProcessSend};

#[derive(new)]
#[must_use]
pub struct All<P, F> {
	pipe: P,
	f: F,
}

impl<P: ParallelPipe<Item>, Item, F> ParallelSink<Item> for All<P, F>
where
	F: FnMut<(P::Output,), Output = bool> + Clone + Send,
{
	type Done = bool;
	type Pipe = P;
	type ReduceA = AllReducer<P::Output, F>;
	type ReduceC = BoolAndReducer;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceC) {
		(self.pipe, AllReducer(self.f, PhantomData), BoolAndReducer)
	}
}
impl<P: DistributedPipe<Item>, Item, F> DistributedSink<Item> for All<P, F>
where
	F: FnMut<(P::Output,), Output = bool> + Clone + ProcessSend,
{
	type Done = bool;
	type Pipe = P;
	type ReduceA = AllReducer<P::Output, F>;
	type ReduceB = BoolAndReducer;
	type ReduceC = BoolAndReducer;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		(
			self.pipe,
			AllReducer(self.f, PhantomData),
			BoolAndReducer,
			BoolAndReducer,
		)
	}
}

#[derive(Educe, Serialize, Deserialize)]
#[educe(Clone(bound = "F: Clone"))]
#[serde(
	bound(serialize = "F: Serialize"),
	bound(deserialize = "F: Deserialize<'de>")
)]
pub struct AllReducer<Item, F>(F, PhantomData<fn() -> Item>);

impl<Item, F> Reducer<Item> for AllReducer<Item, F>
where
	F: FnMut<(Item,), Output = bool>,
{
	type Done = bool;
	type Async = AllReducerAsync<Item, F>;

	fn into_async(self) -> Self::Async {
		AllReducerAsync(self.0, true, PhantomData)
	}
}
impl<Item, F> ReducerProcessSend<Item> for AllReducer<Item, F>
where
	F: FnMut<(Item,), Output = bool>,
{
	type Done = bool;
}
impl<Item, F> ReducerSend<Item> for AllReducer<Item, F>
where
	F: FnMut<(Item,), Output = bool>,
{
	type Done = bool;
}

#[pin_project]
#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "F: Serialize"),
	bound(deserialize = "F: Deserialize<'de>")
)]
pub struct AllReducerAsync<Item, F>(F, bool, PhantomData<fn() -> Item>);

impl<Item, F> Sink<Item> for AllReducerAsync<Item, F>
where
	F: FnMut<(Item,), Output = bool>,
{
	type Done = bool;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<Self::Done> {
		let self_ = self.project();
		while *self_.1 {
			if let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
				*self_.1 = *self_.1 && self_.0.call_mut((item,));
			} else {
				break;
			}
		}
		Poll::Ready(*self_.1)
	}
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BoolAndReducer;

impl Reducer<bool> for BoolAndReducer {
	type Done = bool;
	type Async = BoolAndReducerAsync;

	fn into_async(self) -> Self::Async {
		BoolAndReducerAsync(true)
	}
}
impl ReducerProcessSend<bool> for BoolAndReducer {
	type Done = bool;
}
impl ReducerSend<bool> for BoolAndReducer {
	type Done = bool;
}

#[pin_project]
pub struct BoolAndReducerAsync(bool);
impl Sink<bool> for BoolAndReducerAsync {
	type Done = bool;

	#[inline(always)]
	fn poll_forward(
		mut self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = bool>>,
	) -> Poll<Self::Done> {
		while self.0 {
			if let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
				self.0 = self.0 && item;
			} else {
				break;
			}
		}
		Poll::Ready(self.0)
	}
}
