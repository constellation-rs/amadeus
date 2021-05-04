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
pub struct Any<P, F> {
	pipe: P,
	f: F,
}

impl<P: ParallelPipe<Item>, Item, F> ParallelSink<Item> for Any<P, F>
where
	F: FnMut<(P::Output,), Output = bool> + Clone + Send,
{
	type Done = bool;
	type Pipe = P;
	type ReduceA = AnyReducer<P::Output, F>;
	type ReduceC = BoolOrReducer;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceC) {
		(self.pipe, AnyReducer(self.f, PhantomData), BoolOrReducer)
	}
}
impl<P: DistributedPipe<Item>, Item, F> DistributedSink<Item> for Any<P, F>
where
	F: FnMut<(P::Output,), Output = bool> + Clone + ProcessSend,
{
	type Done = bool;
	type Pipe = P;
	type ReduceA = AnyReducer<P::Output, F>;
	type ReduceB = BoolOrReducer;
	type ReduceC = BoolOrReducer;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		(
			self.pipe,
			AnyReducer(self.f, PhantomData),
			BoolOrReducer,
			BoolOrReducer,
		)
	}
}

#[derive(Educe, Serialize, Deserialize)]
#[educe(Clone(bound = "F: Clone"))]
#[serde(
	bound(serialize = "F: Serialize"),
	bound(deserialize = "F: Deserialize<'de>")
)]
pub struct AnyReducer<Item, F>(F, PhantomData<fn() -> Item>);

impl<Item, F> Reducer<Item> for AnyReducer<Item, F>
where
	F: FnMut<(Item,), Output = bool>,
{
	type Done = bool;
	type Async = AnyReducerAsync<Item, F>;

	fn into_async(self) -> Self::Async {
		AnyReducerAsync(self.0, true, PhantomData)
	}
}
impl<Item, F> ReducerProcessSend<Item> for AnyReducer<Item, F>
where
	F: FnMut<(Item,), Output = bool>,
{
	type Done = bool;
}
impl<Item, F> ReducerSend<Item> for AnyReducer<Item, F>
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
pub struct AnyReducerAsync<Item, F>(F, bool, PhantomData<fn() -> Item>);

impl<Item, F> Sink<Item> for AnyReducerAsync<Item, F>
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
				*self_.1 = *self_.1 && !self_.0.call_mut((item,));
			} else {
				break;
			}
		}
		Poll::Ready(!*self_.1)
	}
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BoolOrReducer;

impl Reducer<bool> for BoolOrReducer {
	type Done = bool;
	type Async = BoolOrReducerAsync;

	fn into_async(self) -> Self::Async {
		BoolOrReducerAsync(true)
	}
}
impl ReducerProcessSend<bool> for BoolOrReducer {
	type Done = bool;
}
impl ReducerSend<bool> for BoolOrReducer {
	type Done = bool;
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct BoolOrReducerAsync(bool);

impl Sink<bool> for BoolOrReducerAsync {
	type Done = bool;

	#[inline(always)]
	fn poll_forward(
		mut self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = bool>>,
	) -> Poll<Self::Done> {
		while self.0 {
			if let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
				self.0 = self.0 && !item;
			} else {
				break;
			}
		}
		Poll::Ready(!self.0)
	}
}
