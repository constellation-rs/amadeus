use derive_new::new;
use educe::Educe;
use futures::{ready, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedPipe, DistributedSink, ParallelPipe, ParallelSink, Reducer, ReducerAsync, ReducerProcessSend, ReducerSend
};
use crate::pool::ProcessSend;

#[derive(new)]
#[must_use]
pub struct All<I, F> {
	i: I,
	f: F,
}

impl<I: ParallelPipe<Source>, Source, F> ParallelSink<Source> for All<I, F>
where
	F: FnMut(I::Item) -> bool + Clone + Send + 'static,
{
	type Output = bool;
	type Pipe = I;
	type ReduceA = AllReducer<I::Item, F>;
	type ReduceC = BoolAndReducer;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceC) {
		(self.i, AllReducer(self.f, PhantomData), BoolAndReducer)
	}
}
impl<I: DistributedPipe<Source>, Source, F> DistributedSink<Source> for All<I, F>
where
	F: FnMut(I::Item) -> bool + Clone + ProcessSend + 'static,
{
	type Output = bool;
	type Pipe = I;
	type ReduceA = AllReducer<I::Item, F>;
	type ReduceB = BoolAndReducer;
	type ReduceC = BoolAndReducer;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		(
			self.i,
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
pub struct AllReducer<A, F>(F, PhantomData<fn() -> A>);

impl<A, F> Reducer for AllReducer<A, F>
where
	F: FnMut(A) -> bool,
{
	type Item = A;
	type Output = bool;
	type Async = AllReducerAsync<A, F>;

	fn into_async(self) -> Self::Async {
		AllReducerAsync(self.0, true, PhantomData)
	}
}
impl<A, F> ReducerProcessSend for AllReducer<A, F>
where
	F: FnMut(A) -> bool,
{
	type Output = bool;
}
impl<A, F> ReducerSend for AllReducer<A, F>
where
	F: FnMut(A) -> bool,
{
	type Output = bool;
}

#[pin_project]
#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "F: Serialize"),
	bound(deserialize = "F: Deserialize<'de>")
)]
pub struct AllReducerAsync<A, F>(F, bool, PhantomData<fn() -> A>);

impl<A, F> ReducerAsync for AllReducerAsync<A, F>
where
	F: FnMut(A) -> bool,
{
	type Item = A;
	type Output = bool;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		while *self_.1 {
			if let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
				*self_.1 = *self_.1 && self_.0(item);
			} else {
				break;
			}
		}
		Poll::Ready(())
	}
	fn poll_output(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
		Poll::Ready(self.1)
	}
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BoolAndReducer;

impl Reducer for BoolAndReducer {
	type Item = bool;
	type Output = bool;
	type Async = BoolAndReducerAsync;

	fn into_async(self) -> Self::Async {
		BoolAndReducerAsync(true)
	}
}
impl ReducerProcessSend for BoolAndReducer {
	type Output = bool;
}
impl ReducerSend for BoolAndReducer {
	type Output = bool;
}

#[pin_project]
pub struct BoolAndReducerAsync(bool);
impl ReducerAsync for BoolAndReducerAsync {
	type Item = bool;
	type Output = bool;

	#[inline(always)]
	fn poll_forward(
		mut self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		while self.0 {
			if let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
				self.0 = self.0 && item;
			} else {
				break;
			}
		}
		Poll::Ready(())
	}
	fn poll_output(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
		Poll::Ready(self.0)
	}
}
