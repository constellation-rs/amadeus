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
pub struct Any<I, F> {
	i: I,
	f: F,
}

impl<I: ParallelPipe<Source>, Source, F> ParallelSink<Source> for Any<I, F>
where
	F: FnMut(I::Item) -> bool + Clone + Send + 'static,
{
	type Output = bool;
	type Pipe = I;
	type ReduceA = AnyReducer<I::Item, F>;
	type ReduceC = BoolOrReducer;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceC) {
		(self.i, AnyReducer(self.f, PhantomData), BoolOrReducer)
	}
}
impl<I: DistributedPipe<Source>, Source, F> DistributedSink<Source> for Any<I, F>
where
	F: FnMut(I::Item) -> bool + Clone + ProcessSend + 'static,
{
	type Output = bool;
	type Pipe = I;
	type ReduceA = AnyReducer<I::Item, F>;
	type ReduceB = BoolOrReducer;
	type ReduceC = BoolOrReducer;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		(
			self.i,
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
pub struct AnyReducer<A, F>(F, PhantomData<fn() -> A>);

impl<A, F> Reducer for AnyReducer<A, F>
where
	F: FnMut(A) -> bool,
{
	type Item = A;
	type Output = bool;
	type Async = AnyReducerAsync<A, F>;

	fn into_async(self) -> Self::Async {
		AnyReducerAsync(self.0, true, PhantomData)
	}
}
impl<A, F> ReducerProcessSend for AnyReducer<A, F>
where
	F: FnMut(A) -> bool,
{
	type Output = bool;
}
impl<A, F> ReducerSend for AnyReducer<A, F>
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
pub struct AnyReducerAsync<A, F>(F, bool, PhantomData<fn() -> A>);

impl<A, F> ReducerAsync for AnyReducerAsync<A, F>
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
				*self_.1 = *self_.1 && !self_.0(item);
			} else {
				break;
			}
		}
		Poll::Ready(())
	}
	fn poll_output(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
		Poll::Ready(!self.1)
	}
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BoolOrReducer;

impl Reducer for BoolOrReducer {
	type Item = bool;
	type Output = bool;
	type Async = BoolOrReducerAsync;

	fn into_async(self) -> Self::Async {
		BoolOrReducerAsync(true)
	}
}
impl ReducerProcessSend for BoolOrReducer {
	type Output = bool;
}
impl ReducerSend for BoolOrReducer {
	type Output = bool;
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct BoolOrReducerAsync(bool);

impl ReducerAsync for BoolOrReducerAsync {
	type Item = bool;
	type Output = bool;

	#[inline(always)]
	fn poll_forward(
		mut self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		while self.0 {
			if let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
				self.0 = self.0 && !item;
			} else {
				break;
			}
		}
		Poll::Ready(())
	}
	fn poll_output(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
		Poll::Ready(!self.0)
	}
}
