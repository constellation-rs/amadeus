use derive_new::new;
use educe::Educe;
use futures::{ready, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	future::Future, marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedPipe, DistributedSink, ParallelPipe, ParallelSink, Reducer, ReducerAsync, ReducerProcessSend, ReducerSend
};
use crate::{pipe::Sink, pool::ProcessSend};

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

impl<A, F> Reducer<A> for AnyReducer<A, F>
where
	F: FnMut(A) -> bool,
{
	type Output = bool;
	type Async = AnyReducerAsync<A, F>;

	fn into_async(self) -> Self::Async {
		AnyReducerAsync(self.0, true, PhantomData)
	}
}
impl<A, F> ReducerProcessSend<A> for AnyReducer<A, F>
where
	F: FnMut(A) -> bool,
{
	type Output = bool;
}
impl<A, F> ReducerSend<A> for AnyReducer<A, F>
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

impl<A, F> Sink<A> for AnyReducerAsync<A, F>
where
	F: FnMut(A) -> bool,
{
	#[inline(always)]
	fn poll_pipe(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = A>>,
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
}
impl<A, F> ReducerAsync<A> for AnyReducerAsync<A, F>
where
	F: FnMut(A) -> bool,
{
	type Output = bool;

	fn output<'a>(self: Pin<&'a mut Self>) -> Pin<Box<dyn Future<Output = Self::Output> + 'a>> {
		Box::pin(async move { !self.1 })
	}
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BoolOrReducer;

impl Reducer<bool> for BoolOrReducer {
	type Output = bool;
	type Async = BoolOrReducerAsync;

	fn into_async(self) -> Self::Async {
		BoolOrReducerAsync(true)
	}
}
impl ReducerProcessSend<bool> for BoolOrReducer {
	type Output = bool;
}
impl ReducerSend<bool> for BoolOrReducer {
	type Output = bool;
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct BoolOrReducerAsync(bool);

impl Sink<bool> for BoolOrReducerAsync {
	#[inline(always)]
	fn poll_pipe(
		mut self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = bool>>,
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
}
impl ReducerAsync<bool> for BoolOrReducerAsync {
	type Output = bool;

	fn output<'a>(self: Pin<&'a mut Self>) -> Pin<Box<dyn Future<Output = Self::Output> + 'a>> {
		Box::pin(async move { !self.0 })
	}
}
