use derive_new::new;
use futures::{ready, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedPipe, DistributedSink, Factory, ParallelPipe, ParallelSink, Reducer, ReducerAsync, ReducerProcessSend, ReducerSend
};
use crate::pool::ProcessSend;

#[derive(new)]
#[must_use]
pub struct Any<I, F> {
	i: I,
	f: F,
}

impl<I: DistributedPipe<Source>, Source, F> DistributedSink<Source> for Any<I, F>
where
	F: FnMut(I::Item) -> bool + Clone + ProcessSend + 'static,
{
	type Output = bool;
	type Pipe = I;
	type ReduceAFactory = AnyReducerFactory<I::Item, F>;
	type ReduceBFactory = BoolOrReducerFactory;
	type ReduceA = AnyReducer<I::Item, F>;
	type ReduceB = BoolOrReducer;
	type ReduceC = BoolOrReducer;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceBFactory, Self::ReduceC) {
		(
			self.i,
			AnyReducerFactory(self.f, PhantomData),
			BoolOrReducerFactory,
			BoolOrReducer(true),
		)
	}
}
impl<I: ParallelPipe<Source>, Source, F> ParallelSink<Source> for Any<I, F>
where
	F: FnMut(I::Item) -> bool + Clone + Send + 'static,
{
	type Output = bool;
	type Pipe = I;
	type ReduceAFactory = AnyReducerFactory<I::Item, F>;
	type ReduceA = AnyReducer<I::Item, F>;
	type ReduceC = BoolOrReducer;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceC) {
		(
			self.i,
			AnyReducerFactory(self.f, PhantomData),
			BoolOrReducer(true),
		)
	}
}

#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "F: Serialize"),
	bound(deserialize = "F: Deserialize<'de>")
)]
pub struct AnyReducerFactory<A, F>(F, PhantomData<fn() -> A>);
impl<A, F> Factory for AnyReducerFactory<A, F>
where
	F: FnMut(A) -> bool + Clone,
{
	type Item = AnyReducer<A, F>;
	fn make(&self) -> Self::Item {
		AnyReducer(self.0.clone(), true, PhantomData)
	}
}
impl<A, F> Clone for AnyReducerFactory<A, F>
where
	F: Clone,
{
	fn clone(&self) -> Self {
		Self(self.0.clone(), PhantomData)
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "F: Serialize"),
	bound(deserialize = "F: Deserialize<'de>")
)]
pub struct AnyReducer<A, F>(F, bool, PhantomData<fn() -> A>);

impl<A, F> Reducer for AnyReducer<A, F>
where
	F: FnMut(A) -> bool,
{
	type Item = A;
	type Output = bool;
	type Async = Self;
	fn into_async(self) -> Self::Async {
		self
	}
}
impl<A, F> ReducerAsync for AnyReducer<A, F>
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

#[derive(Serialize, Deserialize)]
pub struct BoolOrReducerFactory;
impl Factory for BoolOrReducerFactory {
	type Item = BoolOrReducer;
	fn make(&self) -> Self::Item {
		BoolOrReducer(true)
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct BoolOrReducer(bool);

impl Reducer for BoolOrReducer {
	type Item = bool;
	type Output = bool;
	type Async = Self;
	fn into_async(self) -> Self::Async {
		self
	}
}
impl ReducerAsync for BoolOrReducer {
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
impl ReducerProcessSend for BoolOrReducer {
	type Output = bool;
}
impl ReducerSend for BoolOrReducer {
	type Output = bool;
}
