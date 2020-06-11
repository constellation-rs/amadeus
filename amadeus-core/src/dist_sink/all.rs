use futures::{ready, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedPipe, DistributedSink, Factory, Reducer, ReducerAsync, ReducerProcessSend, ReducerSend
};
use crate::pool::ProcessSend;

#[must_use]
pub struct All<I, F> {
	i: I,
	f: F,
}
impl<I, F> All<I, F> {
	pub(crate) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedPipe<Source>, Source, F> DistributedSink<I, Source, bool> for All<I, F>
where
	F: FnMut(I::Item) -> bool + Clone + ProcessSend,
	I::Item: 'static,
{
	type ReduceAFactory = AllReduceFactory<I::Item, F>;
	type ReduceBFactory = BoolAndReduceFactory;
	type ReduceA = AllReducer<I::Item, F>;
	type ReduceB = BoolAndReducer;
	type ReduceC = BoolAndReducer;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceBFactory, Self::ReduceC) {
		(
			self.i,
			AllReduceFactory(self.f, PhantomData),
			BoolAndReduceFactory,
			BoolAndReducer(true),
		)
	}
}

#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "F: Serialize"),
	bound(deserialize = "F: Deserialize<'de>")
)]
pub struct AllReduceFactory<A, F>(F, PhantomData<fn(A)>);
impl<A, F> Factory for AllReduceFactory<A, F>
where
	F: FnMut(A) -> bool + Clone,
{
	type Item = AllReducer<A, F>;
	fn make(&self) -> Self::Item {
		AllReducer(self.0.clone(), true, PhantomData)
	}
}
impl<A, F> Clone for AllReduceFactory<A, F>
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
pub struct AllReducer<A, F>(F, bool, PhantomData<fn(A)>);

impl<A, F> Reducer for AllReducer<A, F>
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
impl<A, F> ReducerAsync for AllReducer<A, F>
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
impl<A, F> ReducerProcessSend for AllReducer<A, F>
where
	A: 'static,
	F: FnMut(A) -> bool + ProcessSend,
{
	type Output = bool;
}
impl<A, F> ReducerSend for AllReducer<A, F>
where
	A: 'static,
	F: FnMut(A) -> bool + Send + 'static,
{
	type Output = bool;
}

#[derive(Serialize, Deserialize)]
pub struct BoolAndReduceFactory;
impl Factory for BoolAndReduceFactory {
	type Item = BoolAndReducer;
	fn make(&self) -> Self::Item {
		BoolAndReducer(true)
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct BoolAndReducer(bool);

impl Reducer for BoolAndReducer {
	type Item = bool;
	type Output = bool;
	type Async = Self;
	fn into_async(self) -> Self::Async {
		self
	}
}
impl ReducerAsync for BoolAndReducer {
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
impl ReducerProcessSend for BoolAndReducer {
	type Output = bool;
}
impl ReducerSend for BoolAndReducer {
	type Output = bool;
}
