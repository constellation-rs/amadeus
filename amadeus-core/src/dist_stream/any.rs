use futures::{ready, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedReducer, DistributedStreamMulti, ReduceFactory, Reducer, ReducerAsync, ReducerProcessSend, ReducerSend
};
use crate::pool::ProcessSend;

#[must_use]
pub struct Any<I, F> {
	i: I,
	f: F,
}
impl<I, F> Any<I, F> {
	pub(super) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedStreamMulti<Source>, Source, F> DistributedReducer<I, Source, bool> for Any<I, F>
where
	F: FnMut(I::Item) -> bool + Clone + ProcessSend,
	I::Item: 'static,
{
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

#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "F: Serialize"),
	bound(deserialize = "F: Deserialize<'de>")
)]
pub struct AnyReducerFactory<A, F>(F, PhantomData<fn(A)>);
impl<A, F> ReduceFactory for AnyReducerFactory<A, F>
where
	F: FnMut(A) -> bool + Clone,
{
	type Reducer = AnyReducer<A, F>;
	fn make(&self) -> Self::Reducer {
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
pub struct AnyReducer<A, F>(F, bool, PhantomData<fn(A)>);

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
	A: 'static,
	F: FnMut(A) -> bool + ProcessSend,
{
	type Output = bool;
}
impl<A, F> ReducerSend for AnyReducer<A, F>
where
	A: 'static,
	F: FnMut(A) -> bool + Send + 'static,
{
	type Output = bool;
}

#[derive(Serialize, Deserialize)]
pub struct BoolOrReducerFactory;
impl ReduceFactory for BoolOrReducerFactory {
	type Reducer = BoolOrReducer;
	fn make(&self) -> Self::Reducer {
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
