use futures::{ready, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	iter, marker::PhantomData, mem, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedPipe, DistributedSink, Factory, Reducer, ReducerAsync, ReducerProcessSend, ReducerSend
};
use crate::pool::ProcessSend;

#[must_use]
pub struct Sum<I, B> {
	i: I,
	marker: PhantomData<fn() -> B>,
}
impl<I, B> Sum<I, B> {
	pub(crate) fn new(i: I) -> Self {
		Self {
			i,
			marker: PhantomData,
		}
	}
}

impl<I: DistributedPipe<Source>, B, Source> DistributedSink<I, Source, B> for Sum<I, B>
where
	B: iter::Sum<I::Item> + iter::Sum<B> + ProcessSend,
	I::Item: 'static,
{
	type ReduceAFactory = SumReduceFactory<I::Item, B>;
	type ReduceBFactory = SumReduceFactory<B, B>;
	type ReduceA = SumReducer<I::Item, B>;
	type ReduceB = SumReducer<B, B>;
	type ReduceC = SumReducer<B, B>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceBFactory, Self::ReduceC) {
		(
			self.i,
			SumReduceFactory(PhantomData),
			SumReduceFactory::new(),
			SumReducer(Some(iter::empty::<B>().sum()), PhantomData),
		)
	}
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct SumReduceFactory<A, B>(PhantomData<fn(A, B)>);
impl<A, B> SumReduceFactory<A, B> {
	pub fn new() -> Self {
		Self(PhantomData)
	}
}
impl<A, B> Default for SumReduceFactory<A, B> {
	fn default() -> Self {
		Self(PhantomData)
	}
}
impl<A, B> Factory for SumReduceFactory<A, B>
where
	B: iter::Sum<A> + iter::Sum,
{
	type Item = SumReducer<A, B>;
	fn make(&self) -> Self::Item {
		SumReducer(Some(iter::empty::<B>().sum()), PhantomData)
	}
}
impl<A, B> Clone for SumReduceFactory<A, B> {
	fn clone(&self) -> Self {
		Self(PhantomData)
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "B: Serialize"),
	bound(deserialize = "B: Deserialize<'de>")
)]
pub struct SumReducer<A, B>(Option<B>, PhantomData<fn(A)>);
impl<A, B> SumReducer<A, B> {
	pub(crate) fn new(b: B) -> Self {
		Self(Some(b), PhantomData)
	}
}
impl<A, B> Reducer for SumReducer<A, B>
where
	B: iter::Sum<A> + iter::Sum,
{
	type Item = A;
	type Output = B;
	type Async = Self;

	fn into_async(self) -> Self::Async {
		self
	}
}
impl<A, B> ReducerAsync for SumReducer<A, B>
where
	B: iter::Sum<A> + iter::Sum,
{
	type Item = A;
	type Output = B;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		let self_0 = self_.0.as_mut().unwrap();
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			*self_0 = iter::once(mem::replace(self_0, iter::empty::<A>().sum()))
				.chain(iter::once(iter::once(item).sum()))
				.sum();
		}
		Poll::Ready(())
	}
	fn poll_output(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
		Poll::Ready(self.project().0.take().unwrap())
	}
}
impl<A, B> ReducerProcessSend for SumReducer<A, B>
where
	A: 'static,
	B: iter::Sum<A> + iter::Sum + ProcessSend,
{
	type Output = B;
}
impl<A, B> ReducerSend for SumReducer<A, B>
where
	A: 'static,
	B: iter::Sum<A> + iter::Sum + Send + 'static,
{
	type Output = B;
}
