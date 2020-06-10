use futures::{ready, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedReducer, DistributedStreamMulti, ReduceFactory, Reducer, ReducerAsync, ReducerProcessSend, ReducerSend, SumReducer, SumReducerFactory
};

#[must_use]
pub struct Count<I> {
	i: I,
}
impl<I> Count<I> {
	pub(super) fn new(i: I) -> Self {
		Self { i }
	}
}

impl<I: DistributedStreamMulti<Source>, Source> DistributedReducer<I, Source, usize> for Count<I>
where
	I::Item: 'static,
{
	type ReduceAFactory = CountReducerFactory<I::Item>;
	type ReduceBFactory = SumReducerFactory<usize, usize>;
	type ReduceA = CountReducer<I::Item>;
	type ReduceB = SumReducer<usize, usize>;
	type ReduceC = SumReducer<usize, usize>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceBFactory, Self::ReduceC) {
		(
			self.i,
			CountReducerFactory(PhantomData),
			SumReducerFactory::new(),
			SumReducer::new(0),
		)
	}
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct CountReducerFactory<A>(PhantomData<fn(A)>);
impl<A> ReduceFactory for CountReducerFactory<A> {
	type Reducer = CountReducer<A>;
	fn make(&self) -> Self::Reducer {
		CountReducer(0, PhantomData)
	}
}
impl<A> Clone for CountReducerFactory<A> {
	fn clone(&self) -> Self {
		Self(PhantomData)
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct CountReducer<A>(usize, PhantomData<fn(A)>);

impl<A> Reducer for CountReducer<A> {
	type Item = A;
	type Output = usize;
	type Async = Self;

	fn into_async(self) -> Self::Async {
		self
	}
}
impl<A> ReducerAsync for CountReducer<A> {
	type Item = A;
	type Output = usize;

	#[inline(always)]
	fn poll_forward(
		mut self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		while let Some(_item) = ready!(stream.as_mut().poll_next(cx)) {
			self.0 += 1;
		}
		Poll::Ready(())
	}
	fn poll_output(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
		Poll::Ready(self.0)
	}
}
impl<A> ReducerProcessSend for CountReducer<A>
where
	A: 'static,
{
	type Output = usize;
}
impl<A> ReducerSend for CountReducer<A>
where
	A: 'static,
{
	type Output = usize;
}
