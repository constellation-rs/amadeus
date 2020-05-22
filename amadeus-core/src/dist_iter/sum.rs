use futures::{ready, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	iter, marker::PhantomData, mem, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedIteratorMulti, DistributedReducer, ReduceFactory, Reducer, ReducerA, ReducerAsync
};
use crate::pool::ProcessSend;

#[must_use]
pub struct Sum<I, B> {
	i: I,
	marker: PhantomData<fn() -> B>,
}
impl<I, B> Sum<I, B> {
	pub(super) fn new(i: I) -> Self {
		Self {
			i,
			marker: PhantomData,
		}
	}
}

impl<I: DistributedIteratorMulti<Source>, B, Source> DistributedReducer<I, Source, B> for Sum<I, B>
where
	B: iter::Sum<I::Item> + iter::Sum<B> + ProcessSend,
	I::Item: 'static,
{
	type ReduceAFactory = SumReducerFactory<I::Item, B>;
	type ReduceA = SumReducer<I::Item, B>;
	type ReduceB = SumReducer<B, B>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			SumReducerFactory(PhantomData),
			SumReducer(Some(iter::empty::<B>().sum()), PhantomData),
		)
	}
}

pub struct SumReducerFactory<A, B>(PhantomData<fn(A, B)>);

impl<A, B> ReduceFactory for SumReducerFactory<A, B>
where
	B: iter::Sum<A> + iter::Sum,
{
	type Reducer = SumReducer<A, B>;
	fn make(&self) -> Self::Reducer {
		SumReducer(Some(iter::empty::<B>().sum()), PhantomData)
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
impl<A, B> ReducerA for SumReducer<A, B>
where
	A: 'static,
	B: iter::Sum<A> + iter::Sum + ProcessSend,
{
	type Output = B;
}
