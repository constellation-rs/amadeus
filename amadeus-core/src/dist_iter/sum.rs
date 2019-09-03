use serde::{Deserialize, Serialize};
use std::{iter, marker::PhantomData, mem};

use super::{DistributedIteratorMulti, DistributedReducer, ReduceFactory, Reducer, ReducerA};
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
			SumReducer(iter::empty::<B>().sum(), PhantomData),
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
		SumReducer(iter::empty::<B>().sum(), PhantomData)
	}
}

#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "B: Serialize"),
	bound(deserialize = "B: Deserialize<'de>")
)]
pub struct SumReducer<A, B>(pub(super) B, pub(super) PhantomData<fn(A)>);

impl<A, B> Reducer for SumReducer<A, B>
where
	B: iter::Sum<A> + iter::Sum,
{
	type Item = A;
	type Output = B;

	#[inline(always)]
	fn push(&mut self, item: Self::Item) -> bool {
		self.0 = iter::once(mem::replace(&mut self.0, iter::empty::<A>().sum()))
			.chain(iter::once(iter::once(item).sum()))
			.sum();
		true
	}
	fn ret(self) -> Self::Output {
		self.0
	}
}
impl<A, B> ReducerA for SumReducer<A, B>
where
	A: 'static,
	B: iter::Sum<A> + iter::Sum + ProcessSend,
{
	type Output = B;
}
