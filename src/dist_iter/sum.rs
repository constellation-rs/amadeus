use super::{DistributedIteratorMulti, DistributedReducer, ReduceFactory, Reducer};
use serde::{de::Deserialize, ser::Serialize};
use std::{iter, marker, mem};

#[must_use]
pub struct Sum<I, B> {
	i: I,
	b: marker::PhantomData<fn() -> B>,
}
impl<I, B> Sum<I, B> {
	pub(super) fn new(i: I) -> Self {
		Self {
			i,
			b: marker::PhantomData,
		}
	}
}

impl<I: DistributedIteratorMulti<Source>, B, Source> DistributedReducer<I, Source, B> for Sum<I, B>
where
	B: iter::Sum<I::Item> + iter::Sum<B>,
{
	type ReduceAFactory = SumReducerFactory<I::Item, B>;
	type ReduceA = SumReducer<I::Item, B>;
	type ReduceB = SumReducer<B, B>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			SumReducerFactory(marker::PhantomData),
			SumReducer(iter::empty::<B>().sum(), marker::PhantomData),
		)
	}
}

pub struct SumReducerFactory<A, B>(marker::PhantomData<fn(A, B)>);

impl<A, B> ReduceFactory for SumReducerFactory<A, B>
where
	B: iter::Sum<A> + iter::Sum,
{
	type Reducer = SumReducer<A, B>;
	fn make(&self) -> Self::Reducer {
		SumReducer(iter::empty::<B>().sum(), marker::PhantomData)
	}
}

#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "B: Serialize"),
	bound(deserialize = "B: Deserialize<'de>")
)]
pub struct SumReducer<A, B>(pub(super) B, pub(super) marker::PhantomData<fn(A)>);

impl<A, B> Reducer for SumReducer<A, B>
where
	B: iter::Sum<A> + iter::Sum,
{
	type Item = A;
	type Output = B;

	#[inline(always)]
	fn push(&mut self, item: Self::Item) {
		self.0 = iter::once(mem::replace(&mut self.0, iter::empty::<A>().sum()))
			.chain(iter::once(iter::once(item).sum()))
			.sum();
	}
	fn ret(self) -> Self::Output {
		self.0
	}
}
