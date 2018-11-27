use super::{DistributedIteratorMulti, DistributedReducer, ReduceFactory, Reducer};
use serde::{de::Deserialize, ser::Serialize};
use std::{iter, marker, mem};

#[must_use]
pub struct All<I, F> {
	i: I,
	f: F,
}
impl<I, F> All<I, F> {
	pub(super) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedIteratorMulti<Source>, Source, F> DistributedReducer<I, Source, bool>
	for All<I, F>
where
	F: FnMut(I::Item) -> bool + Clone,
{
	type ReduceAFactory = AllReducerFactory<I::Item, F>;
	type ReduceA = AllReducer<I::Item, F>;
	type ReduceB = BoolSumReducer;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			AllReducerFactory(self.f, marker::PhantomData),
			BoolSumReducer(true),
		)
	}
}

pub struct AllReducerFactory<A, F>(F, marker::PhantomData<fn(A)>);

impl<A, F> ReduceFactory for AllReducerFactory<A, F>
where
	F: FnMut(A) -> bool + Clone,
{
	type Reducer = AllReducer<A, F>;
	fn make(&self) -> Self::Reducer {
		AllReducer(self.0.clone(), false, marker::PhantomData)
	}
}

#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "F: Serialize"),
	bound(deserialize = "F: Deserialize<'de>")
)]
pub struct AllReducer<A, F>(F, bool, marker::PhantomData<fn(A)>);

impl<A, F> Reducer for AllReducer<A, F>
where
	F: FnMut(A) -> bool,
{
	type Item = A;
	type Output = bool;

	#[inline(always)]
	fn push(&mut self, item: Self::Item) {
		if !self.1 {
			self.1 = !self.0(item);
		}
	}
	fn ret(self) -> Self::Output {
		!self.1
	}
}

#[derive(Serialize, Deserialize)]
pub struct BoolSumReducer(bool);

impl Reducer for BoolSumReducer where {
	type Item = bool;
	type Output = bool;

	#[inline(always)]
	fn push(&mut self, item: Self::Item) {
		self.0 = self.0 && item;
	}
	fn ret(self) -> Self::Output {
		self.0
	}
}
