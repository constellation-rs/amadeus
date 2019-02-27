use serde::{ser::Serialize, Deserialize};

use super::{DistributedIteratorMulti, DistributedReducer, ReduceFactory, Reducer, ReducerA};
use std::marker::PhantomData;

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
	F: FnMut(I::Item) -> bool + Clone + Serialize + for<'de> Deserialize<'de> + 'static,
	I::Item: 'static,
{
	type ReduceAFactory = AllReducerFactory<I::Item, F>;
	type ReduceA = AllReducer<I::Item, F>;
	type ReduceB = BoolAndReducer;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			AllReducerFactory(self.f, PhantomData),
			BoolAndReducer(true),
		)
	}
}

pub struct AllReducerFactory<A, F>(F, PhantomData<fn(A)>);

impl<A, F> ReduceFactory for AllReducerFactory<A, F>
where
	F: FnMut(A) -> bool + Clone,
{
	type Reducer = AllReducer<A, F>;
	fn make(&self) -> Self::Reducer {
		AllReducer(self.0.clone(), true, PhantomData)
	}
}

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

	#[inline(always)]
	fn push(&mut self, item: Self::Item) -> bool {
		self.1 = self.1 && self.0(item);
		self.1
	}
	fn ret(self) -> Self::Output {
		self.1
	}
}
impl<A, F> ReducerA for AllReducer<A, F>
where
	A: 'static,
	F: FnMut(A) -> bool + Serialize + for<'de> Deserialize<'de> + 'static,
{
	type Output = bool;
}

#[derive(Serialize, Deserialize)]
pub struct BoolAndReducer(bool);

impl Reducer for BoolAndReducer {
	type Item = bool;
	type Output = bool;

	#[inline(always)]
	fn push(&mut self, item: Self::Item) -> bool {
		self.0 = self.0 && item;
		self.0
	}
	fn ret(self) -> Self::Output {
		self.0
	}
}
impl ReducerA for BoolAndReducer {
	type Output = bool;
}
