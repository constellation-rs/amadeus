use super::{DistributedIteratorMulti, DistributedReducer, ReduceFactory, Reducer};
use serde::{de::Deserialize, ser::Serialize};
use std::marker;

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

impl<I: DistributedIteratorMulti<Source>, Source, F> DistributedReducer<I, Source, bool>
	for Any<I, F>
where
	F: FnMut(I::Item) -> bool + Clone,
{
	type ReduceAFactory = AnyReducerFactory<I::Item, F>;
	type ReduceA = AnyReducer<I::Item, F>;
	type ReduceB = BoolOrReducer;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			AnyReducerFactory(self.f, marker::PhantomData),
			BoolOrReducer(false),
		)
	}
}

pub struct AnyReducerFactory<A, F>(F, marker::PhantomData<fn(A)>);

impl<A, F> ReduceFactory for AnyReducerFactory<A, F>
where
	F: FnMut(A) -> bool + Clone,
{
	type Reducer = AnyReducer<A, F>;
	fn make(&self) -> Self::Reducer {
		AnyReducer(self.0.clone(), true, marker::PhantomData)
	}
}

#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "F: Serialize"),
	bound(deserialize = "F: Deserialize<'de>")
)]
pub struct AnyReducer<A, F>(F, bool, marker::PhantomData<fn(A)>);

impl<A, F> Reducer for AnyReducer<A, F>
where
	F: FnMut(A) -> bool,
{
	type Item = A;
	type Output = bool;

	#[inline(always)]
	fn push(&mut self, item: Self::Item) -> bool {
		self.1 = self.1 && !self.0(item);
		self.1
	}
	fn ret(self) -> Self::Output {
		!self.1
	}
}

#[derive(Serialize, Deserialize)]
pub struct BoolOrReducer(bool);

impl Reducer for BoolOrReducer {
	type Item = bool;
	type Output = bool;

	#[inline(always)]
	fn push(&mut self, item: Self::Item) -> bool {
		self.0 = self.0 || item;
		self.0
	}
	fn ret(self) -> Self::Output {
		self.0
	}
}
