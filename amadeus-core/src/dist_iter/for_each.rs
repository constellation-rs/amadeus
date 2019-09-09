use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use super::{
	DistributedIteratorMulti, DistributedReducer, PushReducer, ReduceFactory, Reducer, ReducerA
};
use crate::pool::ProcessSend;

#[must_use]
pub struct ForEach<I, F> {
	i: I,
	f: F,
}
impl<I, F> ForEach<I, F> {
	pub(super) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedIteratorMulti<Source>, Source, F> DistributedReducer<I, Source, ()>
	for ForEach<I, F>
where
	F: FnMut(I::Item) + Clone + ProcessSend,
	I::Item: 'static,
{
	type ReduceAFactory = ForEachReducerFactory<I::Item, F>;
	type ReduceA = ForEachReducer<I::Item, F>;
	type ReduceB = PushReducer<()>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			ForEachReducerFactory(self.f, PhantomData),
			PushReducer((), PhantomData),
		)
	}
}

pub struct ForEachReducerFactory<A, F>(F, PhantomData<fn(A)>);

impl<A, F> ReduceFactory for ForEachReducerFactory<A, F>
where
	F: FnMut(A) + Clone,
{
	type Reducer = ForEachReducer<A, F>;
	fn make(&self) -> Self::Reducer {
		ForEachReducer(self.0.clone(), PhantomData)
	}
}

#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "F: Serialize"),
	bound(deserialize = "F: Deserialize<'de>")
)]
pub struct ForEachReducer<A, F>(F, PhantomData<fn(A)>);

impl<A, F> Reducer for ForEachReducer<A, F>
where
	F: FnMut(A) + Clone,
{
	type Item = A;
	type Output = ();

	#[inline(always)]
	fn push(&mut self, item: Self::Item) -> bool {
		self.0(item);
		true
	}
	fn ret(self) -> Self::Output {}
}
impl<A, F> ReducerA for ForEachReducer<A, F>
where
	A: 'static,
	F: FnMut(A) + Clone + ProcessSend,
{
	type Output = ();
}
