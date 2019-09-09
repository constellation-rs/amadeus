use replace_with::replace_with_or_abort;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use super::{DistributedIteratorMulti, DistributedReducer, ReduceFactory, Reducer, ReducerA};
use crate::pool::ProcessSend;

#[must_use]
pub struct Combine<I, F> {
	i: I,
	f: F,
}
impl<I, F> Combine<I, F> {
	pub(super) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedIteratorMulti<Source>, Source, F> DistributedReducer<I, Source, Option<I::Item>>
	for Combine<I, F>
where
	F: FnMut(I::Item, I::Item) -> I::Item + Clone + ProcessSend,
	I::Item: ProcessSend,
{
	type ReduceAFactory = CombineReducerFactory<I::Item, I::Item, CombineFn<F>>;
	type ReduceA = CombineReducer<I::Item, I::Item, CombineFn<F>>;
	type ReduceB = CombineReducer<Option<I::Item>, I::Item, CombineFn<F>>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			CombineReducerFactory(CombineFn(self.f.clone()), PhantomData),
			CombineReducer(None, CombineFn(self.f), PhantomData),
		)
	}
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct CombineFn<F>(F);
impl<F, A> Combiner<A> for CombineFn<F>
where
	F: FnMut(A, A) -> A,
{
	fn combine(&mut self, a: A, b: A) -> A {
		self.0(a, b)
	}
}

pub trait Combiner<A> {
	fn combine(&mut self, a: A, b: A) -> A;
}

pub struct CombineReducerFactory<A, B, F>(pub(crate) F, pub(crate) PhantomData<fn(A, B)>);

impl<A, B, F> ReduceFactory for CombineReducerFactory<A, B, F>
where
	Option<B>: From<A>,
	F: Combiner<B> + Clone,
{
	type Reducer = CombineReducer<A, B, F>;
	fn make(&self) -> Self::Reducer {
		CombineReducer(None, self.0.clone(), PhantomData)
	}
}

#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "B: Serialize, F: Serialize"),
	bound(deserialize = "B: Deserialize<'de>, F: Deserialize<'de>")
)]
pub struct CombineReducer<A, B, F>(
	pub(crate) Option<B>,
	pub(crate) F,
	pub(crate) PhantomData<fn(A)>,
);

impl<A, B, F> Reducer for CombineReducer<A, B, F>
where
	Option<B>: From<A>,
	F: Combiner<B>,
{
	type Item = A;
	type Output = Option<B>;

	#[inline(always)]
	fn push(&mut self, item: Self::Item) -> bool {
		let item: Option<B> = item.into();
		let self_1 = &mut self.1;
		if let Some(item) = item {
			replace_with_or_abort(&mut self.0, |self_0| {
				Some(if let Some(cur) = self_0 {
					self_1.combine(cur, item)
				} else {
					item
				})
			});
		}
		true
	}
	fn ret(self) -> Self::Output {
		self.0
	}
}
impl<A, B, F> ReducerA for CombineReducer<A, B, F>
where
	A: 'static,
	Option<B>: From<A>,
	F: Combiner<B> + ProcessSend,
	B: ProcessSend,
{
	type Output = Option<B>;
}
