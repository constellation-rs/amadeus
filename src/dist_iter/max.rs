use super::{
	CombineReducer, CombineReducerFactory, Combiner, DistributedIteratorMulti, DistributedReducer
};
use serde::{de::Deserialize, ser::Serialize};
use std::{cmp::Ordering, marker::PhantomData};

#[must_use]
pub struct Max<I> {
	i: I,
}
impl<I> Max<I> {
	pub(super) fn new(i: I) -> Self {
		Self { i }
	}
}

impl<I: DistributedIteratorMulti<Source>, Source> DistributedReducer<I, Source, Option<I::Item>>
	for Max<I>
where
	I::Item: Ord,
{
	type ReduceAFactory = CombineReducerFactory<I::Item, I::Item, combine::Max>;
	type ReduceA = CombineReducer<I::Item, I::Item, combine::Max>;
	type ReduceB = CombineReducer<Option<I::Item>, I::Item, combine::Max>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			CombineReducerFactory(combine::Max, PhantomData),
			CombineReducer(None, combine::Max, PhantomData),
		)
	}
}

#[must_use]
pub struct MaxBy<I, F> {
	i: I,
	f: F,
}
impl<I, F> MaxBy<I, F> {
	pub(super) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedIteratorMulti<Source>, Source, F> DistributedReducer<I, Source, Option<I::Item>>
	for MaxBy<I, F>
where
	F: FnMut(&I::Item, &I::Item) -> Ordering + Clone,
{
	type ReduceAFactory = CombineReducerFactory<I::Item, I::Item, combine::MaxBy<F>>;
	type ReduceA = CombineReducer<I::Item, I::Item, combine::MaxBy<F>>;
	type ReduceB = CombineReducer<Option<I::Item>, I::Item, combine::MaxBy<F>>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			CombineReducerFactory(combine::MaxBy(self.f.clone()), PhantomData),
			CombineReducer(None, combine::MaxBy(self.f), PhantomData),
		)
	}
}

#[must_use]
pub struct MaxByKey<I, F> {
	i: I,
	f: F,
}
impl<I, F> MaxByKey<I, F> {
	pub(super) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedIteratorMulti<Source>, Source, F, B>
	DistributedReducer<I, Source, Option<I::Item>> for MaxByKey<I, F>
where
	F: FnMut(&I::Item) -> B + Clone,
	B: Ord,
{
	type ReduceAFactory = CombineReducerFactory<I::Item, I::Item, combine::MaxByKey<F, B>>;
	type ReduceA = CombineReducer<I::Item, I::Item, combine::MaxByKey<F, B>>;
	type ReduceB = CombineReducer<Option<I::Item>, I::Item, combine::MaxByKey<F, B>>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			CombineReducerFactory(combine::MaxByKey(self.f.clone(), PhantomData), PhantomData),
			CombineReducer(None, combine::MaxByKey(self.f, PhantomData), PhantomData),
		)
	}
}

#[must_use]
pub struct Min<I> {
	i: I,
}
impl<I> Min<I> {
	pub(super) fn new(i: I) -> Self {
		Self { i }
	}
}

impl<I: DistributedIteratorMulti<Source>, Source> DistributedReducer<I, Source, Option<I::Item>>
	for Min<I>
where
	I::Item: Ord,
{
	type ReduceAFactory = CombineReducerFactory<I::Item, I::Item, combine::Min>;
	type ReduceA = CombineReducer<I::Item, I::Item, combine::Min>;
	type ReduceB = CombineReducer<Option<I::Item>, I::Item, combine::Min>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			CombineReducerFactory(combine::Min, PhantomData),
			CombineReducer(None, combine::Min, PhantomData),
		)
	}
}

#[must_use]
pub struct MinBy<I, F> {
	i: I,
	f: F,
}
impl<I, F> MinBy<I, F> {
	pub(super) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedIteratorMulti<Source>, Source, F> DistributedReducer<I, Source, Option<I::Item>>
	for MinBy<I, F>
where
	F: FnMut(&I::Item, &I::Item) -> Ordering + Clone,
{
	type ReduceAFactory = CombineReducerFactory<I::Item, I::Item, combine::MinBy<F>>;
	type ReduceA = CombineReducer<I::Item, I::Item, combine::MinBy<F>>;
	type ReduceB = CombineReducer<Option<I::Item>, I::Item, combine::MinBy<F>>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			CombineReducerFactory(combine::MinBy(self.f.clone()), PhantomData),
			CombineReducer(None, combine::MinBy(self.f), PhantomData),
		)
	}
}

#[must_use]
pub struct MinByKey<I, F> {
	i: I,
	f: F,
}
impl<I, F> MinByKey<I, F> {
	pub(super) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedIteratorMulti<Source>, Source, F, B>
	DistributedReducer<I, Source, Option<I::Item>> for MinByKey<I, F>
where
	F: FnMut(&I::Item) -> B + Clone,
	B: Ord,
{
	type ReduceAFactory = CombineReducerFactory<I::Item, I::Item, combine::MinByKey<F, B>>;
	type ReduceA = CombineReducer<I::Item, I::Item, combine::MinByKey<F, B>>;
	type ReduceB = CombineReducer<Option<I::Item>, I::Item, combine::MinByKey<F, B>>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			CombineReducerFactory(combine::MinByKey(self.f.clone(), PhantomData), PhantomData),
			CombineReducer(None, combine::MinByKey(self.f, PhantomData), PhantomData),
		)
	}
}

mod combine {
	use super::*;

	#[derive(Copy, Clone, Serialize, Deserialize)]
	pub struct Max;
	impl<A: Ord> Combiner<A> for Max {
		fn combine(&mut self, a: A, b: A) -> A {
			// switch to b even if it is only equal, to preserve stability.
			if a.cmp(&b) != Ordering::Greater {
				b
			} else {
				a
			}
		}
	}

	#[derive(Copy, Clone, Serialize, Deserialize)]
	pub struct MaxBy<F>(pub F);
	impl<A, F: FnMut(&A, &A) -> Ordering> Combiner<A> for MaxBy<F> {
		fn combine(&mut self, a: A, b: A) -> A {
			if self.0(&a, &b) != Ordering::Greater {
				b
			} else {
				a
			}
		}
	}

	#[derive(Serialize, Deserialize)]
	#[serde(
		bound(serialize = "F: Serialize"),
		bound(deserialize = "F: Deserialize<'de>")
	)]
	pub struct MaxByKey<F, B>(pub F, pub PhantomData<fn(B)>);
	impl<F: Clone, B> Clone for MaxByKey<F, B> {
		fn clone(&self) -> Self {
			MaxByKey(self.0.clone(), PhantomData)
		}
	}
	impl<F: Copy, B> Copy for MaxByKey<F, B> {}
	impl<A, F: FnMut(&A) -> B, B: Ord> Combiner<A> for MaxByKey<F, B> {
		fn combine(&mut self, a: A, b: A) -> A {
			if self.0(&a).cmp(&self.0(&b)) != Ordering::Greater {
				b
			} else {
				a
			}
		}
	}

	#[derive(Copy, Clone, Serialize, Deserialize)]
	pub struct Min;
	impl<A: Ord> Combiner<A> for Min {
		fn combine(&mut self, a: A, b: A) -> A {
			// switch to b even if it is strictly smaller, to preserve stability.
			if a.cmp(&b) == Ordering::Greater {
				b
			} else {
				a
			}
		}
	}

	#[derive(Copy, Clone, Serialize, Deserialize)]
	pub struct MinBy<F>(pub F);
	impl<A, F: FnMut(&A, &A) -> Ordering> Combiner<A> for MinBy<F> {
		fn combine(&mut self, a: A, b: A) -> A {
			if self.0(&a, &b) == Ordering::Greater {
				b
			} else {
				a
			}
		}
	}

	#[derive(Serialize, Deserialize)]
	#[serde(
		bound(serialize = "F: Serialize"),
		bound(deserialize = "F: Deserialize<'de>")
	)]
	pub struct MinByKey<F, B>(pub F, pub PhantomData<fn(B)>);
	impl<F: Clone, B> Clone for MinByKey<F, B> {
		fn clone(&self) -> Self {
			MinByKey(self.0.clone(), PhantomData)
		}
	}
	impl<F: Copy, B> Copy for MinByKey<F, B> {}
	impl<A, F: FnMut(&A) -> B, B: Ord> Combiner<A> for MinByKey<F, B> {
		fn combine(&mut self, a: A, b: A) -> A {
			if self.0(&a).cmp(&self.0(&b)) == Ordering::Greater {
				b
			} else {
				a
			}
		}
	}
}
