use derive_new::new;
use educe::Educe;
use serde::{Deserialize, Serialize};
use serde_closure::traits::FnMut;
use std::{cmp::Ordering, marker::PhantomData};

use super::{combiner_par_sink, CombinerSync, FolderSyncReducer, ParallelPipe, ParallelSink};

#[derive(new)]
#[must_use]
pub struct Max<I> {
	i: I,
}
impl_par_dist! {
	impl<I: ParallelPipe<Source>, Source> ParallelSink<Source> for Max<I>
	where
		I::Item: Ord + Send + 'static,
	{
		combiner_par_sink!(combine::Max<I::Item>, self, combine::Max::new());
	}
}

#[derive(new)]
#[must_use]
pub struct MaxBy<I, F> {
	i: I,
	f: F,
}
impl_par_dist! {
	impl<I: ParallelPipe<Source>, Source, F> ParallelSink<Source>
		for MaxBy<I, F>
	where
		F: for<'a,'b> FnMut<(&'a I::Item, &'b I::Item,), Output = Ordering> + Clone + Send + 'static,
		I::Item: Send + 'static,
	{
		combiner_par_sink!(combine::MaxBy<I::Item,F>, self, combine::MaxBy::new(self.f));
	}
}

#[derive(new)]
#[must_use]
pub struct MaxByKey<I, F> {
	i: I,
	f: F,
}
impl_par_dist! {
	impl<I: ParallelPipe<Source>, Source, F, B> ParallelSink<Source>
		for MaxByKey<I, F>
	where
		F: for<'a> FnMut<(&'a I::Item,), Output = B> + Clone + Send + 'static,
		B: Ord + 'static,
		I::Item: Send + 'static,
	{
		combiner_par_sink!(combine::MaxByKey<I::Item,F, B>, self, combine::MaxByKey::new(self.f));
	}
}

#[derive(new)]
#[must_use]
pub struct Min<I> {
	i: I,
}
impl_par_dist! {
	impl<I: ParallelPipe<Source>, Source> ParallelSink<Source> for Min<I>
	where
		I::Item: Ord + Send + 'static,
	{
		combiner_par_sink!(combine::Min<I::Item>, self, combine::Min::new());
	}
}

#[derive(new)]
#[must_use]
pub struct MinBy<I, F> {
	i: I,
	f: F,
}
impl_par_dist! {
	impl<I: ParallelPipe<Source>, Source, F> ParallelSink<Source>
		for MinBy<I, F>
	where
		F: for<'a,'b> FnMut<(&'a I::Item, &'b I::Item,), Output = Ordering> + Clone + Send + 'static,
		I::Item: Send + 'static,
	{
		combiner_par_sink!(combine::MinBy<I::Item,F>, self, combine::MinBy::new(self.f));
	}
}

#[derive(new)]
#[must_use]
pub struct MinByKey<I, F> {
	i: I,
	f: F,
}
impl_par_dist! {
	impl<I: ParallelPipe<Source>, Source, F, B> ParallelSink<Source>
		for MinByKey<I, F>
	where
		F: for<'a> FnMut<(&'a I::Item,), Output = B> + Clone + Send + 'static,
		B: Ord + 'static,
		I::Item: Send + 'static,
	{
		combiner_par_sink!(combine::MinByKey<I::Item,F, B>, self, combine::MinByKey::new(self.f));
	}
}

mod combine {
	use super::*;

	#[derive(Educe, Serialize, Deserialize, new)]
	#[educe(Clone)]
	#[serde(bound = "")]
	pub struct Max<A>(PhantomData<fn() -> A>);
	impl<A: Ord> CombinerSync for Max<A> {
		type Output = A;

		fn combine(&mut self, a: A, b: A) -> A {
			// switch to b even if it is only equal, to preserve stability.
			if a.cmp(&b) != Ordering::Greater {
				b
			} else {
				a
			}
		}
	}

	#[derive(Educe, Serialize, Deserialize, new)]
	#[educe(Clone(bound = "F: Clone"))]
	#[serde(
		bound(serialize = "F: Serialize"),
		bound(deserialize = "F: Deserialize<'de>")
	)]
	pub struct MaxBy<A, F>(pub F, PhantomData<fn() -> A>);
	impl<A, F: for<'a, 'b> FnMut<(&'a A, &'b A), Output = Ordering>> CombinerSync for MaxBy<A, F> {
		type Output = A;

		fn combine(&mut self, a: A, b: A) -> A {
			if self.0.call_mut((&a, &b)) != Ordering::Greater {
				b
			} else {
				a
			}
		}
	}

	#[derive(Educe, Serialize, Deserialize, new)]
	#[educe(Clone(bound = "F: Clone"))]
	#[serde(
		bound(serialize = "F: Serialize"),
		bound(deserialize = "F: Deserialize<'de>")
	)]
	pub struct MaxByKey<A, F, B>(pub F, pub PhantomData<fn(A, B)>);
	impl<A, F: for<'a> FnMut<(&'a A,), Output = B>, B: Ord> CombinerSync for MaxByKey<A, F, B> {
		type Output = A;

		fn combine(&mut self, a: A, b: A) -> A {
			if self.0.call_mut((&a,)).cmp(&self.0.call_mut((&b,))) != Ordering::Greater {
				b
			} else {
				a
			}
		}
	}

	#[derive(Educe, Serialize, Deserialize, new)]
	#[educe(Clone)]
	#[serde(bound = "")]
	pub struct Min<A>(PhantomData<fn() -> A>);
	impl<A: Ord> CombinerSync for Min<A> {
		type Output = A;

		fn combine(&mut self, a: A, b: A) -> A {
			// switch to b even if it is strictly smaller, to preserve stability.
			if a.cmp(&b) == Ordering::Greater {
				b
			} else {
				a
			}
		}
	}

	#[derive(Educe, Serialize, Deserialize, new)]
	#[educe(Clone(bound = "F: Clone"))]
	#[serde(
		bound(serialize = "F: Serialize"),
		bound(deserialize = "F: Deserialize<'de>")
	)]
	pub struct MinBy<A, F>(pub F, PhantomData<fn() -> A>);
	impl<A, F: for<'a, 'b> FnMut<(&'a A, &'b A), Output = Ordering>> CombinerSync for MinBy<A, F> {
		type Output = A;

		fn combine(&mut self, a: A, b: A) -> A {
			if self.0.call_mut((&a, &b)) == Ordering::Greater {
				b
			} else {
				a
			}
		}
	}

	#[derive(Educe, Serialize, Deserialize, new)]
	#[educe(Clone(bound = "F: Clone"))]
	#[serde(
		bound(serialize = "F: Serialize"),
		bound(deserialize = "F: Deserialize<'de>")
	)]
	pub struct MinByKey<A, F, B>(pub F, pub PhantomData<fn(A, B)>);
	impl<A, F: for<'a> FnMut<(&'a A,), Output = B>, B: Ord> CombinerSync for MinByKey<A, F, B> {
		type Output = A;

		fn combine(&mut self, a: A, b: A) -> A {
			if self.0.call_mut((&a,)).cmp(&self.0.call_mut((&b,))) == Ordering::Greater {
				b
			} else {
				a
			}
		}
	}
}
