use derive_new::new;
use educe::Educe;
use serde::{Deserialize, Serialize};
use serde_closure::traits::FnMut;
use std::{cmp::Ordering, marker::PhantomData};

use super::{combiner_par_sink, CombinerSync, FolderSyncReducer, ParallelPipe, ParallelSink};

#[derive(new)]
#[must_use]
pub struct Max<P> {
	pipe: P,
}
impl_par_dist! {
	impl<P: ParallelPipe<Item>, Item> ParallelSink<Item> for Max<P>
	where
		P::Output: Ord + Send,
	{
		combiner_par_sink!(combine::Max<P::Output>, self, combine::Max::new());
	}
}

#[derive(new)]
#[must_use]
pub struct MaxBy<P, F> {
	pipe: P,
	f: F,
}
impl_par_dist! {
	impl<P: ParallelPipe<Item>, Item, F> ParallelSink<Item> for MaxBy<P, F>
	where
		F: for<'a, 'b> FnMut<(&'a P::Output, &'b P::Output), Output = Ordering>
			+ Clone
			+ Send,
		P::Output: Send,
	{
		combiner_par_sink!(combine::MaxBy<P::Output,F>, self, combine::MaxBy::new(self.f));
	}
}

#[derive(new)]
#[must_use]
pub struct MaxByKey<P, F> {
	pipe: P,
	f: F,
}
impl_par_dist! {
	impl<P: ParallelPipe<Item>, Item, F, B> ParallelSink<Item> for MaxByKey<P, F>
	where
		F: for<'a> FnMut<(&'a P::Output,), Output = B> + Clone + Send,
		B: Ord,
		P::Output: Send,
	{
		combiner_par_sink!(combine::MaxByKey<P::Output,F, B>, self, combine::MaxByKey::new(self.f));
	}
}

#[derive(new)]
#[must_use]
pub struct Min<P> {
	pipe: P,
}
impl_par_dist! {
	impl<P: ParallelPipe<Item>, Item> ParallelSink<Item> for Min<P>
	where
		P::Output: Ord + Send,
	{
		combiner_par_sink!(combine::Min<P::Output>, self, combine::Min::new());
	}
}

#[derive(new)]
#[must_use]
pub struct MinBy<P, F> {
	pipe: P,
	f: F,
}
impl_par_dist! {
	impl<P: ParallelPipe<Item>, Item, F> ParallelSink<Item> for MinBy<P, F>
	where
		F: for<'a, 'b> FnMut<(&'a P::Output, &'b P::Output), Output = Ordering>
			+ Clone
			+ Send,
		P::Output: Send,
	{
		combiner_par_sink!(combine::MinBy<P::Output,F>, self, combine::MinBy::new(self.f));
	}
}

#[derive(new)]
#[must_use]
pub struct MinByKey<P, F> {
	pipe: P,
	f: F,
}
impl_par_dist! {
	impl<P: ParallelPipe<Item>, Item, F, B> ParallelSink<Item> for MinByKey<P, F>
	where
		F: for<'a> FnMut<(&'a P::Output,), Output = B> + Clone + Send,
		B: Ord,
		P::Output: Send,
	{
		combiner_par_sink!(combine::MinByKey<P::Output,F, B>, self, combine::MinByKey::new(self.f));
	}
}

mod combine {
	use super::*;

	#[derive(Educe, Serialize, Deserialize, new)]
	#[educe(Clone)]
	#[serde(bound = "")]
	pub struct Max<A>(PhantomData<fn() -> A>);
	impl<A: Ord> CombinerSync for Max<A> {
		type Done = A;

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
		type Done = A;

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
		type Done = A;

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
		type Done = A;

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
		type Done = A;

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
		type Done = A;

		fn combine(&mut self, a: A, b: A) -> A {
			if self.0.call_mut((&a,)).cmp(&self.0.call_mut((&b,))) == Ordering::Greater {
				b
			} else {
				a
			}
		}
	}
}
