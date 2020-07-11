use either::Either;
use futures::Stream;
use std::{cmp::Ordering, hash::Hash, iter};

use crate::{pipe::Pipe, pool::ProcessSend};

use super::{par_sink::*, par_stream::*};

#[must_use]
pub trait PipeTask<Source> {
	type Item;
	type Async: Pipe<Source, Item = Self::Item>;

	fn into_async(self) -> Self::Async;
}

macro_rules! pipe {
	($pipe:ident $sink:ident $from_sink:ident $send:ident $assert_pipe:ident $assert_sink:ident $($meta:meta)*) => {
		$(#[$meta])*
		#[must_use]
		pub trait $pipe<Source> {
			type Item;
			type Task: PipeTask<Source, Item = Self::Item> + $send;

			fn task(&self) -> Self::Task;

			fn inspect<F>(self, f: F) -> Inspect<Self, F>
			where
				F: FnMut(&Self::Item) + Clone + $send + 'static,
				Self: Sized,
			{
				$assert_pipe(Inspect::new(self, f))
			}

			fn update<F>(self, f: F) -> Update<Self, F>
			where
				F: FnMut(&mut Self::Item) + Clone + $send + 'static,
				Self: Sized,
			{
				$assert_pipe(Update::new(self, f))
			}

			fn map<B, F>(self, f: F) -> Map<Self, F>
			where
				F: FnMut(Self::Item) -> B + Clone + $send + 'static,
				Self: Sized,
			{
				$assert_pipe(Map::new(self, f))
			}

			fn flat_map<B, F>(self, f: F) -> FlatMap<Self, F>
			where
				F: FnMut(Self::Item) -> B + Clone + $send + 'static,
				B: Stream,
				Self: Sized,
			{
				$assert_pipe(FlatMap::new(self, f))
			}

			fn filter<F>(self, f: F) -> Filter<Self, F>
			where
				F: FnMut(&Self::Item) -> bool + Clone + $send + 'static,
				Self: Sized,
			{
				$assert_pipe(Filter::new(self, f))
			}

			fn cloned<'a, T>(self) -> Cloned<Self, T, Source>
			where
				T: Clone + 'a,
				Source: 'a,
				Self: $pipe<&'a Source, Item = &'a T> + Sized,
			{
				$assert_pipe(Cloned::new(self))
			}

			// #[must_use]
			// fn chain<C>(self, chain: C) -> Chain<Self, C::Iter>
			// where
			// 	C: IntoParallelStream<Item = Self::Item>,
			// 	Self: Sized,
			// {
			// 	$assert_pipe(Chain::new(self, chain.into_par_stream()))
			// }

			fn pipe<S>(self, sink: S) -> super::par_sink::Pipe<Self, S>
			where
				S: $sink<Self::Item>,
				Self: Sized,
			{
				$assert_sink(super::par_sink::Pipe::new(self, sink))
			}

			fn fork<A, B, RefAItem>(
				self, sink: A, sink_ref: B,
			) -> Fork<Self, A, B, &'static Self::Item>
			where
				A: $sink<Self::Item>,
				B: for<'a> $sink<&'a Self::Item>,
				Self: Sized,
			{
				$assert_sink(Fork::new(self, sink, sink_ref))
			}

			fn for_each<F>(self, f: F) -> ForEach<Self, F>
			where
				F: FnMut(Self::Item) + Clone + $send + 'static,
				Self: Sized,
			{
				$assert_sink(ForEach::new(self, f))
			}

			fn fold<ID, F, B>(self, identity: ID, op: F) -> Fold<Self, ID, F, B>
			where
				ID: FnMut() -> B + Clone + $send + 'static,
				F: FnMut(B, Either<Self::Item, B>) -> B + Clone + $send + 'static,
				B: $send + 'static,
				Self: Sized,
			{
				$assert_sink(Fold::new(self, identity, op))
			}

			fn group_by<S, A, B>(self, sink: S) -> GroupBy<Self, S>
			where
				A: Eq + Hash + $send + 'static,
				S: $sink<B>,
				<S::Pipe as $pipe<B>>::Task: Clone + $send + 'static,
				S::ReduceA: 'static,
				S::ReduceC: Clone,
				S::Output: $send + 'static,
				Self: $pipe<Source, Item = (A, B)> + Sized,
			{
				$assert_sink(GroupBy::new(self, sink))
			}

			fn histogram(self) -> Histogram<Self>
			where
				Self::Item: Hash + Ord + $send + 'static,
				Self: Sized,
			{
				$assert_sink(Histogram::new(self))
			}

			fn count(self) -> Count<Self>
			where
				Self: Sized,
			{
				$assert_sink(Count::new(self))
			}

			fn sum<B>(self) -> Sum<Self, B>
			where
				B: iter::Sum<Self::Item> + iter::Sum<B> + $send + 'static,
				Self: Sized,
			{
				$assert_sink(Sum::new(self))
			}

			fn combine<F>(self, f: F) -> Combine<Self, F>
			where
				F: FnMut(Self::Item, Self::Item) -> Self::Item + Clone + $send + 'static,
				Self::Item: $send + 'static,
				Self: Sized,
			{
				$assert_sink(Combine::new(self, f))
			}

			fn max(self) -> Max<Self>
			where
				Self::Item: Ord + $send + 'static,
				Self: Sized,
			{
				$assert_sink(Max::new(self))
			}

			fn max_by<F>(self, f: F) -> MaxBy<Self, F>
			where
				F: FnMut(&Self::Item, &Self::Item) -> Ordering + Clone + $send + 'static,
				Self::Item: $send + 'static,
				Self: Sized,
			{
				$assert_sink(MaxBy::new(self, f))
			}

			fn max_by_key<F, B>(self, f: F) -> MaxByKey<Self, F>
			where
				F: FnMut(&Self::Item) -> B + Clone + $send + 'static,
				B: Ord + 'static,
				Self::Item: $send + 'static,
				Self: Sized,
			{
				$assert_sink(MaxByKey::new(self, f))
			}

			fn min(self) -> Min<Self>
			where
				Self::Item: Ord + $send + 'static,
				Self: Sized,
			{
				$assert_sink(Min::new(self))
			}

			fn min_by<F>(self, f: F) -> MinBy<Self, F>
			where
				F: FnMut(&Self::Item, &Self::Item) -> Ordering + Clone + $send + 'static,
				Self::Item: $send + 'static,
				Self: Sized,
			{
				$assert_sink(MinBy::new(self, f))
			}

			fn min_by_key<F, B>(self, f: F) -> MinByKey<Self, F>
			where
				F: FnMut(&Self::Item) -> B + Clone + $send + 'static,
				B: Ord + 'static,
				Self::Item: $send + 'static,
				Self: Sized,
			{
				$assert_sink(MinByKey::new(self, f))
			}

			fn most_frequent(self, n: usize, probability: f64, tolerance: f64) -> MostFrequent<Self>
			where
				Self::Item: Hash + Eq + Clone + $send + 'static,
				Self: Sized,
			{
				$assert_sink(MostFrequent::new(self, n, probability, tolerance))
			}

			fn most_distinct<A, B>(
				self, n: usize, probability: f64, tolerance: f64, error_rate: f64,
			) -> MostDistinct<Self>
			where
				Self: $pipe<Source, Item = (A, B)> + Sized,
				A: Hash + Eq + Clone + $send + 'static,
				B: Hash + 'static,
			{
				$assert_sink(MostDistinct::new(
					self,
					n,
					probability,
					tolerance,
					error_rate,
				))
			}

			fn sample_unstable(self, samples: usize) -> SampleUnstable<Self>
			where
				Self::Item: $send + 'static,
				Self: Sized,
			{
				$assert_sink(SampleUnstable::new(self, samples))
			}

			fn all<F>(self, f: F) -> All<Self, F>
			where
				F: FnMut(Self::Item) -> bool + Clone + $send + 'static,
				Self: Sized,
			{
				$assert_sink(All::new(self, f))
			}

			fn any<F>(self, f: F) -> Any<Self, F>
			where
				F: FnMut(Self::Item) -> bool + Clone + $send + 'static,
				Self: Sized,
			{
				$assert_sink(Any::new(self, f))
			}

			fn collect<B>(self) -> Collect<Self, B>
			where
				B: $from_sink<Self::Item>,
				Self: Sized,
			{
				$assert_sink(Collect::new(self))
			}
		}

		#[inline(always)]
		pub(crate) fn $assert_pipe<T, I: $pipe<Source, Item = T>, Source>(i: I) -> I {
			i
		}
	};
}

pipe!(ParallelPipe ParallelSink FromParallelStream Send assert_parallel_pipe assert_parallel_sink);
pipe!(DistributedPipe DistributedSink FromDistributedStream ProcessSend assert_distributed_pipe assert_distributed_sink cfg_attr(not(feature = "doc"), serde_closure::generalize));
