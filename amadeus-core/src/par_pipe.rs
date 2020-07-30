#![allow(unused_qualifications)]

use either::Either;
use futures::Stream;
use serde_closure::traits;
use std::{cmp::Ordering, hash::Hash, iter, ops};

use super::{par_sink::*, par_stream::*};
use crate::{pipe::Pipe, pool::ProcessSend};

#[must_use]
pub trait PipeTask<Input> {
	type Output;
	type Async: Pipe<Input, Output = Self::Output>;

	fn into_async(self) -> Self::Async;
}

macro_rules! pipe {
	($pipe:ident $sink:ident $from_sink:ident $send:ident $fns:ident $assert_pipe:ident $assert_sink:ident $($meta:meta)*) => {
		$(#[$meta])*
		#[must_use]
		pub trait $pipe<Input> {
			type Output;
			type Task: PipeTask<Input, Output = Self::Output> + $send;

			fn task(&self) -> Self::Task;

			#[inline]
			fn inspect<F>(self, f: F) -> Inspect<Self, F>
			where
				F: $fns::FnMut(&Self::Output) + Clone + $send,
				Self: Sized,
			{
				$assert_pipe(Inspect::new(self, f))
			}

			#[inline]
			fn update<F>(self, f: F) -> Update<Self, F>
			where
				F: $fns::FnMut(&mut Self::Output) + Clone + $send,
				Self: Sized,
			{
				$assert_pipe(Update::new(self, f))
			}

			#[inline]
			fn map<B, F>(self, f: F) -> Map<Self, F>
			where
				F: $fns::FnMut(Self::Output) -> B + Clone + $send,
				Self: Sized,
			{
				$assert_pipe(Map::new(self, f))
			}

			#[inline]
			fn flat_map<B, F>(self, f: F) -> FlatMap<Self, F>
			where
				F: $fns::FnMut(Self::Output) -> B + Clone + $send,
				B: Stream,
				Self: Sized,
			{
				$assert_pipe(FlatMap::new(self, f))
			}

			#[inline]
			fn filter<F>(self, f: F) -> Filter<Self, F>
			where
				F: $fns::FnMut(&Self::Output) -> bool + Clone + $send,
				Self: Sized,
			{
				$assert_pipe(Filter::new(self, f))
			}

			#[inline]
			fn cloned<'a, T>(self) -> Cloned<Self, T, Input>
			where
				T: Clone + 'a,
				Input: 'a,
				Self: $pipe<&'a Input, Output = &'a T> + Sized,
			{
				$assert_pipe(Cloned::new(self))
			}

			#[inline]
			fn left_join<K, V1, V2>(self, right: impl IntoIterator<Item = (K, V2)>) -> LeftJoin<Self, K, V1, V2>
			where
				K: Eq + Hash + Clone + $send,
				V2: Clone + $send,
				Self: $pipe<Input, Output = (K, V1)> + Sized,
			{
				$assert_pipe(LeftJoin::new(self, right.into_iter().collect()))
			}

			#[inline]
			fn inner_join<K, V1, V2>(self, right: impl IntoIterator<Item = (K, V2)>) -> InnerJoin<Self, K, V1, V2>
			where
				K: Eq + Hash + Clone + $send,
				V2: Clone + $send,
				Self: $pipe<Input, Output = (K, V1)> + Sized,
			{
				$assert_pipe(InnerJoin::new(self, right.into_iter().collect()))
			}

			// #[must_use]
			// fn chain<C>(self, chain: C) -> Chain<Self, C::Iter>
			// where
			// 	C: IntoParallelStream<Output = Self::Output>,
			// 	Self: Sized,
			// {
			// 	$assert_pipe(Chain::new(self, chain.into_par_stream()))
			// }

			#[inline]
			fn pipe<S>(self, sink: S) -> super::par_sink::Pipe<Self, S>
			where
				S: $sink<Self::Output>,
				Self: Sized,
			{
				$assert_sink(super::par_sink::Pipe::new(self, sink))
			}

			#[inline]
			fn fork<A, B, RefAItem>(
				self, sink: A, sink_ref: B,
			) -> Fork<Self, A, B, &'static Self::Output>
			where
				A: $sink<Self::Output>,
				B: for<'a> $sink<&'a Self::Output>,
				Self: Sized,
			{
				$assert_sink(Fork::new(self, sink, sink_ref))
			}

			#[inline]
			fn for_each<F>(self, f: F) -> ForEach<Self, F>
			where
				F: $fns::FnMut(Self::Output) + Clone + $send,
				Self: Sized,
			{
				$assert_sink(ForEach::new(self, f))
			}

			#[inline]
			fn fold<ID, F, B>(self, identity: ID, op: F) -> Fold<Self, ID, F, B>
			where
				ID: $fns::FnMut() -> B + Clone + $send,
				F: $fns::FnMut(B, Either<Self::Output, B>) -> B + Clone + $send,
				B: $send,
				Self: Sized,
			{
				$assert_sink(Fold::new(self, identity, op))
			}

			#[inline]
			fn group_by<S, A, B>(self, sink: S) -> GroupBy<Self, S>
			where
				A: Eq + Hash + $send,
				S: $sink<B>,
				<S::Pipe as $pipe<B>>::Task: Clone + $send,
				S::ReduceC: Clone,
				S::Done: $send,
				Self: $pipe<Input, Output = (A, B)> + Sized,
			{
				$assert_sink(GroupBy::new(self, sink))
			}

			#[inline]
			fn histogram(self) -> Histogram<Self>
			where
				Self::Output: Hash + Ord + $send,
				Self: Sized,
			{
				$assert_sink(Histogram::new(self))
			}

			#[inline]
			fn sort_n_by<F>(self, n: usize, cmp: F) -> Sort<Self, F>
			where
				F: $fns::Fn(&Self::Output, &Self::Output) -> Ordering + Clone + $send,
				Self::Output: Clone + $send,
				Self: Sized,
			{
				$assert_sink(Sort::new(self, cmp, n))
			}

			#[inline]
			fn count(self) -> Count<Self>
			where
				Self: Sized,
			{
				$assert_sink(Count::new(self))
			}

			#[inline]
			fn sum<B>(self) -> Sum<Self, B>
			where
				B: iter::Sum<Self::Output> + iter::Sum<B> + $send,
				Self: Sized,
			{
				$assert_sink(Sum::new(self))
			}

			#[inline]
			fn mean(self) -> Mean<Self>
			where
				Self: $pipe<Input, Output = f64> + Sized,
			{
				$assert_sink(Mean::new(self))
			}

			#[inline]
			fn stddev(self) -> StdDev<Self>
			where
				Self: $pipe<Input, Output = f64> + Sized,
			{
				$assert_sink(StdDev::new(self))
			}

			#[inline]
			fn combine<F>(self, f: F) -> Combine<Self, F>
			where
				F: $fns::FnMut(Self::Output, Self::Output) -> Self::Output + Clone + $send,
				Self::Output: $send,
				Self: Sized,
			{
				$assert_sink(Combine::new(self, f))
			}

			#[inline]
			fn max(self) -> Max<Self>
			where
				Self::Output: Ord + $send,
				Self: Sized,
			{
				$assert_sink(Max::new(self))
			}

			#[inline]
			fn max_by<F>(self, f: F) -> MaxBy<Self, F>
			where
				F: $fns::FnMut(&Self::Output, &Self::Output) -> Ordering + Clone + $send,
				Self::Output: $send,
				Self: Sized,
			{
				$assert_sink(MaxBy::new(self, f))
			}

			#[inline]
			fn max_by_key<F, B>(self, f: F) -> MaxByKey<Self, F>
			where
				F: $fns::FnMut(&Self::Output) -> B + Clone + $send,
				B: Ord,
				Self::Output: $send,
				Self: Sized,
			{
				$assert_sink(MaxByKey::new(self, f))
			}

			#[inline]
			fn min(self) -> Min<Self>
			where
				Self::Output: Ord + $send,
				Self: Sized,
			{
				$assert_sink(Min::new(self))
			}

			#[inline]
			fn min_by<F>(self, f: F) -> MinBy<Self, F>
			where
				F: $fns::FnMut(&Self::Output, &Self::Output) -> Ordering + Clone + $send,
				Self::Output: $send,
				Self: Sized,
			{
				$assert_sink(MinBy::new(self, f))
			}

			#[inline]
			fn min_by_key<F, B>(self, f: F) -> MinByKey<Self, F>
			where
				F: $fns::FnMut(&Self::Output) -> B + Clone + $send,
				B: Ord,
				Self::Output: $send,
				Self: Sized,
			{
				$assert_sink(MinByKey::new(self, f))
			}

			#[inline]
			fn most_frequent(self, n: usize, probability: f64, tolerance: f64) -> MostFrequent<Self>
			where
				Self::Output: Hash + Eq + Clone + $send,
				Self: Sized,
			{
				$assert_sink(MostFrequent::new(self, n, probability, tolerance))
			}

			#[inline]
			fn most_distinct<A, B>(
				self, n: usize, probability: f64, tolerance: f64, error_rate: f64,
			) -> MostDistinct<Self>
			where
				Self: $pipe<Input, Output = (A, B)> + Sized,
				A: Hash + Eq + Clone + $send,
				B: Hash,
			{
				$assert_sink(MostDistinct::new(
					self,
					n,
					probability,
					tolerance,
					error_rate,
				))
			}

			#[inline]
			fn sample_unstable(self, samples: usize) -> SampleUnstable<Self>
			where
				Self::Output: $send,
				Self: Sized,
			{
				$assert_sink(SampleUnstable::new(self, samples))
			}

			#[inline]
			fn all<F>(self, f: F) -> All<Self, F>
			where
				F: $fns::FnMut(Self::Output) -> bool + Clone + $send,
				Self: Sized,
			{
				$assert_sink(All::new(self, f))
			}

			#[inline]
			fn any<F>(self, f: F) -> Any<Self, F>
			where
				F: $fns::FnMut(Self::Output) -> bool + Clone + $send,
				Self: Sized,
			{
				$assert_sink(Any::new(self, f))
			}

			#[inline]
			fn collect<B>(self) -> Collect<Self, B>
			where
				B: $from_sink<Self::Output>,
				Self: Sized,
			{
				$assert_sink(Collect::new(self))
			}
		}

		#[inline(always)]
		pub(crate) fn $assert_pipe<T, I: $pipe<Input, Output = T>, Input>(i: I) -> I {
			i
		}
	};
}

pipe!(ParallelPipe ParallelSink FromParallelStream Send ops assert_parallel_pipe assert_parallel_sink);
pipe!(DistributedPipe DistributedSink FromDistributedStream ProcessSend traits assert_distributed_pipe assert_distributed_sink cfg_attr(not(nightly), serde_closure::desugar));
