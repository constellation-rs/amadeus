use either::Either;
use futures::Stream;
use std::{
	cmp::Ordering, future::Future, hash::Hash, iter, ops::FnMut, pin::Pin, task::{Context, Poll}
};

use crate::{pool::ProcessSend, sink::Sink};

use super::{dist_sink::*, dist_stream::*};

#[must_use]
pub trait PipeTask<Source> {
	type Item;
	type Async: PipeTaskAsync<Source, Item = Self::Item>;

	fn into_async(self) -> Self::Async;
}
#[must_use]
pub trait PipeTaskAsync<Source> {
	type Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
		sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()>;
}

#[must_use]
pub trait DistributedPipe<Source> {
	type Item;
	type Task: PipeTask<Source, Item = Self::Item> + ProcessSend;

	fn task(&self) -> Self::Task;

	fn for_each<F>(self, f: F) -> ForEach<Self, F>
	where
		F: FnMut(Self::Item) + Clone + ProcessSend,
		Self::Item: 'static,
		Self: Sized,
	{
		assert_distributed_sink(ForEach::new(self, f))
	}

	fn inspect<F>(self, f: F) -> Inspect<Self, F>
	where
		F: FnMut(&Self::Item) + Clone + ProcessSend,
		Self: Sized,
	{
		assert_distributed_pipe(Inspect::new(self, f))
	}

	fn update<F>(self, f: F) -> Update<Self, F>
	where
		F: FnMut(&mut Self::Item) + Clone + ProcessSend,
		Self: Sized,
	{
		assert_distributed_pipe(Update::new(self, f))
	}

	fn map<B, F>(self, f: F) -> Map<Self, F>
	where
		F: FnMut(Self::Item) -> B + Clone + ProcessSend,
		Self: Sized,
	{
		assert_distributed_pipe(Map::new(self, f))
	}

	fn flat_map<B, F>(self, f: F) -> FlatMap<Self, F>
	where
		F: FnMut(Self::Item) -> B + Clone + ProcessSend,
		B: Stream,
		Self: Sized,
	{
		assert_distributed_pipe(FlatMap::new(self, f))
	}

	fn filter<F, Fut>(self, f: F) -> Filter<Self, F>
	where
		F: FnMut(&Self::Item) -> Fut + Clone + ProcessSend,
		Fut: Future<Output = bool>,
		Self: Sized,
	{
		assert_distributed_pipe(Filter::new(self, f))
	}

	// #[must_use]
	// fn chain<C>(self, chain: C) -> Chain<Self, C::Iter>
	// where
	// 	C: IntoDistributedStream<Item = Self::Item>,
	// 	Self: Sized,
	// {
	// 	Chain::new(self, chain.into_dist_stream())
	// }

	fn fold<ID, F, B>(self, identity: ID, op: F) -> Fold<Self, ID, F, B>
	where
		ID: FnMut() -> B + Clone + ProcessSend,
		F: FnMut(B, Either<Self::Item, B>) -> B + Clone + ProcessSend,
		B: ProcessSend,
		Self::Item: 'static,
		Self: Sized,
	{
		assert_distributed_sink(Fold::new(self, identity, op))
	}

	fn count(self) -> Count<Self>
	where
		Self::Item: 'static,
		Self: Sized,
	{
		assert_distributed_sink(Count::new(self))
	}

	fn sum<B>(self) -> Sum<Self, B>
	where
		B: iter::Sum<Self::Item> + iter::Sum<B> + ProcessSend,
		Self::Item: 'static,
		Self: Sized,
	{
		assert_distributed_sink(Sum::new(self))
	}

	fn combine<F>(self, f: F) -> Combine<Self, F>
	where
		F: FnMut(Self::Item, Self::Item) -> Self::Item + Clone + ProcessSend,
		Self::Item: ProcessSend,
		Self: Sized,
	{
		assert_distributed_sink(Combine::new(self, f))
	}

	fn max(self) -> Max<Self>
	where
		Self::Item: Ord + ProcessSend,
		Self: Sized,
	{
		assert_distributed_sink(Max::new(self))
	}

	fn max_by<F>(self, f: F) -> MaxBy<Self, F>
	where
		F: FnMut(&Self::Item, &Self::Item) -> Ordering + Clone + ProcessSend,
		Self::Item: ProcessSend,
		Self: Sized,
	{
		assert_distributed_sink(MaxBy::new(self, f))
	}

	fn max_by_key<F, B>(self, f: F) -> MaxByKey<Self, F>
	where
		F: FnMut(&Self::Item) -> B + Clone + ProcessSend,
		B: Ord + 'static,
		Self::Item: ProcessSend,
		Self: Sized,
	{
		assert_distributed_sink(MaxByKey::new(self, f))
	}

	fn min(self) -> Min<Self>
	where
		Self::Item: Ord + ProcessSend,
		Self: Sized,
	{
		assert_distributed_sink(Min::new(self))
	}

	fn min_by<F>(self, f: F) -> MinBy<Self, F>
	where
		F: FnMut(&Self::Item, &Self::Item) -> Ordering + Clone + ProcessSend,
		Self::Item: ProcessSend,
		Self: Sized,
	{
		assert_distributed_sink(MinBy::new(self, f))
	}

	fn min_by_key<F, B>(self, f: F) -> MinByKey<Self, F>
	where
		F: FnMut(&Self::Item) -> B + Clone + ProcessSend,
		B: Ord + 'static,
		Self::Item: ProcessSend,
		Self: Sized,
	{
		assert_distributed_sink(MinByKey::new(self, f))
	}

	fn most_frequent(self, n: usize, probability: f64, tolerance: f64) -> MostFrequent<Self>
	where
		Self::Item: Hash + Eq + Clone + ProcessSend,
		Self: Sized,
	{
		assert_distributed_sink(MostFrequent::new(self, n, probability, tolerance))
	}

	fn most_distinct<A, B>(
		self, n: usize, probability: f64, tolerance: f64, error_rate: f64,
	) -> MostDistinct<Self>
	where
		Self: DistributedPipe<Source, Item = (A, B)> + Sized,
		A: Hash + Eq + Clone + ProcessSend,
		B: Hash + 'static,
	{
		assert_distributed_sink(MostDistinct::new(
			self,
			n,
			probability,
			tolerance,
			error_rate,
		))
	}

	fn sample_unstable(self, samples: usize) -> SampleUnstable<Self>
	where
		Self::Item: ProcessSend,
		Self: Sized,
	{
		assert_distributed_sink(SampleUnstable::new(self, samples))
	}

	fn all<F>(self, f: F) -> All<Self, F>
	where
		F: FnMut(Self::Item) -> bool + Clone + ProcessSend,
		Self::Item: 'static,
		Self: Sized,
	{
		assert_distributed_sink(All::new(self, f))
	}

	fn any<F>(self, f: F) -> Any<Self, F>
	where
		F: FnMut(Self::Item) -> bool + Clone + ProcessSend,
		Self::Item: 'static,
		Self: Sized,
	{
		assert_distributed_sink(Any::new(self, f))
	}

	fn collect<B>(self) -> Collect<Self, B>
	where
		B: FromDistributedStream<Self::Item>,
		Self: Sized,
	{
		assert_distributed_sink::<B, _, _, _>(Collect::new(self))
	}

	fn cloned<'a, T>(self) -> Cloned<Self, T, Source>
	where
		T: Clone + 'a,
		Source: 'a,
		Self: DistributedPipe<&'a Source, Item = &'a T> + Sized,
	{
		assert_distributed_pipe::<T, _, _>(Cloned::new(self))
	}
}

#[inline(always)]
pub(crate) fn assert_distributed_pipe<T, I: DistributedPipe<Source, Item = T>, Source>(i: I) -> I {
	i
}
