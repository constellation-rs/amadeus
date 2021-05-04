use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_closure::traits;
use std::{
	iter, pin::Pin, task::{Context, Poll}
};

use super::{
	All, Any, Collect, Combine, Count, Filter, FlatMap, Fold, ForEach, Fork, GroupBy, Histogram, Inspect, Map, Max, MaxBy, MaxByKey, Mean, Min, MinBy, MinByKey, MostDistinct, MostFrequent, ParallelPipe, Pipe, PipeTask, SampleUnstable, StdDev, Sum, Update
};

// TODO: add type parameter to Identity when type the type system includes HRTB in the ParallelPipe impl https://github.com/dtolnay/ghost/

#[derive(Clone, Copy, Debug)]
pub struct Identity;

impl_par_dist! {
	impl<Item> ParallelPipe<Item> for Identity {
		type Output = Item;
		type Task = IdentityTask;

		#[inline]
		fn task(&self) -> Self::Task {
			IdentityTask
		}
	}
}

// These sortof work around https://github.com/rust-lang/rust/issues/73433
mod workaround {
	use super::*;

	#[cfg_attr(not(nightly), serde_closure::desugar)]
	#[doc(hidden)]
	impl Identity {
		#[inline]
		pub fn pipe<S>(self, sink: S) -> Pipe<Self, S> {
			Pipe::new(self, sink)
		}

		#[inline]
		pub fn fork<A, B, RefAItem>(self, sink: A, sink_ref: B) -> Fork<Self, A, B, RefAItem> {
			Fork::new(self, sink, sink_ref)
		}

		#[inline]
		pub fn inspect<F>(self, f: F) -> Inspect<Self, F>
		where
			F: Clone + Send,
		{
			Inspect::new(self, f)
		}

		#[inline]
		pub fn update<T, F>(self, f: F) -> Update<Self, F>
		where
			F: Clone + Send,
		{
			Update::new(self, f)
		}

		#[inline]
		pub fn map<F>(self, f: F) -> Map<Self, F>
		where
			F: Clone + Send,
		{
			Map::new(self, f)
		}

		#[inline]
		pub fn flat_map<F>(self, f: F) -> FlatMap<Self, F>
		where
			F: Clone + Send,
		{
			FlatMap::new(self, f)
		}

		#[inline]
		pub fn filter<F>(self, f: F) -> Filter<Self, F>
		where
			F: Clone + Send,
		{
			Filter::new(self, f)
		}

		// #[must_use]
		// #[inline]
		// pub fn chain<C>(self, chain: C) -> Chain<Self, C::Iter>
		// where
		// 	C: IntoParallelStream<Item = Self::Item>,
		// {
		// 	Chain::new(self, chain.into_par_stream())
		// }

		#[inline]
		pub fn for_each<F>(self, f: F) -> ForEach<Self, F>
		where
			F: Clone + Send,
		{
			ForEach::new(self, f)
		}

		#[inline]
		pub fn fold<ID, F, B>(self, identity: ID, op: F) -> Fold<Self, ID, F, B>
		where
			ID: traits::FnMut() -> B + Clone + Send,
			F: Clone + Send,
			B: Send,
		{
			Fold::new(self, identity, op)
		}

		#[inline]
		pub fn group_by<S>(self, sink: S) -> GroupBy<Self, S> {
			GroupBy::new(self, sink)
		}

		#[inline]
		pub fn histogram(self) -> Histogram<Self> {
			Histogram::new(self)
		}

		#[inline]
		pub fn count(self) -> Count<Self> {
			Count::new(self)
		}

		#[inline]
		pub fn sum<B>(self) -> Sum<Self, B>
		where
			B: iter::Sum<B> + Send,
		{
			Sum::new(self)
		}

		#[inline]
		pub fn mean(self) -> Mean<Self> {
			Mean::new(self)
		}

		#[inline]
		pub fn stddev(self) -> StdDev<Self> {
			StdDev::new(self)
		}

		#[inline]
		pub fn combine<F>(self, f: F) -> Combine<Self, F>
		where
			F: Clone + Send,
		{
			Combine::new(self, f)
		}

		#[inline]
		pub fn max(self) -> Max<Self> {
			Max::new(self)
		}

		#[inline]
		pub fn max_by<F>(self, f: F) -> MaxBy<Self, F>
		where
			F: Clone + Send,
		{
			MaxBy::new(self, f)
		}

		#[inline]
		pub fn max_by_key<F>(self, f: F) -> MaxByKey<Self, F>
		where
			F: Clone + Send,
		{
			MaxByKey::new(self, f)
		}

		#[inline]
		pub fn min(self) -> Min<Self> {
			Min::new(self)
		}

		#[inline]
		pub fn min_by<F>(self, f: F) -> MinBy<Self, F>
		where
			F: Clone + Send,
		{
			MinBy::new(self, f)
		}

		#[inline]
		pub fn min_by_key<F>(self, f: F) -> MinByKey<Self, F>
		where
			F: Clone + Send,
		{
			MinByKey::new(self, f)
		}

		#[inline]
		pub fn most_frequent(
			self, n: usize, probability: f64, tolerance: f64,
		) -> MostFrequent<Self> {
			MostFrequent::new(self, n, probability, tolerance)
		}

		#[inline]
		pub fn most_distinct(
			self, n: usize, probability: f64, tolerance: f64, error_rate: f64,
		) -> MostDistinct<Self> {
			MostDistinct::new(self, n, probability, tolerance, error_rate)
		}

		#[inline]
		pub fn sample_unstable(self, samples: usize) -> SampleUnstable<Self> {
			SampleUnstable::new(self, samples)
		}

		#[inline]
		pub fn all<F>(self, f: F) -> All<Self, F>
		where
			F: Clone + Send,
		{
			All::new(self, f)
		}

		#[inline]
		pub fn any<F>(self, f: F) -> Any<Self, F>
		where
			F: Clone + Send,
		{
			Any::new(self, f)
		}

		#[inline]
		pub fn collect<B>(self) -> Collect<Self, B> {
			Collect::new(self)
		}
	}
}

#[derive(Clone, Serialize, Deserialize)]
pub struct IdentityTask;
impl<Item> PipeTask<Item> for IdentityTask {
	type Output = Item;
	type Async = IdentityTask;

	#[inline]
	fn into_async(self) -> Self::Async {
		IdentityTask
	}
}
impl<Item> crate::pipe::Pipe<Item> for IdentityTask {
	type Output = Item;

	#[inline(always)]
	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<Option<Self::Output>> {
		stream.poll_next(cx)
	}
}
