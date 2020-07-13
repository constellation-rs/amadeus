use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_closure::traits;
use std::{
	iter, pin::Pin, task::{Context, Poll}
};

use super::{
	All, Any, Collect, Combine, Count, Filter, FlatMap, Fold, ForEach, Fork, GroupBy, Histogram, Inspect, Map, Max, MaxBy, MaxByKey, Min, MinBy, MinByKey, MostDistinct, MostFrequent, ParallelPipe, Pipe, PipeTask, SampleUnstable, Sum, Update
};

// TODO: add type parameter to Identity when type the type system includes HRTB in the ParallelPipe impl https://github.com/dtolnay/ghost/

#[derive(Clone, Copy, Debug)]
pub struct Identity;

impl_par_dist! {
	impl<Item> ParallelPipe<Item> for Identity {
		type Item = Item;
		type Task = IdentityTask;

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
		pub fn pipe<S>(self, sink: S) -> Pipe<Self, S> {
			Pipe::new(self, sink)
		}

		pub fn fork<A, B, RefAItem>(self, sink: A, sink_ref: B) -> Fork<Self, A, B, RefAItem> {
			Fork::new(self, sink, sink_ref)
		}

		pub fn inspect<F>(self, f: F) -> Inspect<Self, F>
		where
			F: Clone + Send + 'static,
		{
			Inspect::new(self, f)
		}

		pub fn update<T, F>(self, f: F) -> Update<Self, F>
		where
			F: Clone + Send + 'static,
		{
			Update::new(self, f)
		}

		pub fn map<F>(self, f: F) -> Map<Self, F>
		where
			F: Clone + Send + 'static,
		{
			Map::new(self, f)
		}

		pub fn flat_map<F>(self, f: F) -> FlatMap<Self, F>
		where
			F: Clone + Send + 'static,
		{
			FlatMap::new(self, f)
		}

		pub fn filter<F>(self, f: F) -> Filter<Self, F>
		where
			F: Clone + Send + 'static,
		{
			Filter::new(self, f)
		}

		// #[must_use]
		// pub fn chain<C>(self, chain: C) -> Chain<Self, C::Iter>
		// where
		// 	C: IntoParallelStream<Item = Self::Item>,
		// {
		// 	Chain::new(self, chain.into_par_stream())
		// }

		pub fn for_each<F>(self, f: F) -> ForEach<Self, F>
		where
			F: Clone + Send + 'static,
		{
			ForEach::new(self, f)
		}

		pub fn fold<ID, F, B>(self, identity: ID, op: F) -> Fold<Self, ID, F, B>
		where
			ID: traits::FnMut() -> B + Clone + Send + 'static,
			F: Clone + Send + 'static,
			B: Send + 'static,
		{
			Fold::new(self, identity, op)
		}

		pub fn group_by<S>(self, sink: S) -> GroupBy<Self, S> {
			GroupBy::new(self, sink)
		}

		pub fn histogram(self) -> Histogram<Self> {
			Histogram::new(self)
		}

		pub fn count(self) -> Count<Self> {
			Count::new(self)
		}

		pub fn sum<B>(self) -> Sum<Self, B>
		where
			B: iter::Sum<B> + Send + 'static,
		{
			Sum::new(self)
		}

		pub fn combine<F>(self, f: F) -> Combine<Self, F>
		where
			F: Clone + Send + 'static,
		{
			Combine::new(self, f)
		}

		pub fn max(self) -> Max<Self> {
			Max::new(self)
		}

		pub fn max_by<F>(self, f: F) -> MaxBy<Self, F>
		where
			F: Clone + Send + 'static,
		{
			MaxBy::new(self, f)
		}

		pub fn max_by_key<F>(self, f: F) -> MaxByKey<Self, F>
		where
			F: Clone + Send + 'static,
		{
			MaxByKey::new(self, f)
		}

		pub fn min(self) -> Min<Self> {
			Min::new(self)
		}

		pub fn min_by<F>(self, f: F) -> MinBy<Self, F>
		where
			F: Clone + Send + 'static,
		{
			MinBy::new(self, f)
		}

		pub fn min_by_key<F>(self, f: F) -> MinByKey<Self, F>
		where
			F: Clone + Send + 'static,
		{
			MinByKey::new(self, f)
		}

		pub fn most_frequent(
			self, n: usize, probability: f64, tolerance: f64,
		) -> MostFrequent<Self> {
			MostFrequent::new(self, n, probability, tolerance)
		}

		pub fn most_distinct(
			self, n: usize, probability: f64, tolerance: f64, error_rate: f64,
		) -> MostDistinct<Self> {
			MostDistinct::new(self, n, probability, tolerance, error_rate)
		}

		pub fn sample_unstable(self, samples: usize) -> SampleUnstable<Self> {
			SampleUnstable::new(self, samples)
		}

		pub fn all<F>(self, f: F) -> All<Self, F>
		where
			F: Clone + Send + 'static,
		{
			All::new(self, f)
		}

		pub fn any<F>(self, f: F) -> Any<Self, F>
		where
			F: Clone + Send + 'static,
		{
			Any::new(self, f)
		}

		pub fn collect<B>(self) -> Collect<Self, B> {
			Collect::new(self)
		}
	}
}

#[derive(Clone, Serialize, Deserialize)]
pub struct IdentityTask;
impl<Item> PipeTask<Item> for IdentityTask {
	type Item = Item;
	type Async = IdentityTask;

	fn into_async(self) -> Self::Async {
		IdentityTask
	}
}
impl<Item> crate::pipe::Pipe<Item> for IdentityTask {
	type Item = Item;

	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<Option<Item>> {
		stream.poll_next(cx)
	}
}
