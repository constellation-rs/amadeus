use futures::Stream;
use serde::{Deserialize, Serialize};
use std::{
	iter, pin::Pin, task::{Context, Poll}
};

use super::{
	All, Any, Collect, Combine, Count, Filter, FlatMap, Fold, ForEach, GroupBy, Inspect, Map, Max, MaxBy, MaxByKey, Min, MinBy, MinByKey, MostDistinct, MostFrequent, ParallelPipe, PipeTask, PipeTaskAsync, SampleUnstable, Sum, Update
};
use crate::sink::Sink;

// TODO: add type parameter to Identity when type the type system includes HRTB in the ParallelPipe impl https://github.com/dtolnay/ghost/

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

	impl Identity {
		pub fn inspect<F>(self, f: F) -> Inspect<Self, F>
		where
			F: Clone + Send + 'static,
			Self: Sized,
		{
			Inspect::new(self, f)
		}

		pub fn update<T, F>(self, f: F) -> Update<Self, F>
		where
			F: Clone + Send + 'static,
			Self: Sized,
		{
			Update::new(self, f)
		}

		pub fn map<F>(self, f: F) -> Map<Self, F>
		where
			F: Clone + Send + 'static,
			Self: Sized,
		{
			Map::new(self, f)
		}

		pub fn flat_map<F>(self, f: F) -> FlatMap<Self, F>
		where
			F: Clone + Send + 'static,
			Self: Sized,
		{
			FlatMap::new(self, f)
		}

		pub fn filter<F>(self, f: F) -> Filter<Self, F>
		where
			F: Clone + Send + 'static,
			Self: Sized,
		{
			Filter::new(self, f)
		}

		// #[must_use]
		// pub fn chain<C>(self, chain: C) -> Chain<Self, C::Iter>
		// where
		// 	C: IntoParallelStream<Item = Self::Item>,
		// 	Self: Sized,
		// {
		// 	Chain::new(self, chain.into_par_stream())
		// }

		pub fn for_each<F>(self, f: F) -> ForEach<Self, F>
		where
			F: Clone + Send + 'static,
			Self: Sized,
		{
			ForEach::new(self, f)
		}

		pub fn fold<ID, F, B>(self, identity: ID, op: F) -> Fold<Self, ID, F, B>
		where
			ID: FnMut() -> B + Clone + Send + 'static,
			F: Clone + Send + 'static,
			B: Send + 'static,
			Self: Sized,
		{
			Fold::new(self, identity, op)
		}

		pub fn group_by<ID, F, C>(self, identity: ID, op: F) -> GroupBy<Self, ID, F, C>
		where
			ID: FnMut() -> C + Clone + Send + 'static,
			C: Send + 'static,
			Self: Sized,
		{
			GroupBy::new(self, identity, op)
		}

		pub fn count(self) -> Count<Self>
		where
			Self: Sized,
		{
			Count::new(self)
		}

		pub fn sum<B>(self) -> Sum<Self, B>
		where
			B: iter::Sum<B> + Send + 'static,
			Self: Sized,
		{
			Sum::new(self)
		}

		pub fn combine<F>(self, f: F) -> Combine<Self, F>
		where
			F: Clone + Send + 'static,
			Self: Sized,
		{
			Combine::new(self, f)
		}

		pub fn max(self) -> Max<Self>
		where
			Self: Sized,
		{
			Max::new(self)
		}

		pub fn max_by<F>(self, f: F) -> MaxBy<Self, F>
		where
			F: Clone + Send + 'static,
			Self: Sized,
		{
			MaxBy::new(self, f)
		}

		pub fn max_by_key<F>(self, f: F) -> MaxByKey<Self, F>
		where
			F: Clone + Send + 'static,
			Self: Sized,
		{
			MaxByKey::new(self, f)
		}

		pub fn min(self) -> Min<Self>
		where
			Self: Sized,
		{
			Min::new(self)
		}

		pub fn min_by<F>(self, f: F) -> MinBy<Self, F>
		where
			F: Clone + Send + 'static,
			Self: Sized,
		{
			MinBy::new(self, f)
		}

		pub fn min_by_key<F>(self, f: F) -> MinByKey<Self, F>
		where
			F: Clone + Send + 'static,
			Self: Sized,
		{
			MinByKey::new(self, f)
		}

		pub fn most_frequent(self, n: usize, probability: f64, tolerance: f64) -> MostFrequent<Self>
		where
			Self: Sized,
		{
			MostFrequent::new(self, n, probability, tolerance)
		}

		pub fn most_distinct(
			self, n: usize, probability: f64, tolerance: f64, error_rate: f64,
		) -> MostDistinct<Self>
where {
			MostDistinct::new(self, n, probability, tolerance, error_rate)
		}

		pub fn sample_unstable(self, samples: usize) -> SampleUnstable<Self>
		where
			Self: Sized,
		{
			SampleUnstable::new(self, samples)
		}

		pub fn all<F>(self, f: F) -> All<Self, F>
		where
			F: Clone + Send + 'static,
			Self: Sized,
		{
			All::new(self, f)
		}

		pub fn any<F>(self, f: F) -> Any<Self, F>
		where
			F: Clone + Send + 'static,
			Self: Sized,
		{
			Any::new(self, f)
		}

		pub fn collect<B>(self) -> Collect<Self, B>
		where
			Self: Sized,
		{
			Collect::new(self)
		}
	}
}

#[derive(Serialize, Deserialize)]
pub struct IdentityTask;
impl<Item> PipeTask<Item> for IdentityTask {
	type Item = Item;
	type Async = IdentityTask;
	fn into_async(self) -> Self::Async {
		IdentityTask
	}
}
impl<Item> PipeTaskAsync<Item> for IdentityTask {
	type Item = Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Item>>,
		sink: Pin<&mut impl Sink<Item = Self::Item>>,
	) -> Poll<()> {
		sink.poll_forward(cx, stream)
	}
}
