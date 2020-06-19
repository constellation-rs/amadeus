use futures::Stream;
use serde::{Deserialize, Serialize};
use std::{
	pin::Pin, task::{Context, Poll}
};

use super::{
	Count, Filter, FlatMap, Fold, ForEach, Inspect, Map, ParallelPipe, PipeTask, PipeTaskAsync, Update
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

		pub fn fold<ID, F, B>(self, identity: ID, op: F) -> Fold<Self, ID, F, B>
		where
			ID: FnMut() -> B + Clone + Send + 'static,
			F: Clone + Send + 'static,
			B: Send + 'static,
			Self: Sized,
		{
			Fold::new(self, identity, op)
		}

		pub fn count(self) -> Count<Self> {
			Count::new(self)
		}

		pub fn for_each<F>(self, f: F) -> ForEach<Self, F>
		where
			F: Clone + Send + 'static,
		{
			ForEach::new(self, f)
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
