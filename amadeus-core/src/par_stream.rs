// TODO: P: Pool -> impl Pool: async_trait triggers https://github.com/rust-lang/rust/issues/71869
// TODO: how to dedup??

#![allow(clippy::too_many_lines)]

mod chain;
mod cloned;
mod filter;
mod flat_map;
mod identity;
mod inspect;
mod map;
mod sum_type;
mod update;

use async_trait::async_trait;
use either::Either;
use futures::{pin_mut, stream, stream::StreamExt as _, Stream};
use serde_closure::*;
use std::{cmp::Ordering, collections::HashMap, hash::Hash, iter, vec};

use crate::{
	into_par_stream::IntoDistributedStream, pipe::StreamExt, pool::{ProcessPool, ProcessSend, ThreadPool}
};

pub use self::{
	chain::*, cloned::*, filter::*, flat_map::*, identity::*, inspect::*, map::*, update::*
};

use super::{par_pipe::*, par_sink::*};

#[must_use]
pub trait StreamTask {
	type Item;
	type Async: Stream<Item = Self::Item>;

	fn into_async(self) -> Self::Async;
}

#[async_trait(?Send)]
#[cfg_attr(not(feature = "doc"), serde_closure::generalize)]
#[must_use]
pub trait DistributedStream {
	type Item;
	type Task: StreamTask<Item = Self::Item> + ProcessSend;
	fn size_hint(&self) -> (usize, Option<usize>);
	fn next_task(&mut self) -> Option<Self::Task>;

	fn inspect<F>(self, f: F) -> Inspect<Self, F>
	where
		F: FnMut(&Self::Item) + Clone + ProcessSend + 'static,
		Self: Sized,
	{
		assert_distributed_stream(Inspect::new(self, f))
	}

	fn update<F>(self, f: F) -> Update<Self, F>
	where
		F: FnMut(&mut Self::Item) + Clone + ProcessSend + 'static,
		Self: Sized,
	{
		assert_distributed_stream(Update::new(self, f))
	}

	fn map<B, F>(self, f: F) -> Map<Self, F>
	where
		F: FnMut(Self::Item) -> B + Clone + ProcessSend + 'static,
		Self: Sized,
	{
		assert_distributed_stream(Map::new(self, f))
	}

	fn flat_map<B, F>(self, f: F) -> FlatMap<Self, F>
	where
		F: FnMut(Self::Item) -> B + Clone + ProcessSend + 'static,
		B: Stream,
		Self: Sized,
	{
		assert_distributed_stream(FlatMap::new(self, f))
	}

	fn filter<F>(self, f: F) -> Filter<Self, F>
	where
		F: FnMut(&Self::Item) -> bool + Clone + ProcessSend + 'static,
		Self: Sized,
	{
		assert_distributed_stream(Filter::new(self, f))
	}

	fn chain<C>(self, chain: C) -> Chain<Self, C::DistStream>
	where
		C: IntoDistributedStream<Item = Self::Item>,
		Self: Sized,
	{
		assert_distributed_stream(Chain::new(self, chain.into_dist_stream()))
	}

	async fn reduce<P, B, R1, R2, R3>(
		mut self, pool: &P, reduce_a_factory: R1, reduce_b_factory: R2, reduce_c: R3,
	) -> B
	where
		P: ProcessPool,
		R1: ReducerSend<Self::Item> + Clone + ProcessSend + 'static,
		R2: ReducerProcessSend<<R1 as ReducerSend<Self::Item>>::Output>
			+ Clone
			+ ProcessSend
			+ 'static,
		R3: Reducer<
			<R2 as ReducerProcessSend<<R1 as ReducerSend<Self::Item>>::Output>>::Output,
			Output = B,
		>,
		Self::Task: 'static,
		Self: Sized,
	{
		// TODO: don't buffer tasks before sending. requires changes to ProcessPool
		let mut tasks = (0..pool.processes()).map(|_| vec![]).collect::<Vec<_>>();
		let mut allocated = 0;
		'a: loop {
			for i in 0..tasks.len() {
				loop {
					let (mut lower, _upper) = self.size_hint();
					if lower == 0 {
						lower = 1;
					}
					let mut batch = (allocated + lower) / tasks.len();
					if i < (allocated + lower) % tasks.len() {
						batch += 1;
					}
					batch -= tasks[i].len();
					if batch == 0 {
						break;
					}
					for _ in 0..batch {
						if let Some(task) = self.next_task() {
							tasks[i].push(task);
							allocated += 1;
						} else {
							break 'a;
						}
					}
				}
			}
		}
		for (i, task) in tasks.iter().enumerate() {
			let mut count = allocated / tasks.len();
			if i < allocated % tasks.len() {
				count += 1;
			}
			assert_eq!(
				task.len(),
				count,
				"alloc: {:#?}",
				tasks.iter().map(Vec::len).collect::<Vec<_>>()
			);
		}

		let handles = tasks
			.into_iter()
			.filter(|tasks| !tasks.is_empty())
			.map(|tasks| {
				let reduce_b = reduce_b_factory.clone();
				let reduce_a_factory = reduce_a_factory.clone();
				pool.spawn(FnOnce!(move |pool: &P::ThreadPool| {
					let mut process_tasks = tasks.into_iter();

					let mut tasks = (0..pool.threads()).map(|_| vec![]).collect::<Vec<_>>();
					let mut allocated = 0;
					'a: loop {
						for i in 0..tasks.len() {
							loop {
								let (mut lower, _upper) = process_tasks.size_hint();
								if lower == 0 {
									lower = 1;
								}
								let mut batch = (allocated + lower) / tasks.len();
								if i < (allocated + lower) % tasks.len() {
									batch += 1;
								}
								batch -= tasks[i].len();
								if batch == 0 {
									break;
								}
								for _ in 0..batch {
									if let Some(task) = process_tasks.next() {
										tasks[i].push(task);
										allocated += 1;
									} else {
										break 'a;
									}
								}
							}
						}
					}
					for (i, task) in tasks.iter().enumerate() {
						let mut count = allocated / tasks.len();
						if i < allocated % tasks.len() {
							count += 1;
						}
						assert_eq!(
							task.len(),
							count,
							"alloc: {:#?}",
							tasks.iter().map(Vec::len).collect::<Vec<_>>()
						);
					}
					let handles = tasks
						.into_iter()
						.filter(|tasks| !tasks.is_empty())
						.map(|tasks| {
							let reduce_a = reduce_a_factory.clone();
							pool.spawn(move || async move {
								let sink = reduce_a.into_async();
								pin_mut!(sink);
								let tasks =
									stream::iter(tasks.into_iter().map(StreamTask::into_async))
										.flatten();
								tasks.sink(sink.as_mut()).await;
								sink.output().await
							})
						})
						.collect::<futures::stream::FuturesUnordered<_>>();

					let stream = handles.map(|item| {
						item.unwrap_or_else(|err| {
							panic!("Amadeus: task '<unnamed>' panicked at '{}'", err)
						})
					});
					let reduce_b = reduce_b.into_async();
					async move {
						pin_mut!(reduce_b);
						stream.sink(reduce_b.as_mut()).await;
						reduce_b.output().await
					}
				}))
			})
			.collect::<futures::stream::FuturesUnordered<_>>();
		let stream = handles.map(|item| {
			item.unwrap_or_else(|err| panic!("Amadeus: task '<unnamed>' panicked at '{}'", err))
		});
		let reduce_c = reduce_c.into_async();
		pin_mut!(reduce_c);
		stream.sink(reduce_c.as_mut()).await;
		reduce_c.output().await
	}

	async fn pipe<P, DistSink, A>(self, pool: &P, sink: DistSink) -> A
	where
		P: ProcessPool,
		DistSink: DistributedSink<Self::Item, Output = A>,
		<DistSink::Pipe as DistributedPipe<Self::Item>>::Task: 'static,
		DistSink::ReduceA: 'static,
		DistSink::ReduceB: 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		let (iterator, reducer_a, reducer_b, reducer_c) = sink.reducers();
		Pipe::new(self, iterator)
			.reduce(pool, reducer_a, reducer_b, reducer_c)
			.await
	}

	// These messy bounds are unfortunately necessary as requiring 'static in ParallelSink breaks sink_b being e.g. Identity.count()
	async fn fork<P, DistSinkA, DistSinkB, A, B>(
		self, pool: &P, sink_a: DistSinkA, sink_b: DistSinkB,
	) -> (A, B)
	where
		P: ProcessPool,
		DistSinkA: DistributedSink<Self::Item, Output = A>,
		DistSinkB: for<'a> DistributedSink<&'a Self::Item, Output = B> + 'static,
		<DistSinkA::Pipe as DistributedPipe<Self::Item>>::Task: 'static,
		DistSinkA::ReduceA: 'static,
		DistSinkA::ReduceB: 'static,
		<DistSinkB as DistributedSink<&'static Self::Item>>::ReduceA: 'static,
		<DistSinkB as DistributedSink<&'static Self::Item>>::ReduceB: 'static,
		<<DistSinkB as DistributedSink<&'static Self::Item>>::Pipe as DistributedPipe<
			&'static Self::Item,
		>>::Task: 'static,
		Self::Item: 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		let (iterator_a, reducer_a_a, reducer_a_b, reducer_a_c) = sink_a.reducers();
		let (iterator_b, reducer_b_a, reducer_b_b, reducer_b_c) = sink_b.reducers();
		Fork::new(self, iterator_a, iterator_b)
			.reduce(
				pool,
				ReduceA2::new(reducer_a_a, reducer_b_a),
				ReduceC2::new(reducer_a_b, reducer_b_b),
				ReduceC2::new(reducer_a_c, reducer_b_c),
			)
			.await
	}

	async fn for_each<P, F>(self, pool: &P, f: F)
	where
		P: ProcessPool,
		F: FnMut(Self::Item) + Clone + ProcessSend + 'static,
		Self::Item: 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::for_each(Identity, f))
			.await
	}

	async fn fold<P, ID, F, B>(self, pool: &P, identity: ID, op: F) -> B
	where
		P: ProcessPool,
		ID: FnMut() -> B + Clone + ProcessSend + 'static,
		F: FnMut(B, Either<Self::Item, B>) -> B + Clone + ProcessSend + 'static,
		B: ProcessSend + 'static,
		Self::Item: 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(
			pool,
			DistributedPipe::<Self::Item>::fold(Identity, identity, op),
		)
		.await
	}

	async fn group_by<P, S, A, B>(self, pool: &P, sink: S) -> HashMap<A, S::Output>
	where
		P: ProcessPool,
		A: Eq + Hash + ProcessSend + 'static,
		B: 'static,
		S: DistributedSink<B>,
		<S::Pipe as DistributedPipe<B>>::Task: Clone + ProcessSend + 'static,
		S::ReduceA: 'static,
		S::ReduceB: 'static,
		S::ReduceC: Clone,
		S::Output: ProcessSend + 'static,
		Self::Task: 'static,
		Self: DistributedStream<Item = (A, B)> + Sized,
	{
		self.pipe(
			pool,
			DistributedPipe::<Self::Item>::group_by(Identity, sink),
		)
		.await
	}

	async fn histogram<P>(self, pool: &P) -> Vec<(Self::Item, usize)>
	where
		P: ProcessPool,
		Self::Item: Hash + Ord + ProcessSend + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::histogram(Identity))
			.await
	}

	async fn count<P>(self, pool: &P) -> usize
	where
		P: ProcessPool,
		Self::Item: 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::count(Identity))
			.await
	}

	async fn sum<P, S>(self, pool: &P) -> S
	where
		P: ProcessPool,
		S: iter::Sum<Self::Item> + iter::Sum<S> + ProcessSend + 'static,
		Self::Item: 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::sum(Identity))
			.await
	}

	async fn combine<P, F>(self, pool: &P, f: F) -> Option<Self::Item>
	where
		P: ProcessPool,
		F: FnMut(Self::Item, Self::Item) -> Self::Item + Clone + ProcessSend + 'static,
		Self::Item: ProcessSend + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::combine(Identity, f))
			.await
	}

	async fn max<P>(self, pool: &P) -> Option<Self::Item>
	where
		P: ProcessPool,
		Self::Item: Ord + ProcessSend + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::max(Identity))
			.await
	}

	async fn max_by<P, F>(self, pool: &P, f: F) -> Option<Self::Item>
	where
		P: ProcessPool,
		F: FnMut(&Self::Item, &Self::Item) -> Ordering + Clone + ProcessSend + 'static,
		Self::Item: ProcessSend + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::max_by(Identity, f))
			.await
	}

	async fn max_by_key<P, F, B>(self, pool: &P, f: F) -> Option<Self::Item>
	where
		P: ProcessPool,
		F: FnMut(&Self::Item) -> B + Clone + ProcessSend + 'static,
		B: Ord + 'static,
		Self::Item: ProcessSend + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::max_by_key(Identity, f))
			.await
	}

	async fn min<P>(self, pool: &P) -> Option<Self::Item>
	where
		P: ProcessPool,
		Self::Item: Ord + ProcessSend + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::min(Identity))
			.await
	}

	async fn min_by<P, F>(self, pool: &P, f: F) -> Option<Self::Item>
	where
		P: ProcessPool,
		F: FnMut(&Self::Item, &Self::Item) -> Ordering + Clone + ProcessSend + 'static,
		Self::Item: ProcessSend + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::min_by(Identity, f))
			.await
	}

	async fn min_by_key<P, F, B>(self, pool: &P, f: F) -> Option<Self::Item>
	where
		P: ProcessPool,
		F: FnMut(&Self::Item) -> B + Clone + ProcessSend + 'static,
		B: Ord + 'static,
		Self::Item: ProcessSend + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::min_by_key(Identity, f))
			.await
	}

	async fn most_frequent<P>(
		self, pool: &P, n: usize, probability: f64, tolerance: f64,
	) -> ::streaming_algorithms::Top<Self::Item, usize>
	where
		P: ProcessPool,
		Self::Item: Hash + Eq + Clone + ProcessSend + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(
			pool,
			DistributedPipe::<Self::Item>::most_frequent(Identity, n, probability, tolerance),
		)
		.await
	}

	async fn most_distinct<P, A, B>(
		self, pool: &P, n: usize, probability: f64, tolerance: f64, error_rate: f64,
	) -> ::streaming_algorithms::Top<A, streaming_algorithms::HyperLogLogMagnitude<B>>
	where
		P: ProcessPool,
		Self: DistributedStream<Item = (A, B)> + Sized,
		A: Hash + Eq + Clone + ProcessSend + 'static,
		B: Hash + 'static,
		Self::Task: 'static,
	{
		self.pipe(
			pool,
			DistributedPipe::<Self::Item>::most_distinct(
				Identity,
				n,
				probability,
				tolerance,
				error_rate,
			),
		)
		.await
	}

	async fn sample_unstable<P>(
		self, pool: &P, samples: usize,
	) -> ::streaming_algorithms::SampleUnstable<Self::Item>
	where
		P: ProcessPool,
		Self::Item: ProcessSend + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(
			pool,
			DistributedPipe::<Self::Item>::sample_unstable(Identity, samples),
		)
		.await
	}

	async fn all<P, F>(self, pool: &P, f: F) -> bool
	where
		P: ProcessPool,
		F: FnMut(Self::Item) -> bool + Clone + ProcessSend + 'static,
		Self::Item: 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::all(Identity, f))
			.await
	}

	async fn any<P, F>(self, pool: &P, f: F) -> bool
	where
		P: ProcessPool,
		F: FnMut(Self::Item) -> bool + Clone + ProcessSend + 'static,
		Self::Item: 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::any(Identity, f))
			.await
	}

	async fn collect<P, B>(self, pool: &P) -> B
	where
		P: ProcessPool,
		B: FromDistributedStream<Self::Item>,
		B::ReduceA: ProcessSend + 'static,
		B::ReduceB: ProcessSend + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::collect(Identity))
			.await
	}
}

#[inline(always)]
pub(crate) fn assert_distributed_stream<T, I: DistributedStream<Item = T>>(i: I) -> I {
	i
}

#[async_trait(?Send)]
#[must_use]
pub trait ParallelStream {
	type Item;
	type Task: StreamTask<Item = Self::Item> + Send;
	fn size_hint(&self) -> (usize, Option<usize>);
	fn next_task(&mut self) -> Option<Self::Task>;

	fn inspect<F>(self, f: F) -> Inspect<Self, F>
	where
		F: FnMut(&Self::Item) + Clone + Send + 'static,
		Self: Sized,
	{
		assert_parallel_stream(Inspect::new(self, f))
	}

	fn update<F>(self, f: F) -> Update<Self, F>
	where
		F: FnMut(&mut Self::Item) + Clone + Send + 'static,
		Self: Sized,
	{
		assert_parallel_stream(Update::new(self, f))
	}

	fn map<B, F>(self, f: F) -> Map<Self, F>
	where
		F: FnMut(Self::Item) -> B + Clone + Send + 'static,
		Self: Sized,
	{
		assert_parallel_stream(Map::new(self, f))
	}

	fn flat_map<B, F>(self, f: F) -> FlatMap<Self, F>
	where
		F: FnMut(Self::Item) -> B + Clone + Send + 'static,
		B: Stream,
		Self: Sized,
	{
		assert_parallel_stream(FlatMap::new(self, f))
	}

	fn filter<F>(self, f: F) -> Filter<Self, F>
	where
		F: FnMut(&Self::Item) -> bool + Clone + Send + 'static,
		Self: Sized,
	{
		assert_parallel_stream(Filter::new(self, f))
	}

	// fn chain<C>(self, chain: C) -> Chain<Self, C::ParStream>
	// where
	// 	C: IntoParallelStream<Item = Self::Item>,
	// 	Self: Sized,
	// {
	// 	assert_parallel_stream(Chain::new(self, chain.into_par_stream()))
	// }

	async fn reduce<P, B, R1, R3>(mut self, pool: &P, reduce_a_factory: R1, reduce_c: R3) -> B
	where
		P: ThreadPool,
		R1: ReducerSend<Self::Item> + Clone + Send + 'static,
		R3: Reducer<<R1 as ReducerSend<Self::Item>>::Output, Output = B>,
		Self::Task: 'static,
		Self: Sized,
	{
		// TODO: don't buffer tasks before sending. requires changes to ThreadPool
		let mut tasks = (0..pool.threads()).map(|_| vec![]).collect::<Vec<_>>();
		let mut allocated = 0;
		'a: loop {
			for i in 0..tasks.len() {
				loop {
					let (mut lower, _upper) = self.size_hint();
					if lower == 0 {
						lower = 1;
					}
					let mut batch = (allocated + lower) / tasks.len();
					if i < (allocated + lower) % tasks.len() {
						batch += 1;
					}
					batch -= tasks[i].len();
					if batch == 0 {
						break;
					}
					for _ in 0..batch {
						if let Some(task) = self.next_task() {
							tasks[i].push(task);
							allocated += 1;
						} else {
							break 'a;
						}
					}
				}
			}
		}
		for (i, task) in tasks.iter().enumerate() {
			let mut count = allocated / tasks.len();
			if i < allocated % tasks.len() {
				count += 1;
			}
			assert_eq!(
				task.len(),
				count,
				"alloc: {:#?}",
				tasks.iter().map(Vec::len).collect::<Vec<_>>()
			);
		}

		let handles = tasks
			.into_iter()
			.filter(|tasks| !tasks.is_empty())
			.map(|tasks| {
				let reduce_a = reduce_a_factory.clone();
				pool.spawn(move || async move {
					let sink = reduce_a.into_async();
					pin_mut!(sink);
					let tasks =
						stream::iter(tasks.into_iter().map(StreamTask::into_async)).flatten();
					tasks.sink(sink.as_mut()).await;
					sink.output().await
				})
			})
			.collect::<futures::stream::FuturesUnordered<_>>();
		let stream = handles.map(|item| {
			item.unwrap_or_else(|err| panic!("Amadeus: task '<unnamed>' panicked at '{}'", err))
		});
		let reduce_c = reduce_c.into_async();
		pin_mut!(reduce_c);
		stream.sink(reduce_c.as_mut()).await;
		reduce_c.output().await
	}

	async fn pipe<P, ParSink, A>(self, pool: &P, sink: ParSink) -> A
	where
		P: ThreadPool,
		ParSink: ParallelSink<Self::Item, Output = A>,
		<ParSink::Pipe as ParallelPipe<Self::Item>>::Task: 'static,
		ParSink::ReduceA: 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		let (iterator, reducer_a, reducer_b) = sink.reducers();
		Pipe::new(self, iterator)
			.reduce(pool, reducer_a, reducer_b)
			.await
	}

	// These messy bounds are unfortunately necessary as requiring 'static in ParallelSink breaks sink_b being e.g. Identity.count()
	async fn fork<P, ParSinkA, ParSinkB, A, B>(
		self, pool: &P, sink_a: ParSinkA, sink_b: ParSinkB,
	) -> (A, B)
	where
		P: ThreadPool,
		ParSinkA: ParallelSink<Self::Item, Output = A>,
		ParSinkB: for<'a> ParallelSink<&'a Self::Item, Output = B> + 'static,
		<ParSinkA::Pipe as ParallelPipe<Self::Item>>::Task: 'static,
		ParSinkA::ReduceA: 'static,
		<ParSinkB as ParallelSink<&'static Self::Item>>::ReduceA: 'static,
		<<ParSinkB as ParallelSink<&'static Self::Item>>::Pipe as ParallelPipe<
			&'static Self::Item,
		>>::Task: 'static,
		Self::Item: 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		let (iterator_a, reducer_a_a, reducer_a_b) = sink_a.reducers();
		let (iterator_b, reducer_b_a, reducer_b_b) = sink_b.reducers();
		Fork::new(self, iterator_a, iterator_b)
			.reduce(
				pool,
				ReduceA2::new(reducer_a_a, reducer_b_a),
				ReduceC2::new(reducer_a_b, reducer_b_b),
			)
			.await
	}

	async fn for_each<P, F>(self, pool: &P, f: F)
	where
		P: ThreadPool,
		F: FnMut(Self::Item) + Clone + Send + 'static,
		Self::Item: 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::for_each(Identity, f))
			.await
	}

	async fn fold<P, ID, F, B>(self, pool: &P, identity: ID, op: F) -> B
	where
		P: ThreadPool,
		ID: FnMut() -> B + Clone + Send + 'static,
		F: FnMut(B, Either<Self::Item, B>) -> B + Clone + Send + 'static,
		B: Send + 'static,
		Self::Item: 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(
			pool,
			ParallelPipe::<Self::Item>::fold(Identity, identity, op),
		)
		.await
	}

	async fn group_by<P, S, A, B>(self, pool: &P, sink: S) -> HashMap<A, S::Output>
	where
		P: ThreadPool,
		A: Eq + Hash + Send + 'static,
		B: 'static,
		S: ParallelSink<B>,
		<S::Pipe as ParallelPipe<B>>::Task: Clone + Send + 'static,
		S::ReduceA: 'static,
		S::ReduceC: Clone,
		S::Output: Send + 'static,
		Self::Task: 'static,
		Self: ParallelStream<Item = (A, B)> + Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::group_by(Identity, sink))
			.await
	}

	async fn histogram<P>(self, pool: &P) -> Vec<(Self::Item, usize)>
	where
		P: ThreadPool,
		Self::Item: Hash + Ord + Send + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::histogram(Identity))
			.await
	}

	async fn count<P>(self, pool: &P) -> usize
	where
		P: ThreadPool,
		Self::Item: 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::count(Identity))
			.await
	}

	async fn sum<P, S>(self, pool: &P) -> S
	where
		P: ThreadPool,
		S: iter::Sum<Self::Item> + iter::Sum<S> + Send + 'static,
		Self::Item: 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::sum(Identity))
			.await
	}

	async fn combine<P, F>(self, pool: &P, f: F) -> Option<Self::Item>
	where
		P: ThreadPool,
		F: FnMut(Self::Item, Self::Item) -> Self::Item + Clone + Send + 'static,
		Self::Item: Send + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::combine(Identity, f))
			.await
	}

	async fn max<P>(self, pool: &P) -> Option<Self::Item>
	where
		P: ThreadPool,
		Self::Item: Ord + Send + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::max(Identity))
			.await
	}

	async fn max_by<P, F>(self, pool: &P, f: F) -> Option<Self::Item>
	where
		P: ThreadPool,
		F: FnMut(&Self::Item, &Self::Item) -> Ordering + Clone + Send + 'static,
		Self::Item: Send + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::max_by(Identity, f))
			.await
	}

	async fn max_by_key<P, F, B>(self, pool: &P, f: F) -> Option<Self::Item>
	where
		P: ThreadPool,
		F: FnMut(&Self::Item) -> B + Clone + Send + 'static,
		B: Ord + 'static,
		Self::Item: Send + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::max_by_key(Identity, f))
			.await
	}

	async fn min<P>(self, pool: &P) -> Option<Self::Item>
	where
		P: ThreadPool,
		Self::Item: Ord + Send + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::min(Identity))
			.await
	}

	async fn min_by<P, F>(self, pool: &P, f: F) -> Option<Self::Item>
	where
		P: ThreadPool,
		F: FnMut(&Self::Item, &Self::Item) -> Ordering + Clone + Send + 'static,
		Self::Item: Send + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::min_by(Identity, f))
			.await
	}

	async fn min_by_key<P, F, B>(self, pool: &P, f: F) -> Option<Self::Item>
	where
		P: ThreadPool,
		F: FnMut(&Self::Item) -> B + Clone + Send + 'static,
		B: Ord + 'static,
		Self::Item: Send + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::min_by_key(Identity, f))
			.await
	}

	async fn most_frequent<P>(
		self, pool: &P, n: usize, probability: f64, tolerance: f64,
	) -> ::streaming_algorithms::Top<Self::Item, usize>
	where
		P: ThreadPool,
		Self::Item: Hash + Eq + Clone + Send + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(
			pool,
			ParallelPipe::<Self::Item>::most_frequent(Identity, n, probability, tolerance),
		)
		.await
	}

	async fn most_distinct<P, A, B>(
		self, pool: &P, n: usize, probability: f64, tolerance: f64, error_rate: f64,
	) -> ::streaming_algorithms::Top<A, streaming_algorithms::HyperLogLogMagnitude<B>>
	where
		P: ThreadPool,
		Self: ParallelStream<Item = (A, B)> + Sized,
		A: Hash + Eq + Clone + Send + 'static,
		B: Hash + 'static,
		Self::Task: 'static,
	{
		self.pipe(
			pool,
			ParallelPipe::<Self::Item>::most_distinct(
				Identity,
				n,
				probability,
				tolerance,
				error_rate,
			),
		)
		.await
	}

	async fn sample_unstable<P>(
		self, pool: &P, samples: usize,
	) -> ::streaming_algorithms::SampleUnstable<Self::Item>
	where
		P: ThreadPool,
		Self::Item: Send + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(
			pool,
			ParallelPipe::<Self::Item>::sample_unstable(Identity, samples),
		)
		.await
	}

	async fn all<P, F>(self, pool: &P, f: F) -> bool
	where
		P: ThreadPool,
		F: FnMut(Self::Item) -> bool + Clone + Send + 'static,
		Self::Item: 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::all(Identity, f))
			.await
	}

	async fn any<P, F>(self, pool: &P, f: F) -> bool
	where
		P: ThreadPool,
		F: FnMut(Self::Item) -> bool + Clone + Send + 'static,
		Self::Item: 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::any(Identity, f))
			.await
	}

	async fn collect<P, B>(self, pool: &P) -> B
	where
		P: ThreadPool,
		B: FromParallelStream<Self::Item>,
		B::ReduceA: Send + 'static,
		Self::Task: 'static,
		Self: Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::collect(Identity))
			.await
	}
}

#[inline(always)]
pub(crate) fn assert_parallel_stream<T, I: ParallelStream<Item = T>>(i: I) -> I {
	i
}
