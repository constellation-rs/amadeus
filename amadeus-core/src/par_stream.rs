// TODO: P: Pool -> impl Pool: async_trait triggers https://github.com/rust-lang/rust/issues/71869
// TODO: how to dedup??

#![allow(clippy::too_many_lines, unused_qualifications)]

mod chain;
mod cloned;
mod filter;
mod filter_map_sync;
mod flat_map;
mod flat_map_sync;
mod identity;
mod inspect;
mod join;
mod map;
mod map_sync;
mod sum_type;
mod update;

use async_trait::async_trait;
use either::Either;
use futures::{future, pin_mut, stream::StreamExt as _, Stream};
use indexmap::IndexMap;
use serde_closure::{traits, FnOnce};
use std::{
	cmp::Ordering, hash::Hash, iter, ops, pin::Pin, task::{Context, Poll}, vec
};

use super::{par_pipe::*, par_sink::*};
use crate::{
	into_par_stream::{IntoDistributedStream, IntoParallelStream}, pipe::{Sink, StreamExt}, pool::{ProcessPool, ProcessSend, ThreadPool}
};

pub use self::{
	chain::*, cloned::*, filter::*, filter_map_sync::*, flat_map::*, flat_map_sync::*, identity::*, inspect::*, join::*, map::*, map_sync::*, update::*
};

#[must_use]
pub trait StreamTask {
	type Item;
	type Async: Stream<Item = Self::Item>;

	fn into_async(self) -> Self::Async;
}

macro_rules! stream {
	($stream:ident $pipe:ident $sink:ident $from_stream:ident $into_stream:ident $into_stream_fn:ident $xxx:ident $pool:ident $send:ident $fns:ident $assert_stream:ident $($meta:meta)* { $($items:item)* }) => {
		#[async_trait(?Send)]
		$(#[$meta])*
		#[must_use]
		pub trait $stream {
			type Item;
			type Task: StreamTask<Item = Self::Item> + $send;

			fn next_task(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Task>>;
			fn size_hint(&self) -> (usize, Option<usize>);

			$($items)*

			#[inline]
			fn inspect<F>(self, f: F) -> Inspect<Self, F>
			where
				F: $fns::FnMut(&Self::Item) + Clone + $send,
				Self: Sized,
			{
				$assert_stream(Inspect::new(self, f))
			}

			#[inline]
			fn update<F>(self, f: F) -> Update<Self, F>
			where
				F: $fns::FnMut(&mut Self::Item) + Clone + $send,
				Self: Sized,
			{
				$assert_stream(Update::new(self, f))
			}

			#[inline]
			fn map<B, F>(self, f: F) -> Map<Self, F>
			where
				F: $fns::FnMut(Self::Item) -> B + Clone + $send,
				Self: Sized,
			{
				$assert_stream(Map::new(self, f))
			}

			#[inline]
			fn flat_map<B, F>(self, f: F) -> FlatMap<Self, F>
			where
				F: $fns::FnMut(Self::Item) -> B + Clone + $send,
				B: Stream,
				Self: Sized,
			{
				$assert_stream(FlatMap::new(self, f))
			}

			#[inline]
			fn filter<F>(self, f: F) -> Filter<Self, F>
			where
				F: $fns::FnMut(&Self::Item) -> bool + Clone + $send,
				Self: Sized,
			{
				$assert_stream(Filter::new(self, f))
			}

			#[inline]
			fn left_join<K, V1, V2>(self, right: impl IntoIterator<Item = (K, V2)>) -> LeftJoin<Self, K, V1, V2>
			where
				K: Eq + Hash + Clone + $send,
				V2: Clone + $send,
				Self: $stream<Item = (K, V1)> + Sized,
			{
				$assert_stream(LeftJoin::new(self, right.into_iter().collect()))
			}

			#[inline]
			fn inner_join<K, V1, V2>(self, right: impl IntoIterator<Item = (K, V2)>) -> InnerJoin<Self, K, V1, V2>
			where
				K: Eq + Hash + Clone + $send,
				V2: Clone + $send,
				Self: $stream<Item = (K, V1)> + Sized,
			{
				$assert_stream(InnerJoin::new(self, right.into_iter().collect()))
			}

			#[inline]
			fn chain<C>(self, chain: C) -> Chain<Self, C::$xxx>
			where
				C: $into_stream<Item = Self::Item>,
				Self: Sized,
			{
				$assert_stream(Chain::new(self, chain.$into_stream_fn()))
			}

			#[inline]
			async fn for_each<P, F>(self, pool: &P, f: F)
			where
				P: $pool,
				F: $fns::FnMut(Self::Item) + Clone + $send,


				Self: Sized,
			{
				self.pipe(pool, $pipe::<Self::Item>::for_each(Identity, f))
					.await
			}

			#[inline]
			async fn fold<P, ID, F, B>(self, pool: &P, identity: ID, op: F) -> B
			where
				P: $pool,
				ID: $fns::FnMut() -> B + Clone + $send,
				F: $fns::FnMut(B, Either<Self::Item, B>) -> B + Clone + $send,
				B: $send,


				Self: Sized,
			{
				self.pipe(
					pool,
					$pipe::<Self::Item>::fold(Identity, identity, op),
				)
				.await
			}

			#[inline]
			async fn histogram<P>(self, pool: &P) -> Vec<(Self::Item, usize)>
			where
				P: $pool,
				Self::Item: Hash + Ord + $send,

				Self: Sized,
			{
				self.pipe(pool, $pipe::<Self::Item>::histogram(Identity))
					.await
			}

			#[inline]
			async fn sort_n_by<P, F>(self, pool: &P, n: usize, cmp: F) -> ::amadeus_streaming::Sort<Self::Item, F>
			where
				P: $pool,
				F: $fns::Fn(&Self::Item, &Self::Item) -> Ordering + Clone + $send,
				Self::Item: Clone + $send,
				Self: Sized,
			{
				self.pipe(pool, $pipe::<Self::Item>::sort_n_by(Identity, n, cmp))
					.await
			}

			#[inline]
			async fn count<P>(self, pool: &P) -> usize
			where
				P: $pool,


				Self: Sized,
			{
				self.pipe(pool, $pipe::<Self::Item>::count(Identity))
					.await
			}

			#[inline]
			async fn sum<P, S>(self, pool: &P) -> S
			where
				P: $pool,
				S: iter::Sum<Self::Item> + iter::Sum<S> + $send,


				Self: Sized,
			{
				self.pipe(pool, $pipe::<Self::Item>::sum(Identity))
					.await
			}

			#[inline]
			async fn mean<P>(self, pool: &P) -> f64
			where
				P: $pool,
				Self: $stream<Item = f64> + Sized,
			{
				self.pipe(pool, $pipe::<Self::Item>::mean(Identity))
				.await
			}

			#[inline]
			async fn stddev<P>(self, pool: &P) -> f64
			where
				P: $pool,
				Self: $stream<Item = f64> + Sized,
			{
				self.pipe(pool, $pipe::<Self::Item>::stddev(Identity))
				.await
			}

			#[inline]
			async fn combine<P, F>(self, pool: &P, f: F) -> Option<Self::Item>
			where
				P: $pool,
				F: $fns::FnMut(Self::Item, Self::Item) -> Self::Item + Clone + $send,
				Self::Item: $send,

				Self: Sized,
			{
				self.pipe(pool, $pipe::<Self::Item>::combine(Identity, f))
					.await
			}

			#[inline]
			async fn max<P>(self, pool: &P) -> Option<Self::Item>
			where
				P: $pool,
				Self::Item: Ord + $send,

				Self: Sized,
			{
				self.pipe(pool, $pipe::<Self::Item>::max(Identity))
					.await
			}

			#[inline]
			async fn max_by<P, F>(self, pool: &P, f: F) -> Option<Self::Item>
			where
				P: $pool,
				F: $fns::FnMut(&Self::Item, &Self::Item) -> Ordering + Clone + $send,
				Self::Item: $send,

				Self: Sized,
			{
				self.pipe(pool, $pipe::<Self::Item>::max_by(Identity, f))
					.await
			}

			#[inline]
			async fn max_by_key<P, F, B>(self, pool: &P, f: F) -> Option<Self::Item>
			where
				P: $pool,
				F: $fns::FnMut(&Self::Item) -> B + Clone + $send,
				B: Ord,
				Self::Item: $send,

				Self: Sized,
			{
				self.pipe(pool, $pipe::<Self::Item>::max_by_key(Identity, f))
					.await
			}

			#[inline]
			async fn min<P>(self, pool: &P) -> Option<Self::Item>
			where
				P: $pool,
				Self::Item: Ord + $send,

				Self: Sized,
			{
				self.pipe(pool, $pipe::<Self::Item>::min(Identity))
					.await
			}

			#[inline]
			async fn min_by<P, F>(self, pool: &P, f: F) -> Option<Self::Item>
			where
				P: $pool,
				F: $fns::FnMut(&Self::Item, &Self::Item) -> Ordering + Clone + $send,
				Self::Item: $send,

				Self: Sized,
			{
				self.pipe(pool, $pipe::<Self::Item>::min_by(Identity, f))
					.await
			}

			#[inline]
			async fn min_by_key<P, F, B>(self, pool: &P, f: F) -> Option<Self::Item>
			where
				P: $pool,
				F: $fns::FnMut(&Self::Item) -> B + Clone + $send,
				B: Ord,
				Self::Item: $send,

				Self: Sized,
			{
				self.pipe(pool, $pipe::<Self::Item>::min_by_key(Identity, f))
					.await
			}

			#[inline]
			async fn most_frequent<P>(
				self, pool: &P, n: usize, probability: f64, tolerance: f64,
			) -> ::amadeus_streaming::Top<Self::Item, usize>
			where
				P: $pool,
				Self::Item: Hash + Eq + Clone + $send,

				Self: Sized,
			{
				self.pipe(
					pool,
					$pipe::<Self::Item>::most_frequent(Identity, n, probability, tolerance),
				)
				.await
			}

			#[inline]
			async fn most_distinct<P, A, B>(
				self, pool: &P, n: usize, probability: f64, tolerance: f64, error_rate: f64,
			) -> ::amadeus_streaming::Top<A, amadeus_streaming::HyperLogLogMagnitude<B>>
			where
				P: $pool,
				Self: $stream<Item = (A, B)> + Sized,
				A: Hash + Eq + Clone + $send,
				B: Hash,

			{
				self.pipe(
					pool,
					$pipe::<Self::Item>::most_distinct(
						Identity,
						n,
						probability,
						tolerance,
						error_rate,
					),
				)
				.await
			}

			#[inline]
			async fn sample_unstable<P>(
				self, pool: &P, samples: usize,
			) -> ::amadeus_streaming::SampleUnstable<Self::Item>
			where
				P: $pool,
				Self::Item: $send,

				Self: Sized,
			{
				self.pipe(
					pool,
					$pipe::<Self::Item>::sample_unstable(Identity, samples),
				)
				.await
			}

			#[inline]
			async fn all<P, F>(self, pool: &P, f: F) -> bool
			where
				P: $pool,
				F: $fns::FnMut(Self::Item) -> bool + Clone + $send,


				Self: Sized,
			{
				self.pipe(pool, $pipe::<Self::Item>::all(Identity, f))
					.await
			}

			#[inline]
			async fn any<P, F>(self, pool: &P, f: F) -> bool
			where
				P: $pool,
				F: $fns::FnMut(Self::Item) -> bool + Clone + $send,


				Self: Sized,
			{
				self.pipe(pool, $pipe::<Self::Item>::any(Identity, f))
					.await
			}
		}

		#[inline(always)]
		pub(crate) fn $assert_stream<T, I: $stream<Item = T>>(i: I) -> I {
			i
		}
	}
}

stream!(ParallelStream ParallelPipe ParallelSink FromParallelStream IntoParallelStream into_par_stream ParStream ThreadPool Send ops assert_parallel_stream {
	async fn reduce_owned<P, B, R1, R3>(mut self, pool: &P, reduce_a: R1, reduce_c: R3) -> B
	where
		P: ThreadPool,
		R1: ReducerSend<Self::Item> + Clone + Send + 'static,
		R3: Reducer<<R1 as ReducerSend<Self::Item>>::Done, Done = B>,

		Self::Task: 'static,
		<R1 as Reducer<Self::Item>>::Done: 'static,

		Self: Sized,
	{
		let self_ = self;
		pin_mut!(self_);
		// TODO: don't buffer tasks before sending. requires changes to ThreadPool
		let mut tasks = (0..pool.threads()).map(|_| vec![]).collect::<Vec<_>>();
		let mut allocated = 0;
		'a: loop {
			for i in 0..tasks.len() {
				loop {
					let (mut lower, _upper) = self_.size_hint();
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
						if let Some(task) = future::poll_fn(|cx| self_.as_mut().next_task(cx)).await {
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
				let reduce_a = reduce_a.clone();
				let spawn = move || async move {
					let sink = reduce_a.into_async();
					pin_mut!(sink);
					// this is faster than stream::iter(tasks.into_iter().map(StreamTask::into_async)).flatten().sink(sink).await
					for task in tasks.into_iter().map(StreamTask::into_async) {
						pin_mut!(task);
						if let Some(ret) = sink.send_all(&mut task).await {
							return ret;
						}
					}
					sink.done().await
				};
				pool.spawn(spawn)
			})
			.collect::<futures::stream::FuturesUnordered<_>>();
		let stream = handles.map(|item| {
			item.unwrap_or_else(|err| panic!("Amadeus: task '<unnamed>' panicked at '{}'", err))
		});
		let reduce_c = reduce_c.into_async();
		pin_mut!(reduce_c);
		stream.sink(reduce_c).await
	}

	async fn reduce<P, B, R1, R3>(mut self, pool: &P, reduce_a: R1, reduce_c: R3) -> B
	where
		P: ThreadPool,
		R1: ReducerSend<Self::Item> + Clone + Send,
		R3: Reducer<<R1 as ReducerSend<Self::Item>>::Done, Done = B>,

		Self: Sized,
	{
		let self_ = self;
		pin_mut!(self_);
		// TODO: don't buffer tasks before sending. requires changes to ThreadPool
		let mut tasks = (0..pool.threads()).map(|_| vec![]).collect::<Vec<_>>();
		let mut allocated = 0;
		'a: loop {
			for i in 0..tasks.len() {
				loop {
					let (mut lower, _upper) = self_.size_hint();
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
						if let Some(task) = future::poll_fn(|cx| self_.as_mut().next_task(cx)).await {
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
				let reduce_a = reduce_a.clone();
				let spawn = move || async move {
					let sink = reduce_a.into_async();
					pin_mut!(sink);
					// this is faster than stream::iter(tasks.into_iter().map(StreamTask::into_async)).flatten().sink(sink).await
					for task in tasks.into_iter().map(StreamTask::into_async) {
						pin_mut!(task);
						if let Some(ret) = sink.send_all(&mut task).await {
							return ret;
						}
					}
					sink.done().await
				};
				#[allow(unsafe_code)]
				unsafe { pool.spawn_unchecked(spawn) }
			})
			.collect::<futures::stream::FuturesUnordered<_>>();
		let stream = handles.map(|item| {
			item.unwrap_or_else(|err| panic!("Amadeus: task '<unnamed>' panicked at '{}'", err))
		});
		let reduce_c = reduce_c.into_async();
		pin_mut!(reduce_c);
		let rt = tokio::runtime::Handle::current();
		tokio::task::block_in_place(|| rt.block_on(stream.sink(reduce_c)))
	}

	async fn pipe<P, ParSink, A>(self, pool: &P, sink: ParSink) -> A
	where
		P: ThreadPool,
		ParSink: ParallelSink<Self::Item, Done = A>,
		Self: Sized,
	{
		let (iterator, reducer_a, reducer_b) = sink.reducers();
		Pipe::new(self, iterator)
			.reduce(pool, reducer_a, reducer_b)
			.await
	}

	async fn fork<P, ParSinkA, ParSinkB, A, B>(
		self, pool: &P, sink_a: ParSinkA, sink_b: ParSinkB,
	) -> (A, B)
	where
		P: ThreadPool,
		ParSinkA: ParallelSink<Self::Item, Done = A>,
		ParSinkB: for<'a> ParallelSink<&'a Self::Item, Done = B>,
		Self::Item: 'static,
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

	async fn group_by<P, S, A, B>(self, pool: &P, sink: S) -> IndexMap<A, S::Done>
	where
		P: ThreadPool,
		A: Eq + Hash + Send,
		S: ParallelSink<B>,
		<S::Pipe as ParallelPipe<B>>::Task: Clone + Send,
		S::ReduceC: Clone,
		S::Done: Send,

		Self: ParallelStream<Item = (A, B)> + Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::group_by(Identity, sink))
			.await
	}

	async fn collect<P, B>(self, pool: &P) -> B
	where
		P: ThreadPool,
		B: FromParallelStream<Self::Item>,
		B::ReduceA: Send,

		Self: Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::collect(Identity))
			.await
	}
});

stream!(DistributedStream DistributedPipe DistributedSink FromDistributedStream IntoDistributedStream into_dist_stream DistStream ProcessPool ProcessSend traits assert_distributed_stream cfg_attr(not(nightly), serde_closure::desugar) {
	async fn reduce<P, B, R1, R2, R3>(
		mut self, pool: &P, reduce_a: R1, reduce_b: R2, reduce_c: R3,
	) -> B
	where
		P: ProcessPool,
		R1: ReducerSend<Self::Item> + Clone + ProcessSend,
		R2: ReducerProcessSend<<R1 as ReducerSend<Self::Item>>::Done>
			+ Clone
			+ ProcessSend,
		R3: Reducer<
			<R2 as ReducerProcessSend<<R1 as ReducerSend<Self::Item>>::Done>>::Done,
			Done = B,
		>,

		Self: Sized,
	{
		let self_ = self;
		pin_mut!(self_);
		// TODO: don't buffer tasks before sending. requires changes to ProcessPool
		let mut tasks = (0..pool.processes()).map(|_| vec![]).collect::<Vec<_>>();
		let mut allocated = 0;
		'a: loop {
			for i in 0..tasks.len() {
				loop {
					let (mut lower, _upper) = self_.size_hint();
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
						if let Some(task) = future::poll_fn(|cx| self_.as_mut().next_task(cx)).await {
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
				let reduce_b = reduce_b.clone();
				let reduce_a = reduce_a.clone();
				let spawn = FnOnce!(move |pool: &P::ThreadPool| {
					let reduce_b = reduce_b;
					let reduce_a = reduce_a;
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
							let reduce_a = reduce_a.clone();
							let spawn = move || async move {
								let sink = reduce_a.into_async();
								pin_mut!(sink);
								// this is faster than stream::iter(tasks.into_iter().map(StreamTask::into_async)).flatten().sink(sink).await
								for task in tasks.into_iter().map(StreamTask::into_async) {
									pin_mut!(task);
									if let Some(ret) = sink.send_all(&mut task).await {
										return ret;
									}
								}
								sink.done().await
							};
							#[allow(unsafe_code)]
							unsafe{pool.spawn_unchecked(spawn)}
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
						// TODO: these currently can't be cancelled. when they can be, we'll need to block
						stream.sink(reduce_b).await
					}
				});
				#[allow(unsafe_code)]
				unsafe { pool.spawn_unchecked(spawn) }
			})
			.collect::<futures::stream::FuturesUnordered<_>>();
		let stream = handles.map(|item| {
			item.unwrap_or_else(|err| panic!("Amadeus: task '<unnamed>' panicked at '{}'", err))
		});
		let reduce_c = reduce_c.into_async();
		pin_mut!(reduce_c);
		let rt = tokio::runtime::Handle::current();
		tokio::task::block_in_place(|| rt.block_on(stream.sink(reduce_c)))
	}

	async fn pipe<P, DistSink, A>(self, pool: &P, sink: DistSink) -> A
	where
		P: ProcessPool,
		DistSink: DistributedSink<Self::Item, Done = A>,
		Self: Sized,
	{
		let (iterator, reducer_a, reducer_b, reducer_c) = sink.reducers();
		Pipe::new(self, iterator)
			.reduce(pool, reducer_a, reducer_b, reducer_c)
			.await
	}

	async fn fork<P, DistSinkA, DistSinkB, A, B>(
		self, pool: &P, sink_a: DistSinkA, sink_b: DistSinkB,
	) -> (A, B)
	where
		P: ProcessPool,
		DistSinkA: DistributedSink<Self::Item, Done = A>,
		DistSinkB: for<'a> DistributedSink<&'a Self::Item, Done = B>,
		Self::Item: 'static,
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

	async fn group_by<P, S, A, B>(self, pool: &P, sink: S) -> IndexMap<A, S::Done>
	where
		P: ProcessPool,
		A: Eq + Hash + ProcessSend,
		S: DistributedSink<B>,
		<S::Pipe as DistributedPipe<B>>::Task: Clone + ProcessSend,
		S::ReduceC: Clone,
		S::Done: ProcessSend,

		Self: DistributedStream<Item = (A, B)> + Sized,
	{
		self.pipe(
			pool,
			DistributedPipe::<Self::Item>::group_by(Identity, sink),
		)
		.await
	}

	async fn collect<P, B>(self, pool: &P) -> B
	where
		P: ProcessPool,
		B: FromDistributedStream<Self::Item>,
		B::ReduceA: ProcessSend,
		B::ReduceB: ProcessSend,

		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::collect(Identity))
			.await
	}
});
