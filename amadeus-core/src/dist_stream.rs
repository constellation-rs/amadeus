// TODO: P: Pool -> impl Pool: async_trait triggers https://github.com/rust-lang/rust/issues/71869

mod chain;
mod cloned;
mod filter;
mod flat_map;
mod identity;
mod inspect;
mod map;
mod sum_type;
mod update;

use ::sum::*;
use async_trait::async_trait;
use either::Either;
use futures::{pin_mut, ready, stream, stream::StreamExt, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use serde_closure::*;
use std::{
	cmp::Ordering, future::Future, hash::Hash, iter, marker::PhantomData, ops::FnMut, pin::Pin, task::{Context, Poll}, vec
};

use crate::{
	into_dist_stream::IntoDistributedStream, pool::{ProcessPool, ProcessSend, ThreadPool}, sink::{Sink, SinkMap}, util::type_coerce
};

pub use self::{
	chain::*, cloned::*, filter::*, flat_map::*, identity::*, inspect::*, map::*, update::*
};

use super::{dist_pipe::*, dist_sink::*};

#[must_use]
pub trait StreamTask {
	type Item;
	type Async: StreamTaskAsync<Item = Self::Item>;

	fn into_async(self) -> Self::Async;
}
#[must_use]
pub trait StreamTaskAsync {
	type Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, sink: Pin<&mut impl Sink<Item = Self::Item>>,
	) -> Poll<()>;
}

// TODO: how to dedup??

#[async_trait(?Send)]
#[must_use]
pub trait DistributedStream {
	type Item;
	type Task: StreamTask<Item = Self::Item> + ProcessSend;
	fn size_hint(&self) -> (usize, Option<usize>);
	fn next_task(&mut self) -> Option<Self::Task>;

	fn inspect<F>(self, f: F) -> Inspect<Self, F>
	where
		F: FnMut(&Self::Item) + Clone + ProcessSend,
		Self: Sized,
	{
		assert_distributed_stream(Inspect::new(self, f))
	}

	fn update<F>(self, f: F) -> Update<Self, F>
	where
		F: FnMut(&mut Self::Item) + Clone + ProcessSend,
		Self: Sized,
	{
		assert_distributed_stream(Update::new(self, f))
	}

	fn map<B, F>(self, f: F) -> Map<Self, F>
	where
		F: FnMut(Self::Item) -> B + Clone + ProcessSend,
		Self: Sized,
	{
		assert_distributed_stream(Map::new(self, f))
	}

	fn flat_map<B, F>(self, f: F) -> FlatMap<Self, F>
	where
		F: FnMut(Self::Item) -> B + Clone + ProcessSend,
		B: Stream,
		Self: Sized,
	{
		assert_distributed_stream(FlatMap::new(self, f))
	}

	fn filter<F, Fut>(self, f: F) -> Filter<Self, F>
	where
		F: FnMut(&Self::Item) -> Fut + Clone + ProcessSend,
		Fut: Future<Output = bool>,
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

	async fn reduce<P, B, R1F, R2F, R1, R2, R3>(
		mut self, pool: &P, reduce_a_factory: R1F, reduce_b_factory: R2F, reduce_c: R3,
	) -> B
	where
		P: ProcessPool,
		R1F: Factory<Item = R1> + Clone + ProcessSend,
		R2F: Factory<Item = R2>,
		R1: ReducerSend<Item = Self::Item> + ProcessSend,
		R2: ReducerProcessSend<Item = <R1 as Reducer>::Output> + ProcessSend,
		R3: Reducer<Item = <R2 as ReducerProcessSend>::Output, Output = B>,
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
				let reduce_b = reduce_b_factory.make();
				let reduce_a_factory = reduce_a_factory.clone();
				pool.spawn(FnOnce!(move |pool: &P::ThreadPool| {
					#[pin_project]
					struct Connect<B>(#[pin] B);
					impl<B> Sink for Connect<B>
					where
						B: ReducerAsync,
					{
						type Item = B::Item;

						fn poll_forward(
							self: Pin<&mut Self>, cx: &mut Context,
							stream: Pin<&mut impl Stream<Item = B::Item>>,
						) -> Poll<()> {
							let self_ = self.project();
							self_.0.poll_forward(cx, stream)
						}
					}

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
							let reduce_a = reduce_a_factory.make();
							pool.spawn(move || {
								let sink = Connect(reduce_a.into_async());
								async move {
									pin_mut!(sink);
									// TODO: short circuit
									for task in tasks {
										let task = task.into_async();
										pin_mut!(task);
										futures::future::poll_fn(|cx| {
											task.as_mut().poll_run(cx, sink.as_mut())
										})
										.await
									}
									futures::future::poll_fn(|cx| {
										sink.as_mut().project().0.as_mut().poll_output(cx)
									})
									.await
								}
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
						pin_mut!(stream);
						pin_mut!(reduce_b);
						futures::future::poll_fn(|cx| {
							ready!(reduce_b.as_mut().poll_forward(cx, stream.as_mut()));
							reduce_b.as_mut().poll_output(cx)
						})
						.await
					}
				}))
			})
			.collect::<futures::stream::FuturesUnordered<_>>();
		let stream = handles.map(|item| {
			item.unwrap_or_else(|err| panic!("Amadeus: task '<unnamed>' panicked at '{}'", err))
		});
		pin_mut!(stream);
		let reduce_c = reduce_c.into_async();
		pin_mut!(reduce_c);
		futures::future::poll_fn(|cx| {
			ready!(reduce_c.as_mut().poll_forward(cx, stream.as_mut()));
			reduce_c.as_mut().poll_output(cx)
		})
		.await
	}

	async fn pipe<P, DistSink, A>(self, pool: &P, sink: DistSink) -> A
	where
		P: ProcessPool,
		DistSink: DistributedSink<Self::Item, Output = A>,
		Self: Sized,
	{
		struct Connect<A, B>(A, B);
		impl<A: DistributedStream, B: DistributedPipe<A::Item>> DistributedStream for Connect<A, B> {
			type Item = B::Item;
			type Task = ConnectTask<A::Task, B::Task>;
			fn size_hint(&self) -> (usize, Option<usize>) {
				self.0.size_hint()
			}
			fn next_task(&mut self) -> Option<Self::Task> {
				self.0
					.next_task()
					.map(|task| ConnectTask(task, self.1.task()))
			}
		}
		#[derive(Serialize, Deserialize)]
		#[serde(
			bound(serialize = "A: Serialize, B: Serialize"),
			bound(deserialize = "A: Deserialize<'de>, B: Deserialize<'de>")
		)]
		struct ConnectTask<A, B>(A, B);
		impl<A, B> StreamTask for ConnectTask<A, B>
		where
			A: StreamTask,
			B: PipeTask<A::Item>,
		{
			type Item = B::Item;
			type Async = ConnectStreamTaskAsync<A::Async, B::Async>;
			fn into_async(self) -> Self::Async {
				ConnectStreamTaskAsync(self.0.into_async(), self.1.into_async())
			}
		}
		#[pin_project]
		struct ConnectStreamTaskAsync<A, B>(#[pin] A, #[pin] B);
		impl<A, B> StreamTaskAsync for ConnectStreamTaskAsync<A, B>
		where
			A: StreamTaskAsync,
			B: PipeTaskAsync<A::Item>,
		{
			type Item = B::Item;
			fn poll_run(
				self: Pin<&mut Self>, cx: &mut Context,
				sink: Pin<&mut impl Sink<Item = Self::Item>>,
			) -> Poll<()> {
				#[pin_project]
				struct Proxy<'a, I, B, Item>(#[pin] I, Pin<&'a mut B>, PhantomData<fn() -> Item>);
				impl<'a, I, B, Item> Sink for Proxy<'a, I, B, Item>
				where
					I: Sink<Item = B::Item>,
					B: PipeTaskAsync<Item>,
				{
					type Item = Item;

					fn poll_forward(
						self: Pin<&mut Self>, cx: &mut Context,
						stream: Pin<&mut impl Stream<Item = Self::Item>>,
					) -> Poll<()> {
						let self_ = self.project();
						self_.1.as_mut().poll_run(cx, stream, self_.0)
					}
				}
				let self_ = self.project();
				let sink = Proxy(sink, self_.1, PhantomData);
				pin_mut!(sink);
				self_.0.poll_run(cx, sink)
			}
		}

		let (iterator, reducer_a, reducer_b, reducer_c) = sink.reducers();
		Connect(self, iterator)
			.reduce(pool, reducer_a, reducer_b, reducer_c)
			.await
	}

	async fn pipe_fork<P, DistSinkA, DistSinkB, A, B>(
		self, pool: &P, sink_a: DistSinkA, sink_b: DistSinkB,
	) -> (A, B)
	where
		P: ProcessPool,

		DistSinkA: DistributedSink<Self::Item, Output = A>,
		DistSinkB: for<'a> DistributedSink<&'a Self::Item, Output = B>,

		Self::Item: 'static,
		Self: Sized,
	{
		struct Connect<A, B, C, RefAItem>(A, B, C, PhantomData<fn() -> RefAItem>);
		impl<A, B, C, RefAItem> DistributedStream for Connect<A, B, C, RefAItem>
		where
			A: DistributedStream,
			B: DistributedPipe<A::Item>,
			C: DistributedPipe<RefAItem>,
			RefAItem: 'static,
		{
			type Item = Sum2<B::Item, C::Item>;
			type Task = ConnectTask<A::Task, B::Task, C::Task, RefAItem>;
			fn size_hint(&self) -> (usize, Option<usize>) {
				self.0.size_hint()
			}
			fn next_task(&mut self) -> Option<Self::Task> {
				self.0
					.next_task()
					.map(|task| ConnectTask(task, self.1.task(), self.2.task(), PhantomData))
			}
		}
		#[derive(Serialize, Deserialize)]
		#[serde(
			bound(serialize = "A: Serialize, B: Serialize, C: Serialize"),
			bound(deserialize = "A: Deserialize<'de>, B: Deserialize<'de>, C: Deserialize<'de>")
		)]
		struct ConnectTask<A, B, C, RefAItem>(A, B, C, PhantomData<fn() -> RefAItem>);
		impl<A, B, C, RefAItem> StreamTask for ConnectTask<A, B, C, RefAItem>
		where
			A: StreamTask,
			B: PipeTask<A::Item>,
			C: PipeTask<RefAItem>,
		{
			type Item = Sum2<B::Item, C::Item>;
			type Async = ConnectStreamTaskAsync<A::Async, B::Async, C::Async, RefAItem, A::Item>;
			fn into_async(self) -> Self::Async {
				ConnectStreamTaskAsync(
					self.0.into_async(),
					self.1.into_async(),
					self.2.into_async(),
					false,
					None,
					PhantomData,
				)
			}
		}
		#[pin_project]
		struct ConnectStreamTaskAsync<A, B, C, RefAItem, T>(
			#[pin] A,
			#[pin] B,
			#[pin] C,
			bool,
			Option<Option<T>>,
			PhantomData<fn() -> RefAItem>,
		);
		impl<A, B, C, RefAItem> StreamTaskAsync for ConnectStreamTaskAsync<A, B, C, RefAItem, A::Item>
		where
			A: StreamTaskAsync,
			B: PipeTaskAsync<A::Item>,
			C: PipeTaskAsync<RefAItem>,
		{
			type Item = Sum2<B::Item, C::Item>;
			fn poll_run(
				self: Pin<&mut Self>, cx: &mut Context,
				sink: Pin<&mut impl Sink<Item = Self::Item>>,
			) -> Poll<()> {
				#[pin_project]
				pub struct SinkFn<'a, S, A, B, T, RefAItem>(
					#[pin] S,
					Pin<&'a mut A>,
					Pin<&'a mut B>,
					&'a mut bool,
					&'a mut Option<Option<T>>,
					PhantomData<fn() -> (T, RefAItem)>,
				);
				impl<'a, S, A, B, T, RefAItem> Sink for SinkFn<'a, S, A, B, T, RefAItem>
				where
					S: Sink<Item = Sum2<A::Item, B::Item>>,
					A: PipeTaskAsync<T>,
					B: PipeTaskAsync<RefAItem>,
				{
					type Item = T;

					fn poll_forward(
						self: Pin<&mut Self>, cx: &mut Context,
						mut stream: Pin<&mut impl Stream<Item = T>>,
					) -> Poll<()> {
						let mut self_ = self.project();
						let mut ready = (false, false);
						loop {
							if self_.4.is_none() {
								**self_.4 = match stream.as_mut().poll_next(cx) {
									Poll::Ready(x) => Some(x),
									Poll::Pending => None,
								};
							}
							let given = &mut **self_.3;
							let pending = &mut **self_.4;
							let mut progress = false;
							if !ready.0 {
								let stream = stream::poll_fn(|_cx| match pending {
									Some(x) if !*given => {
										// if x.is_some() {
										*given = true;
										progress = true;
										// }
										Poll::Ready(type_coerce(x.as_ref()))
									}
									_ => Poll::Pending,
								})
								.fuse();
								pin_mut!(stream);
								let sink_ = SinkMap::new(self_.0.as_mut(), |item| {
									Sum2::B(type_coerce(item))
								});
								pin_mut!(sink_);
								ready.0 = self_.2.as_mut().poll_run(cx, stream, sink_).is_ready();
								if ready.0 {
									progress = true; // TODO
								} else {
								}
							}
							if !ready.1 {
								let stream = stream::poll_fn(|_cx| {
									if !ready.0 && !*given {
										return Poll::Pending;
									}
									match pending.take() {
										Some(x) => {
											// *pending = Some(None);
											*given = false;
											progress = true;
											Poll::Ready(x)
										}
										None => Poll::Pending,
									}
								})
								.fuse();
								pin_mut!(stream);
								let sink_ = SinkMap::new(self_.0.as_mut(), Sum2::A);
								pin_mut!(sink_);
								ready.1 = self_.1.as_mut().poll_run(cx, stream, sink_).is_ready();
								if ready.1 {
									progress = true; // TODO
								} else {
								}
							}
							if ready.0 && ready.1 {
								break Poll::Ready(());
							}
							if !progress {
								break Poll::Pending;
							}
						}
					}
				}

				let self_ = self.project();
				let sink = SinkFn(sink, self_.1, self_.2, self_.3, self_.4, PhantomData);
				pin_mut!(sink);
				self_.0.poll_run(cx, sink)
			}
		}

		let (iterator_a, reducer_a_a, reducer_a_b, reducer_a_c) = sink_a.reducers();
		let (iterator_b, reducer_b_a, reducer_b_b, reducer_b_c) = sink_b.reducers();
		Connect(self, iterator_a, iterator_b, PhantomData)
			.reduce(
				pool,
				ReduceA2Factory(reducer_a_a, reducer_b_a),
				ReduceC2Factory(reducer_a_b, reducer_b_b),
				ReduceC2::new(reducer_a_c, reducer_b_c),
			)
			.await
	}

	async fn for_each<P, F>(self, pool: &P, f: F)
	where
		P: ProcessPool,
		F: FnMut(Self::Item) + Clone + ProcessSend,
		Self::Item: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::for_each(Identity, f))
			.await
	}

	async fn fold<P, ID, F, B>(self, pool: &P, identity: ID, op: F) -> B
	where
		P: ProcessPool,
		ID: FnMut() -> B + Clone + ProcessSend,
		F: FnMut(B, Either<Self::Item, B>) -> B + Clone + ProcessSend,
		B: ProcessSend,
		Self::Item: 'static,
		Self: Sized,
	{
		self.pipe(
			pool,
			DistributedPipe::<Self::Item>::fold(Identity, identity, op),
		)
		.await
	}

	async fn count<P>(self, pool: &P) -> usize
	where
		P: ProcessPool,
		Self::Item: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::count(Identity))
			.await
	}

	async fn sum<P, S>(self, pool: &P) -> S
	where
		P: ProcessPool,
		S: iter::Sum<Self::Item> + iter::Sum<S> + ProcessSend,
		Self::Item: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::sum(Identity))
			.await
	}

	async fn combine<P, F>(self, pool: &P, f: F) -> Option<Self::Item>
	where
		P: ProcessPool,
		F: FnMut(Self::Item, Self::Item) -> Self::Item + Clone + ProcessSend,
		Self::Item: ProcessSend,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::combine(Identity, f))
			.await
	}

	async fn max<P>(self, pool: &P) -> Option<Self::Item>
	where
		P: ProcessPool,
		Self::Item: Ord + ProcessSend,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::max(Identity))
			.await
	}

	async fn max_by<P, F>(self, pool: &P, f: F) -> Option<Self::Item>
	where
		P: ProcessPool,
		F: FnMut(&Self::Item, &Self::Item) -> Ordering + Clone + ProcessSend,
		Self::Item: ProcessSend,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::max_by(Identity, f))
			.await
	}

	async fn max_by_key<P, F, B>(self, pool: &P, f: F) -> Option<Self::Item>
	where
		P: ProcessPool,
		F: FnMut(&Self::Item) -> B + Clone + ProcessSend,
		B: Ord + 'static,
		Self::Item: ProcessSend,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::max_by_key(Identity, f))
			.await
	}

	async fn min<P>(self, pool: &P) -> Option<Self::Item>
	where
		P: ProcessPool,
		Self::Item: Ord + ProcessSend,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::min(Identity))
			.await
	}

	async fn min_by<P, F>(self, pool: &P, f: F) -> Option<Self::Item>
	where
		P: ProcessPool,
		F: FnMut(&Self::Item, &Self::Item) -> Ordering + Clone + ProcessSend,
		Self::Item: ProcessSend,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::min_by(Identity, f))
			.await
	}

	async fn min_by_key<P, F, B>(self, pool: &P, f: F) -> Option<Self::Item>
	where
		P: ProcessPool,
		F: FnMut(&Self::Item) -> B + Clone + ProcessSend,
		B: Ord + 'static,
		Self::Item: ProcessSend,
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
		Self::Item: Hash + Eq + Clone + ProcessSend,
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
		A: Hash + Eq + Clone + ProcessSend,
		B: Hash + 'static,
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
		Self::Item: ProcessSend,
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
		F: FnMut(Self::Item) -> bool + Clone + ProcessSend,
		Self::Item: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::all(Identity, f))
			.await
	}

	async fn any<P, F>(self, pool: &P, f: F) -> bool
	where
		P: ProcessPool,
		F: FnMut(Self::Item) -> bool + Clone + ProcessSend,
		Self::Item: 'static,
		Self: Sized,
	{
		self.pipe(pool, DistributedPipe::<Self::Item>::any(Identity, f))
			.await
	}

	async fn collect<P, B>(self, pool: &P) -> B
	where
		P: ProcessPool,
		B: FromDistributedStream<Self::Item>,
		B::ReduceA: ProcessSend,
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
	type Task: StreamTask<Item = Self::Item> + Send + 'static;
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

	fn filter<F, Fut>(self, f: F) -> Filter<Self, F>
	where
		F: FnMut(&Self::Item) -> Fut + Clone + Send + 'static,
		Fut: Future<Output = bool>,
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

	async fn reduce<P, B, R1F, R1, R3>(mut self, pool: &P, reduce_a_factory: R1F, reduce_c: R3) -> B
	where
		P: ThreadPool,
		R1F: Factory<Item = R1>,
		R1: ReducerSend<Item = Self::Item> + Send + 'static,
		R3: Reducer<Item = <R1 as ReducerSend>::Output, Output = B>,
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
				let reduce_a = reduce_a_factory.make();
				pool.spawn(FnOnce!(move || {
					#[pin_project]
					struct Connect<B>(#[pin] B);
					impl<B> Sink for Connect<B>
					where
						B: ReducerAsync,
					{
						type Item = B::Item;

						fn poll_forward(
							self: Pin<&mut Self>, cx: &mut Context,
							stream: Pin<&mut impl Stream<Item = B::Item>>,
						) -> Poll<()> {
							let self_ = self.project();
							self_.0.poll_forward(cx, stream)
						}
					}

					let sink = Connect(reduce_a.into_async());
					async move {
						pin_mut!(sink);
						// TODO: short circuit
						for task in tasks {
							let task = task.into_async();
							pin_mut!(task);
							futures::future::poll_fn(|cx| task.as_mut().poll_run(cx, sink.as_mut()))
								.await
						}
						futures::future::poll_fn(|cx| {
							sink.as_mut().project().0.as_mut().poll_output(cx)
						})
						.await
					}
				}))
			})
			.collect::<futures::stream::FuturesUnordered<_>>();
		let stream = handles.map(|item| {
			item.unwrap_or_else(|err| panic!("Amadeus: task '<unnamed>' panicked at '{}'", err))
		});
		pin_mut!(stream);
		let reduce_c = reduce_c.into_async();
		pin_mut!(reduce_c);
		futures::future::poll_fn(|cx| {
			ready!(reduce_c.as_mut().poll_forward(cx, stream.as_mut()));
			reduce_c.as_mut().poll_output(cx)
		})
		.await
	}

	async fn pipe<P, ParSink, A>(self, pool: &P, sink: ParSink) -> A
	where
		P: ThreadPool,
		ParSink: ParallelSink<Self::Item, Output = A>,
		Self: Sized,
	{
		struct Connect<A, B>(A, B);
		impl<A: ParallelStream, B: ParallelPipe<A::Item>> ParallelStream for Connect<A, B> {
			type Item = B::Item;
			type Task = ConnectTask<A::Task, B::Task>;
			fn size_hint(&self) -> (usize, Option<usize>) {
				self.0.size_hint()
			}
			fn next_task(&mut self) -> Option<Self::Task> {
				self.0
					.next_task()
					.map(|task| ConnectTask(task, self.1.task()))
			}
		}
		struct ConnectTask<A, B>(A, B);
		impl<A, B> StreamTask for ConnectTask<A, B>
		where
			A: StreamTask,
			B: PipeTask<A::Item>,
		{
			type Item = B::Item;
			type Async = ConnectStreamTaskAsync<A::Async, B::Async>;
			fn into_async(self) -> Self::Async {
				ConnectStreamTaskAsync(self.0.into_async(), self.1.into_async())
			}
		}
		#[pin_project]
		struct ConnectStreamTaskAsync<A, B>(#[pin] A, #[pin] B);
		impl<A, B> StreamTaskAsync for ConnectStreamTaskAsync<A, B>
		where
			A: StreamTaskAsync,
			B: PipeTaskAsync<A::Item>,
		{
			type Item = B::Item;
			fn poll_run(
				self: Pin<&mut Self>, cx: &mut Context,
				sink: Pin<&mut impl Sink<Item = Self::Item>>,
			) -> Poll<()> {
				#[pin_project]
				struct Proxy<'a, I, B, Item>(#[pin] I, Pin<&'a mut B>, PhantomData<fn() -> Item>);
				impl<'a, I, B, Item> Sink for Proxy<'a, I, B, Item>
				where
					I: Sink<Item = B::Item>,
					B: PipeTaskAsync<Item>,
				{
					type Item = Item;

					fn poll_forward(
						self: Pin<&mut Self>, cx: &mut Context,
						stream: Pin<&mut impl Stream<Item = Self::Item>>,
					) -> Poll<()> {
						let self_ = self.project();
						self_.1.as_mut().poll_run(cx, stream, self_.0)
					}
				}
				let self_ = self.project();
				let sink = Proxy(sink, self_.1, PhantomData);
				pin_mut!(sink);
				self_.0.poll_run(cx, sink)
			}
		}

		let (iterator, reducer_a, reducer_b) = sink.reducers();
		Connect(self, iterator)
			.reduce(pool, reducer_a, reducer_b)
			.await
	}

	async fn pipe_fork<P, ParSinkA, ParSinkB, A, B>(
		self, pool: &P, sink_a: ParSinkA, sink_b: ParSinkB,
	) -> (A, B)
	where
		P: ThreadPool,

		ParSinkA: ParallelSink<Self::Item, Output = A>,
		ParSinkB: for<'a> ParallelSink<&'a Self::Item, Output = B>,

		Self::Item: 'static,
		Self: Sized,
	{
		struct Connect<A, B, C, RefAItem>(A, B, C, PhantomData<fn() -> RefAItem>);
		impl<A, B, C, RefAItem> ParallelStream for Connect<A, B, C, RefAItem>
		where
			A: ParallelStream,
			B: ParallelPipe<A::Item>,
			C: ParallelPipe<RefAItem>,
			RefAItem: 'static,
		{
			type Item = Sum2<B::Item, C::Item>;
			type Task = ConnectTask<A::Task, B::Task, C::Task, RefAItem>;
			fn size_hint(&self) -> (usize, Option<usize>) {
				self.0.size_hint()
			}
			fn next_task(&mut self) -> Option<Self::Task> {
				self.0
					.next_task()
					.map(|task| ConnectTask(task, self.1.task(), self.2.task(), PhantomData))
			}
		}
		struct ConnectTask<A, B, C, RefAItem>(A, B, C, PhantomData<fn() -> RefAItem>);
		impl<A, B, C, RefAItem> StreamTask for ConnectTask<A, B, C, RefAItem>
		where
			A: StreamTask,
			B: PipeTask<A::Item>,
			C: PipeTask<RefAItem>,
		{
			type Item = Sum2<B::Item, C::Item>;
			type Async = ConnectStreamTaskAsync<A::Async, B::Async, C::Async, RefAItem, A::Item>;
			fn into_async(self) -> Self::Async {
				ConnectStreamTaskAsync(
					self.0.into_async(),
					self.1.into_async(),
					self.2.into_async(),
					false,
					None,
					PhantomData,
				)
			}
		}
		#[pin_project]
		struct ConnectStreamTaskAsync<A, B, C, RefAItem, T>(
			#[pin] A,
			#[pin] B,
			#[pin] C,
			bool,
			Option<Option<T>>,
			PhantomData<fn() -> RefAItem>,
		);
		impl<A, B, C, RefAItem> StreamTaskAsync for ConnectStreamTaskAsync<A, B, C, RefAItem, A::Item>
		where
			A: StreamTaskAsync,
			B: PipeTaskAsync<A::Item>,
			C: PipeTaskAsync<RefAItem>,
		{
			type Item = Sum2<B::Item, C::Item>;
			fn poll_run(
				self: Pin<&mut Self>, cx: &mut Context,
				sink: Pin<&mut impl Sink<Item = Self::Item>>,
			) -> Poll<()> {
				#[pin_project]
				pub struct SinkFn<'a, S, A, B, T, RefAItem>(
					#[pin] S,
					Pin<&'a mut A>,
					Pin<&'a mut B>,
					&'a mut bool,
					&'a mut Option<Option<T>>,
					PhantomData<fn() -> (T, RefAItem)>,
				);
				impl<'a, S, A, B, T, RefAItem> Sink for SinkFn<'a, S, A, B, T, RefAItem>
				where
					S: Sink<Item = Sum2<A::Item, B::Item>>,
					A: PipeTaskAsync<T>,
					B: PipeTaskAsync<RefAItem>,
				{
					type Item = T;

					fn poll_forward(
						self: Pin<&mut Self>, cx: &mut Context,
						mut stream: Pin<&mut impl Stream<Item = T>>,
					) -> Poll<()> {
						let mut self_ = self.project();
						let mut ready = (false, false);
						loop {
							if self_.4.is_none() {
								**self_.4 = match stream.as_mut().poll_next(cx) {
									Poll::Ready(x) => Some(x),
									Poll::Pending => None,
								};
							}
							let given = &mut **self_.3;
							let pending = &mut **self_.4;
							let mut progress = false;
							if !ready.0 {
								let stream = stream::poll_fn(|_cx| match pending {
									Some(x) if !*given => {
										// if x.is_some() {
										*given = true;
										progress = true;
										// }
										Poll::Ready(type_coerce(x.as_ref()))
									}
									_ => Poll::Pending,
								})
								.fuse();
								pin_mut!(stream);
								let sink_ = SinkMap::new(self_.0.as_mut(), |item| {
									Sum2::B(type_coerce(item))
								});
								pin_mut!(sink_);
								ready.0 = self_.2.as_mut().poll_run(cx, stream, sink_).is_ready();
								if ready.0 {
									progress = true; // TODO
								} else {
								}
							}
							if !ready.1 {
								let stream = stream::poll_fn(|_cx| {
									if !ready.0 && !*given {
										return Poll::Pending;
									}
									match pending.take() {
										Some(x) => {
											// *pending = Some(None);
											*given = false;
											progress = true;
											Poll::Ready(x)
										}
										None => Poll::Pending,
									}
								})
								.fuse();
								pin_mut!(stream);
								let sink_ = SinkMap::new(self_.0.as_mut(), Sum2::A);
								pin_mut!(sink_);
								ready.1 = self_.1.as_mut().poll_run(cx, stream, sink_).is_ready();
								if ready.1 {
									progress = true; // TODO
								} else {
								}
							}
							if ready.0 && ready.1 {
								break Poll::Ready(());
							}
							if !progress {
								break Poll::Pending;
							}
						}
					}
				}

				let self_ = self.project();
				let sink = SinkFn(sink, self_.1, self_.2, self_.3, self_.4, PhantomData);
				pin_mut!(sink);
				self_.0.poll_run(cx, sink)
			}
		}

		let (iterator_a, reducer_a_a, reducer_a_b) = sink_a.reducers();
		let (iterator_b, reducer_b_a, reducer_b_b) = sink_b.reducers();
		Connect(self, iterator_a, iterator_b, PhantomData)
			.reduce(
				pool,
				ReduceA2Factory(reducer_a_a, reducer_b_a),
				ReduceC2::new(reducer_a_b, reducer_b_b),
			)
			.await
	}

	async fn for_each<P, F>(self, pool: &P, f: F)
	where
		P: ThreadPool,
		F: FnMut(Self::Item) + Clone + Send + 'static,
		Self::Item: 'static,
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
		Self: Sized,
	{
		self.pipe(
			pool,
			ParallelPipe::<Self::Item>::fold(Identity, identity, op),
		)
		.await
	}

	async fn count<P>(self, pool: &P) -> usize
	where
		P: ThreadPool,
		Self::Item: 'static,
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
		Self: Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::combine(Identity, f))
			.await
	}

	async fn max<P>(self, pool: &P) -> Option<Self::Item>
	where
		P: ThreadPool,
		Self::Item: Ord + Send + 'static,
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
		Self: Sized,
	{
		self.pipe(pool, ParallelPipe::<Self::Item>::max_by_key(Identity, f))
			.await
	}

	async fn min<P>(self, pool: &P) -> Option<Self::Item>
	where
		P: ThreadPool,
		Self::Item: Ord + Send + 'static,
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
