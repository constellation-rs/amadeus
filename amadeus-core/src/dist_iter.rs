// TODO: P: Pool -> impl Pool: async_trait triggers https://github.com/rust-lang/rust/issues/71869

mod all;
mod any;
mod chain;
mod cloned;
mod collect;
mod combine;
mod count;
mod filter;
mod flat_map;
mod fold;
mod for_each;
mod identity;
mod inspect;
mod map;
mod max;
mod sample;
mod sum;
mod sum_type;
mod tuple;
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
	into_dist_iter::IntoDistributedIterator, pool::{ProcessPool, ProcessSend, ThreadPool}, sink::{Sink, SinkMap}, util::type_coerce
};

pub use self::{
	all::*, any::*, chain::*, cloned::*, collect::*, combine::*, count::*, filter::*, flat_map::*, fold::*, for_each::*, identity::*, inspect::*, map::*, max::*, sample::*, sum::*, tuple::*, update::*
};

#[inline(always)]
fn _assert_distributed_iterator<T, I: DistributedIterator<Item = T>>(i: I) -> I {
	i
}
#[inline(always)]
fn _assert_distributed_iterator_multi<T, I: DistributedIteratorMulti<Source, Item = T>, Source>(
	i: I,
) -> I {
	i
}
#[inline(always)]
fn _assert_distributed_reducer<
	T,
	R: DistributedReducer<I, Source, T>,
	I: DistributedIteratorMulti<Source>,
	Source,
>(
	r: R,
) -> R {
	r
}

#[async_trait(?Send)]
pub trait DistributedIterator {
	type Item;
	type Task: Consumer<Item = Self::Item> + ProcessSend;
	fn size_hint(&self) -> (usize, Option<usize>);
	fn next_task(&mut self) -> Option<Self::Task>;

	fn inspect<F>(self, f: F) -> Inspect<Self, F>
	where
		F: FnMut(&Self::Item) + Clone + ProcessSend,
		Self: Sized,
	{
		_assert_distributed_iterator(Inspect::new(self, f))
	}

	fn update<F>(self, f: F) -> Update<Self, F>
	where
		F: FnMut(&mut Self::Item) + Clone + ProcessSend,
		Self: Sized,
	{
		_assert_distributed_iterator(Update::new(self, f))
	}

	fn map<B, F>(self, f: F) -> Map<Self, F>
	where
		F: FnMut(Self::Item) -> B + Clone + ProcessSend,
		Self: Sized,
	{
		_assert_distributed_iterator(Map::new(self, f))
	}

	fn flat_map<B, F>(self, f: F) -> FlatMap<Self, F>
	where
		F: FnMut(Self::Item) -> B + Clone + ProcessSend,
		B: Stream,
		Self: Sized,
	{
		_assert_distributed_iterator(FlatMap::new(self, f))
	}

	fn filter<F, Fut>(self, f: F) -> Filter<Self, F>
	where
		F: FnMut(&Self::Item) -> Fut + Clone + ProcessSend,
		Fut: Future<Output = bool>,
		Self: Sized,
	{
		_assert_distributed_iterator(Filter::new(self, f))
	}

	fn chain<C>(self, chain: C) -> Chain<Self, C::Iter>
	where
		C: IntoDistributedIterator<Item = Self::Item>,
		Self: Sized,
	{
		_assert_distributed_iterator(Chain::new(self, chain.into_dist_iter()))
	}

	async fn reduce<P, B, R1F, R2F, R1, R2, R3>(
		mut self, pool: &P, reduce_a_factory: R1F, reduce_b_factory: R2F, reduce_c: R3,
	) -> B
	where
		P: ProcessPool,
		R1F: ReduceFactory<Reducer = R1> + Clone + ProcessSend,
		R2F: ReduceFactory<Reducer = R2>,
		R1: ReducerSend<Item = Self::Item> + ProcessSend,
		R2: ReducerProcessSend<Item = <R1 as Reducer>::Output>,
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
					impl<B> Sink<B::Item> for Connect<B>
					where
						B: ReducerAsync,
					{
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

	#[doc(hidden)]
	async fn single<P, Iterator, Reducer, A>(self, pool: &P, reducer_a: Reducer) -> A
	where
		P: ProcessPool,
		Iterator: DistributedIteratorMulti<Self::Item>,
		Reducer: DistributedReducer<Iterator, Self::Item, A>,
		Self: Sized,
	{
		struct Connect<A, B>(A, B);
		impl<A: DistributedIterator, B: DistributedIteratorMulti<A::Item>> DistributedIterator
			for Connect<A, B>
		{
			type Item = B::Item;
			type Task = ConnectConsumer<A::Task, B::Task>;
			fn size_hint(&self) -> (usize, Option<usize>) {
				self.0.size_hint()
			}
			fn next_task(&mut self) -> Option<Self::Task> {
				self.0
					.next_task()
					.map(|task| ConnectConsumer(task, self.1.task()))
			}
		}
		#[derive(Serialize, Deserialize)]
		#[serde(
			bound(serialize = "A: Serialize, B: Serialize"),
			bound(deserialize = "A: Deserialize<'de>, B: Deserialize<'de>")
		)]
		struct ConnectConsumer<A, B>(A, B);
		impl<A, B> Consumer for ConnectConsumer<A, B>
		where
			A: Consumer,
			B: ConsumerMulti<A::Item>,
		{
			type Item = B::Item;
			type Async = ConnectConsumerAsync<A::Async, B::Async>;
			fn into_async(self) -> Self::Async {
				ConnectConsumerAsync(self.0.into_async(), self.1.into_async())
			}
		}
		#[pin_project]
		struct ConnectConsumerAsync<A, B>(#[pin] A, #[pin] B);
		impl<A, B> ConsumerAsync for ConnectConsumerAsync<A, B>
		where
			A: ConsumerAsync,
			B: ConsumerMultiAsync<A::Item>,
		{
			type Item = B::Item;
			fn poll_run(
				self: Pin<&mut Self>, cx: &mut Context, sink: Pin<&mut impl Sink<Self::Item>>,
			) -> Poll<()> {
				#[pin_project]
				struct Proxy<'a, I, B>(#[pin] I, Pin<&'a mut B>);
				impl<'a, I, B, Item> Sink<Item> for Proxy<'a, I, B>
				where
					I: Sink<B::Item>,
					B: ConsumerMultiAsync<Item>,
				{
					fn poll_forward(
						self: Pin<&mut Self>, cx: &mut Context,
						stream: Pin<&mut impl Stream<Item = Item>>,
					) -> Poll<()> {
						let self_ = self.project();
						self_.1.as_mut().poll_run(cx, stream, self_.0)
					}
				}
				let self_ = self.project();
				let sink = Proxy(sink, self_.1);
				pin_mut!(sink);
				self_.0.poll_run(cx, sink)
			}
		}

		let (iterator, reducer_a, reducer_b, reducer_c) = reducer_a.reducers();
		Connect(self, iterator)
			.reduce(pool, reducer_a, reducer_b, reducer_c)
			.await
	}

	async fn multi<P, IteratorA, IteratorB, ReducerA, ReducerB, A, B>(
		self, pool: &P, reducer_a: ReducerA, reducer_b: ReducerB,
	) -> (A, B)
	where
		P: ProcessPool,

		IteratorA: DistributedIteratorMulti<Self::Item>,
		IteratorB: for<'a> DistributedIteratorMulti<&'a Self::Item>,

		ReducerA: DistributedReducer<IteratorA, Self::Item, A>,
		ReducerB: for<'a> DistributedReducer<IteratorB, &'a Self::Item, B>,

		Self::Item: 'static,
		Self: Sized,
	{
		struct Connect<A, B, C, RefAItem>(A, B, C, PhantomData<fn(RefAItem)>);
		impl<A, B, C, RefAItem> DistributedIterator for Connect<A, B, C, RefAItem>
		where
			A: DistributedIterator,
			B: DistributedIteratorMulti<A::Item>,
			C: DistributedIteratorMulti<RefAItem>,
			RefAItem: 'static,
		{
			type Item = Sum2<B::Item, C::Item>;
			type Task = ConnectConsumer<A::Task, B::Task, C::Task, RefAItem>;
			fn size_hint(&self) -> (usize, Option<usize>) {
				self.0.size_hint()
			}
			fn next_task(&mut self) -> Option<Self::Task> {
				self.0
					.next_task()
					.map(|task| ConnectConsumer(task, self.1.task(), self.2.task(), PhantomData))
			}
		}
		#[derive(Serialize, Deserialize)]
		#[serde(
			bound(serialize = "A: Serialize, B: Serialize, C: Serialize"),
			bound(deserialize = "A: Deserialize<'de>, B: Deserialize<'de>, C: Deserialize<'de>")
		)]
		struct ConnectConsumer<A, B, C, RefAItem>(A, B, C, PhantomData<fn(RefAItem)>);
		impl<A, B, C, RefAItem> Consumer for ConnectConsumer<A, B, C, RefAItem>
		where
			A: Consumer,
			B: ConsumerMulti<A::Item>,
			C: ConsumerMulti<RefAItem>,
		{
			type Item = Sum2<B::Item, C::Item>;
			type Async = ConnectConsumerAsync<A::Async, B::Async, C::Async, RefAItem, A::Item>;
			fn into_async(self) -> Self::Async {
				ConnectConsumerAsync(
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
		struct ConnectConsumerAsync<A, B, C, RefAItem, T>(
			#[pin] A,
			#[pin] B,
			#[pin] C,
			bool,
			Option<Option<T>>,
			PhantomData<fn(RefAItem)>,
		);
		impl<A, B, C, RefAItem> ConsumerAsync for ConnectConsumerAsync<A, B, C, RefAItem, A::Item>
		where
			A: ConsumerAsync,
			B: ConsumerMultiAsync<A::Item>,
			C: ConsumerMultiAsync<RefAItem>,
		{
			type Item = Sum2<B::Item, C::Item>;
			fn poll_run(
				self: Pin<&mut Self>, cx: &mut Context, sink: Pin<&mut impl Sink<Self::Item>>,
			) -> Poll<()> {
				#[pin_project]
				pub struct SinkFn<'a, S, A, B, T, RefAItem>(
					#[pin] S,
					Pin<&'a mut A>,
					Pin<&'a mut B>,
					&'a mut bool,
					&'a mut Option<Option<T>>,
					PhantomData<(T, RefAItem)>,
				);
				impl<'a, S, A, B, T, RefAItem> Sink<T> for SinkFn<'a, S, A, B, T, RefAItem>
				where
					S: Sink<Sum2<A::Item, B::Item>>,
					A: ConsumerMultiAsync<T>,
					B: ConsumerMultiAsync<RefAItem>,
				{
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

		let (iterator_a, reducer_a_a, reducer_a_b, reducer_a_c) = reducer_a.reducers();
		let (iterator_b, reducer_b_a, reducer_b_b, reducer_b_c) = reducer_b.reducers();
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
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::for_each(Identity, f),
		)
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
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::fold(Identity, identity, op),
		)
		.await
	}

	async fn count<P>(self, pool: &P) -> usize
	where
		P: ProcessPool,
		Self::Item: 'static,
		Self: Sized,
	{
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::count(Identity),
		)
		.await
	}

	async fn sum<P, S>(self, pool: &P) -> S
	where
		P: ProcessPool,
		S: iter::Sum<Self::Item> + iter::Sum<S> + ProcessSend,
		Self::Item: 'static,
		Self: Sized,
	{
		self.single(pool, DistributedIteratorMulti::<Self::Item>::sum(Identity))
			.await
	}

	async fn combine<P, F>(self, pool: &P, f: F) -> Option<Self::Item>
	where
		P: ProcessPool,
		F: FnMut(Self::Item, Self::Item) -> Self::Item + Clone + ProcessSend,
		Self::Item: ProcessSend,
		Self: Sized,
	{
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::combine(Identity, f),
		)
		.await
	}

	async fn max<P>(self, pool: &P) -> Option<Self::Item>
	where
		P: ProcessPool,
		Self::Item: Ord + ProcessSend,
		Self: Sized,
	{
		self.single(pool, DistributedIteratorMulti::<Self::Item>::max(Identity))
			.await
	}

	async fn max_by<P, F>(self, pool: &P, f: F) -> Option<Self::Item>
	where
		P: ProcessPool,
		F: FnMut(&Self::Item, &Self::Item) -> Ordering + Clone + ProcessSend,
		Self::Item: ProcessSend,
		Self: Sized,
	{
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::max_by(Identity, f),
		)
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
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::max_by_key(Identity, f),
		)
		.await
	}

	async fn min<P>(self, pool: &P) -> Option<Self::Item>
	where
		P: ProcessPool,
		Self::Item: Ord + ProcessSend,
		Self: Sized,
	{
		self.single(pool, DistributedIteratorMulti::<Self::Item>::min(Identity))
			.await
	}

	async fn min_by<P, F>(self, pool: &P, f: F) -> Option<Self::Item>
	where
		P: ProcessPool,
		F: FnMut(&Self::Item, &Self::Item) -> Ordering + Clone + ProcessSend,
		Self::Item: ProcessSend,
		Self: Sized,
	{
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::min_by(Identity, f),
		)
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
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::min_by_key(Identity, f),
		)
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
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::most_frequent(
				Identity,
				n,
				probability,
				tolerance,
			),
		)
		.await
	}

	async fn most_distinct<P, A, B>(
		self, pool: &P, n: usize, probability: f64, tolerance: f64, error_rate: f64,
	) -> ::streaming_algorithms::Top<A, streaming_algorithms::HyperLogLogMagnitude<B>>
	where
		P: ProcessPool,
		Self: DistributedIterator<Item = (A, B)> + Sized,
		A: Hash + Eq + Clone + ProcessSend,
		B: Hash + 'static,
	{
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::most_distinct(
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
		// Self::Task: ProcessSend,
		Self::Item: ProcessSend,
		Self: Sized,
	{
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::sample_unstable(Identity, samples),
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
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::all(Identity, f),
		)
		.await
	}

	async fn any<P, F>(self, pool: &P, f: F) -> bool
	where
		P: ProcessPool,
		F: FnMut(Self::Item) -> bool + Clone + ProcessSend,
		Self::Item: 'static,
		Self: Sized,
	{
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::any(Identity, f),
		)
		.await
	}

	async fn collect<P, B>(self, pool: &P) -> B
	where
		P: ProcessPool,
		B: FromDistributedIterator<Self::Item>,
		B::ReduceA: ProcessSend,
		// <B::ReduceA as Reducer>::Output: Serialize + DeserializeOwned + Send,
		Self: Sized,
	{
		// B::from_dist_iter(self, pool)
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::collect(Identity),
		)
		.await
	}
}

#[must_use]
pub trait DistributedIteratorMulti<Source> {
	type Item;
	type Task: ConsumerMulti<Source, Item = Self::Item> + ProcessSend;

	fn task(&self) -> Self::Task;

	fn for_each<F>(self, f: F) -> ForEach<Self, F>
	where
		F: FnMut(Self::Item) + Clone + ProcessSend,
		Self::Item: 'static,
		Self: Sized,
	{
		_assert_distributed_reducer(ForEach::new(self, f))
	}

	fn inspect<F>(self, f: F) -> Inspect<Self, F>
	where
		F: FnMut(&Self::Item) + Clone + ProcessSend,
		Self: Sized,
	{
		_assert_distributed_iterator_multi(Inspect::new(self, f))
	}

	fn update<F>(self, f: F) -> Update<Self, F>
	where
		F: FnMut(&mut Self::Item) + Clone + ProcessSend,
		Self: Sized,
	{
		_assert_distributed_iterator_multi(Update::new(self, f))
	}

	fn map<B, F>(self, f: F) -> Map<Self, F>
	where
		F: FnMut(Self::Item) -> B + Clone + ProcessSend,
		Self: Sized,
	{
		_assert_distributed_iterator_multi(Map::new(self, f))
	}

	fn flat_map<B, F>(self, f: F) -> FlatMap<Self, F>
	where
		F: FnMut(Self::Item) -> B + Clone + ProcessSend,
		B: Stream,
		Self: Sized,
	{
		_assert_distributed_iterator_multi(FlatMap::new(self, f))
	}

	fn filter<F, Fut>(self, f: F) -> Filter<Self, F>
	where
		F: FnMut(&Self::Item) -> Fut + Clone + ProcessSend,
		Fut: Future<Output = bool>,
		Self: Sized,
	{
		_assert_distributed_iterator_multi(Filter::new(self, f))
	}

	// #[must_use]
	// fn chain<C>(self, chain: C) -> Chain<Self, C::Iter>
	// where
	// 	C: IntoDistributedIterator<Item = Self::Item>,
	// 	Self: Sized,
	// {
	// 	Chain::new(self, chain.into_dist_iter())
	// }

	fn fold<ID, F, B>(self, identity: ID, op: F) -> Fold<Self, ID, F, B>
	where
		ID: FnMut() -> B + Clone + ProcessSend,
		F: FnMut(B, Either<Self::Item, B>) -> B + Clone + ProcessSend,
		B: ProcessSend,
		Self::Item: 'static,
		Self: Sized,
	{
		_assert_distributed_reducer(Fold::new(self, identity, op))
	}

	fn count(self) -> Count<Self>
	where
		Self::Item: 'static,
		Self: Sized,
	{
		_assert_distributed_reducer(Count::new(self))
	}

	fn sum<B>(self) -> Sum<Self, B>
	where
		B: iter::Sum<Self::Item> + iter::Sum<B> + ProcessSend,
		Self::Item: 'static,
		Self: Sized,
	{
		_assert_distributed_reducer(Sum::new(self))
	}

	fn combine<F>(self, f: F) -> Combine<Self, F>
	where
		F: FnMut(Self::Item, Self::Item) -> Self::Item + Clone + ProcessSend,
		Self::Item: ProcessSend,
		Self: Sized,
	{
		_assert_distributed_reducer(Combine::new(self, f))
	}

	fn max(self) -> Max<Self>
	where
		Self::Item: Ord + ProcessSend,
		Self: Sized,
	{
		_assert_distributed_reducer(Max::new(self))
	}

	fn max_by<F>(self, f: F) -> MaxBy<Self, F>
	where
		F: FnMut(&Self::Item, &Self::Item) -> Ordering + Clone + ProcessSend,
		Self::Item: ProcessSend,
		Self: Sized,
	{
		_assert_distributed_reducer(MaxBy::new(self, f))
	}

	fn max_by_key<F, B>(self, f: F) -> MaxByKey<Self, F>
	where
		F: FnMut(&Self::Item) -> B + Clone + ProcessSend,
		B: Ord + 'static,
		Self::Item: ProcessSend,
		Self: Sized,
	{
		_assert_distributed_reducer(MaxByKey::new(self, f))
	}

	fn min(self) -> Min<Self>
	where
		Self::Item: Ord + ProcessSend,
		Self: Sized,
	{
		_assert_distributed_reducer(Min::new(self))
	}

	fn min_by<F>(self, f: F) -> MinBy<Self, F>
	where
		F: FnMut(&Self::Item, &Self::Item) -> Ordering + Clone + ProcessSend,
		Self::Item: ProcessSend,
		Self: Sized,
	{
		_assert_distributed_reducer(MinBy::new(self, f))
	}

	fn min_by_key<F, B>(self, f: F) -> MinByKey<Self, F>
	where
		F: FnMut(&Self::Item) -> B + Clone + ProcessSend,
		B: Ord + 'static,
		Self::Item: ProcessSend,
		Self: Sized,
	{
		_assert_distributed_reducer(MinByKey::new(self, f))
	}

	fn most_frequent(self, n: usize, probability: f64, tolerance: f64) -> MostFrequent<Self>
	where
		Self::Item: Hash + Eq + Clone + ProcessSend,
		Self: Sized,
	{
		_assert_distributed_reducer(MostFrequent::new(self, n, probability, tolerance))
	}

	fn most_distinct<A, B>(
		self, n: usize, probability: f64, tolerance: f64, error_rate: f64,
	) -> MostDistinct<Self>
	where
		Self: DistributedIteratorMulti<Source, Item = (A, B)> + Sized,
		A: Hash + Eq + Clone + ProcessSend,
		B: Hash + 'static,
	{
		_assert_distributed_reducer(MostDistinct::new(
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
		_assert_distributed_reducer(SampleUnstable::new(self, samples))
	}

	fn all<F>(self, f: F) -> All<Self, F>
	where
		F: FnMut(Self::Item) -> bool + Clone + ProcessSend,
		Self::Item: 'static,
		Self: Sized,
	{
		_assert_distributed_reducer(All::new(self, f))
	}

	fn any<F>(self, f: F) -> Any<Self, F>
	where
		F: FnMut(Self::Item) -> bool + Clone + ProcessSend,
		Self::Item: 'static,
		Self: Sized,
	{
		_assert_distributed_reducer(Any::new(self, f))
	}

	fn collect<B>(self) -> Collect<Self, B>
	where
		B: FromDistributedIterator<Self::Item>,
		Self: Sized,
	{
		_assert_distributed_reducer::<B, _, _, _>(Collect::new(self))
	}

	fn cloned<'a, T>(self) -> Cloned<Self, T, Source>
	where
		T: Clone + 'a,
		Source: 'a,
		Self: DistributedIteratorMulti<&'a Source, Item = &'a T> + Sized,
	{
		_assert_distributed_iterator_multi::<T, _, _>(Cloned::new(self))
	}
}

pub trait Consumer {
	type Item;
	type Async: ConsumerAsync<Item = Self::Item>;

	fn into_async(self) -> Self::Async;
}
pub trait ConsumerMulti<Source> {
	type Item;
	type Async: ConsumerMultiAsync<Source, Item = Self::Item>;

	fn into_async(self) -> Self::Async;
}
pub trait ConsumerAsync {
	type Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()>;
}
pub trait ConsumerMultiAsync<Source> {
	type Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
		sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()>;
}

pub trait Reducer {
	type Item;
	type Output;
	type Async: ReducerAsync<Item = Self::Item, Output = Self::Output>;

	fn into_async(self) -> Self::Async;
}
pub trait ReducerAsync {
	type Item;
	type Output;

	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()>;
	fn poll_output(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output>;
}
pub trait ReducerSend: Reducer<Output = <Self as ReducerSend>::Output> + Send + 'static {
	type Output: Send + 'static;
}
pub trait ReducerProcessSend:
	ReducerSend<Output = <Self as ReducerProcessSend>::Output> + ProcessSend
{
	type Output: ProcessSend;
}
// impl<T> ReducerSend for T where T: Reducer + Send, <T as Reducer>::Output: Send {
// 	type Output = <T as Reducer>::Output;
// }

pub trait ReduceFactory {
	type Reducer: Reducer;

	fn make(&self) -> Self::Reducer;
}

pub trait DistributedReducer<I: DistributedIteratorMulti<Source>, Source, B> {
	type ReduceAFactory: ReduceFactory<Reducer = Self::ReduceA> + Clone + ProcessSend;
	type ReduceBFactory: ReduceFactory<Reducer = Self::ReduceB>;
	type ReduceA: ReducerSend<Item = <I as DistributedIteratorMulti<Source>>::Item> + ProcessSend;
	type ReduceB: ReducerProcessSend<Item = <Self::ReduceA as Reducer>::Output>;
	type ReduceC: Reducer<Item = <Self::ReduceB as Reducer>::Output, Output = B>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceBFactory, Self::ReduceC);
}
