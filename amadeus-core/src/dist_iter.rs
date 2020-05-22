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
use core::{
	future::Future, pin::Pin, task::{Context, Poll}
};
use either::Either;
use futures::{pin_mut, ready, stream::StreamExt, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use serde_closure::*;
use std::{cmp::Ordering, hash::Hash, iter, marker::PhantomData, ops::FnMut, vec};

use crate::{
	into_dist_iter::IntoDistributedIterator, pool::{ProcessPool, ProcessSend}, sink::{Sink, SinkBuffer, SinkMap}, util::type_coerce
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

	async fn reduce<P, B, R1F, R1, R2>(
		mut self, pool: &P, reduce1factory: R1F, mut reduce2: R2,
	) -> B
	where
		P: ProcessPool,
		R1F: ReduceFactory<Reducer = R1>,
		R1: ReducerA<Item = Self::Item>,
		R2: Reducer<Item = <R1 as ReducerA>::Output, Output = B>,
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
		let mut handles = tasks
			.into_iter()
			.map(|tasks| {
				let reduce1 = reduce1factory.make();
				pool.spawn(FnOnce!(move || -> <R1 as ReducerA>::Output {
					futures::executor::block_on(async {
						for task in tasks {
							let task = task.into_async();
							pin_mut!(task);
							if !futures::future::poll_fn(|cx| {
								task.as_mut().poll_run(
									cx,
									&mut SinkBuffer::new(
										&mut None,
										|_cx: &mut Context, item: &mut Option<_>| {
											Poll::Ready(reduce1.push(item.take().unwrap()))
										},
									),
								)
							})
							.await
							{
								break;
							}
						}
						reduce1.ret()
					})
				}))
			})
			.collect::<futures::stream::FuturesUnordered<_>>();
		let mut more = true;
		let mut panicked = None;
		while let Some(res) = handles.next().await {
			match res {
				Ok(res) => {
					more = more && reduce2.push(res);
				}
				Err(e) => panicked = Some(e),
			}
		}
		if let Some(err) = panicked {
			panic!("Amadeus: task '<unnamed>' panicked at '{}'", err)
		}
		reduce2.ret()
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
		#[pin_project]
		#[derive(Serialize, Deserialize)]
		#[serde(
			bound(serialize = "A: Serialize, B: Serialize"),
			bound(deserialize = "A: Deserialize<'de>, B: Deserialize<'de>")
		)]
		struct ConnectConsumer<A, B>(#[pin] A, #[pin] B);
		impl<A: Consumer, B> Consumer for ConnectConsumer<A, B>
		where
			B: ConsumerMulti<A::Item>,
		{
			type Item = B::Item;
			type Async = ConnectConsumer<A::Async, B::Async>;
			fn into_async(self) -> Self::Async {
				ConnectConsumer(self.0.into_async(), self.1.into_async())
			}
		}
		impl<A: ConsumerAsync, B> ConsumerAsync for ConnectConsumer<A, B>
		where
			B: ConsumerMultiAsync<A::Item>,
		{
			type Item = B::Item;
			fn poll_run(
				self: Pin<&mut Self>, cx: &mut Context, sink: &mut impl Sink<Self::Item>,
			) -> Poll<bool> {
				let self_ = self.project();
				let mut a = self_.1;
				self_.0.poll_run(
					cx,
					&mut SinkBuffer::new(&mut None, |cx: &mut Context, item: &mut Option<_>| {
						a.as_mut().poll_run(cx, item.take(), sink)
					}),
				)
			}
		}

		let (iterator, reducer_a, reducer_b) = reducer_a.reducers();
		Connect(self, iterator)
			.reduce(pool, reducer_a, reducer_b)
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
		#[pin_project]
		#[derive(Serialize, Deserialize)]
		#[serde(
			bound(serialize = "A: Serialize, B: Serialize, C: Serialize"),
			bound(deserialize = "A: Deserialize<'de>, B: Deserialize<'de>, C: Deserialize<'de>")
		)]
		struct ConnectConsumer<A, B, C, RefAItem>(
			#[pin] A,
			#[pin] B,
			#[pin] C,
			PhantomData<fn(RefAItem)>,
		);
		impl<A: Consumer, B, C, RefAItem> Consumer for ConnectConsumer<A, B, C, RefAItem>
		where
			B: ConsumerMulti<A::Item>,
			C: ConsumerMulti<RefAItem>,
		{
			type Item = Sum2<B::Item, C::Item>;
			type Async = ConnectConsumer<A::Async, B::Async, C::Async, RefAItem>;
			fn into_async(self) -> Self::Async {
				ConnectConsumer(
					self.0.into_async(),
					self.1.into_async(),
					self.2.into_async(),
					PhantomData,
				)
			}
		}
		impl<A: ConsumerAsync, B, C, RefAItem> ConsumerAsync for ConnectConsumer<A, B, C, RefAItem>
		where
			B: ConsumerMultiAsync<A::Item>,
			C: ConsumerMultiAsync<RefAItem>,
		{
			type Item = Sum2<B::Item, C::Item>;
			fn poll_run(
				self: Pin<&mut Self>, cx: &mut Context, mut sink: &mut impl Sink<Self::Item>,
			) -> Poll<bool> {
				let self_ = self.project();
				let mut a = self_.1;
				let mut b = self_.2;
				self_.0.poll_run(
					cx,
					&mut SinkBuffer::new(&mut None, |cx: &mut Context, item: &mut Option<_>| {
						let a_ = ready!(b.as_mut().poll_run(
							cx,
							type_coerce(item.as_ref()),
							&mut SinkMap::new(&mut sink, |item| Sum2::B(type_coerce(item)))
						));
						let b_ = ready!(a.as_mut().poll_run(
							cx,
							item.take(),
							&mut SinkMap::new(&mut sink, Sum2::A)
						));
						Poll::Ready(a_ | b_)
					}),
				)
			}
		}

		let (iterator_a, reducer_a_a, reducer_a_b) = reducer_a.reducers();
		let (iterator_b, reducer_b_a, reducer_b_b) = reducer_b.reducers();
		Connect(self, iterator_a, iterator_b, PhantomData)
			.reduce(
				pool,
				ReduceA2Factory(reducer_a_a, reducer_b_a),
				ReduceB2(reducer_a_b, reducer_b_b),
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
		self: Pin<&mut Self>, cx: &mut Context, sink: &mut impl Sink<Self::Item>,
	) -> Poll<bool>;
}
pub trait ConsumerMultiAsync<Source> {
	type Item;
	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, source: Option<Source>,
		sink: &mut impl Sink<Self::Item>,
	) -> Poll<bool>;
}

pub trait Reducer {
	type Item;
	type Output;
	fn push(&mut self, item: Self::Item) -> bool;
	fn ret(self) -> Self::Output;
}
pub trait ReducerA: Reducer<Output = <Self as ReducerA>::Output> + ProcessSend {
	type Output: ProcessSend;
}
// impl<T> ReducerA for T where T: Reducer, T::Output: ProcessSend {
// 	type Output2 = T::Output;
// }

pub trait ReduceFactory {
	type Reducer: Reducer;
	fn make(&self) -> Self::Reducer;
}

pub trait DistributedReducer<I: DistributedIteratorMulti<Source>, Source, B> {
	type ReduceAFactory: ReduceFactory<Reducer = Self::ReduceA>;
	type ReduceA: ReducerA<Item = <I as DistributedIteratorMulti<Source>>::Item> + ProcessSend;
	type ReduceB: Reducer<Item = <Self::ReduceA as Reducer>::Output, Output = B>;
	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB);
}
