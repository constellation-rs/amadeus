mod all;
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
mod tuple;
mod update;
pub use self::{
	all::*, chain::*, cloned::*, collect::*, combine::*, count::*, filter::*, flat_map::*, fold::*, for_each::*, identity::*, inspect::*, map::*, max::*, sample::*, sum::*, tuple::*, update::*
};

use crate::into_dist_iter::IntoDistributedIterator;
use ::sum::*;
use either::Either;
use serde::{
	de::{Deserialize, DeserializeOwned}, ser::Serialize
};
use std::{cmp::Ordering, hash::Hash, iter, marker, vec};

type Pool = crate::process_pool::ProcessPool;
// type Pool = ::no_pool::NoPool;

pub trait DistributedIterator {
	type Item;
	type Task: Consumer<Item = Self::Item>;
	fn size_hint(&self) -> (usize, Option<usize>);
	fn next_task(&mut self) -> Option<Self::Task>;

	fn inspect<F>(self, f: F) -> Inspect<Self, F>
	where
		F: FnMut(&Self::Item),
		Self: Sized,
	{
		Inspect::new(self, f)
	}

	fn update<F>(self, f: F) -> Update<Self, F>
	where
		F: FnMut(&mut Self::Item),
		Self: Sized,
	{
		Update::new(self, f)
	}

	fn map<B, F>(self, f: F) -> Map<Self, F>
	where
		F: FnMut(Self::Item) -> B,
		Self: Sized,
	{
		Map::new(self, f)
	}

	fn flat_map<B, F>(self, f: F) -> FlatMap<Self, F>
	where
		F: FnMut(Self::Item) -> B,
		Self: Sized,
	{
		FlatMap::new(self, f)
	}

	fn filter<F>(self, f: F) -> Filter<Self, F>
	where
		F: FnMut(&Self::Item) -> bool,
		Self: Sized,
	{
		Filter::new(self, f)
	}

	fn chain<C>(self, chain: C) -> Chain<Self, C::Iter>
	where
		C: IntoDistributedIterator<Item = Self::Item>,
		Self: Sized,
	{
		Chain::new(self, chain.into_dist_iter())
	}

	fn reduce<B, R1F, R1, R2>(mut self, pool: &Pool, reduce1factory: R1F, mut reduce2: R2) -> B
	where
		R1F: ReduceFactory<Reducer = R1>,
		R1: Reducer<Item = Self::Item> + Serialize + DeserializeOwned + 'static,
		R2: Reducer<Item = R1::Output, Output = B>,
		R1::Output: Serialize + DeserializeOwned + Send + 'static,
		Self::Task: Serialize + DeserializeOwned + 'static,
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
				tasks.iter().map(|x| x.len()).collect::<Vec<_>>()
			);
		}
		let handles = tasks
			.into_iter()
			.map(|tasks| {
				let reduce1 = reduce1factory.make();
				pool.spawn(FnOnce!([tasks,reduce1] move || -> R1::Output {
					let mut reduce1: R1 = reduce1;
					let tasks: Vec<Self::Task> = tasks;
					for task in tasks {
						task.run(&mut |item| reduce1.push(item));
					};
					reduce1.ret()
				}))
			})
			.collect::<Vec<_>>();
		let mut panicked = false;
		for handle in handles {
			if let Ok(res) = handle.join() {
				reduce2.push(res);
			} else {
				panicked = true;
			}
		}
		if panicked {
			panic!("a process panicked");
		}
		reduce2.ret()
	}

	#[doc(hidden)]
	fn single<Iterator, Reducer, A>(self, pool: &Pool, reducer_a: Reducer) -> A
	where
		Iterator: DistributedIteratorMulti<Self::Item>,
		Iterator::Task: Serialize + DeserializeOwned + 'static,

		Reducer: DistributedReducer<Iterator, Self::Item, A>,
		<Reducer as DistributedReducer<Iterator, Self::Item, A>>::ReduceA:
			Serialize + DeserializeOwned + 'static,
		<<Reducer as DistributedReducer<Iterator, Self::Item, A>>::ReduceA as self::Reducer>::Output:
			Serialize + DeserializeOwned + Send + 'static,

		Self::Task: Serialize + DeserializeOwned + 'static,
		Self: Sized,
	{
		struct Connect<A, B>(A, B);
		impl<A: DistributedIterator, B: DistributedIteratorMulti<A::Item>> DistributedIterator
			for Connect<A, B> where
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
		impl<A: Consumer, B> Consumer for ConnectConsumer<A, B>
		where
			B: ConsumerMulti<A::Item>,
		{
			type Item = B::Item;
			fn run(self, i: &mut impl FnMut(Self::Item)) {
				let a = self.1;
				self.0.run(&mut |item| {
					a.run(item, &mut |item| i(item));
				})
			}
		}

		let (iterator, reducer_a, reducer_b) = reducer_a.reducers();
		Connect(self, iterator).reduce(pool, reducer_a, reducer_b)
	}

	fn multi<IteratorA, IteratorB, ReducerA, ReducerB, A, B>(
		self, pool: &Pool, reducer_a: ReducerA, reducer_b: ReducerB,
	) -> (A, B)
	// TODO: remove Self::Item: 'static
	where
		IteratorA: DistributedIteratorMulti<Self::Item>,
		IteratorA::Task: Serialize + DeserializeOwned + 'static,
		IteratorB: for<'a> DistributedIteratorMulti<&'a Self::Item> + 'static,
		for<'a> <IteratorB as DistributedIteratorMulti<&'static Self::Item>>::Task:
			Serialize + DeserializeOwned + 'static,

		ReducerA: DistributedReducer<IteratorA, Self::Item, A>,
		<ReducerA as DistributedReducer<IteratorA, Self::Item, A>>::ReduceA:
			Serialize + DeserializeOwned + 'static,
		<<ReducerA as DistributedReducer<IteratorA, Self::Item, A>>::ReduceA as Reducer>::Output:
			Serialize + DeserializeOwned + Send + 'static,
		ReducerB: for<'a> DistributedReducer<IteratorB, &'a Self::Item, B> + 'static,
		for<'a> <ReducerB as DistributedReducer<IteratorB, &'static Self::Item, B>>::ReduceA:
			Serialize + DeserializeOwned + 'static,
		for<'a> <<ReducerB as DistributedReducer<IteratorB, &'static Self::Item, B>>::ReduceA as Reducer>::Output:
			Serialize + DeserializeOwned + Send + 'static,

		Self::Task: Serialize + DeserializeOwned + 'static,
		Self::Item: 'static,
		Self: Sized,
	{
		struct Connect<A, B, C, CTask, CItem>(A, B, C, marker::PhantomData<fn(CTask, CItem)>);
		impl<
				A: DistributedIterator,
				B: DistributedIteratorMulti<A::Item>,
				C: for<'a> DistributedIteratorMulti<&'a A::Item>,
				CTask,
				CItem,
			> DistributedIterator for Connect<A, B, C, CTask, CItem> where
		{
			type Item = Sum2<B::Item, CItem>;
			type Task = ConnectConsumer<A::Task, B::Task, CTask, CItem>;
			fn size_hint(&self) -> (usize, Option<usize>) {
				self.0.size_hint()
			}
			fn next_task(&mut self) -> Option<Self::Task> {
				self.0.next_task().map(|task| {
					ConnectConsumer(
						task,
						self.1.task(),
						unsafe { type_transmute(self.2.task()) },
						marker::PhantomData,
					)
				})
			}
		}
		#[derive(Serialize, Deserialize)]
		#[serde(
			bound(serialize = "A: Serialize, B: Serialize, C: Serialize"),
			bound(deserialize = "A: Deserialize<'de>, B: Deserialize<'de>, C: Deserialize<'de>")
		)]
		struct ConnectConsumer<A, B, C, CItem>(A, B, C, marker::PhantomData<fn(CItem)>);
		impl<A: Consumer, B, C, CItem> Consumer for ConnectConsumer<A, B, C, CItem>
		where
			B: ConsumerMulti<A::Item>,
		{
			type Item = Sum2<B::Item, CItem>;
			fn run(self, i: &mut impl FnMut(Self::Item)) {
				let a = self.1;
				let b = self.2;
				self.0.run(&mut |item| {
					trait ConsumerReducerHack<Source> {
						type Item;
						fn run(&self, source: Source, i: &mut impl FnMut(Self::Item));
					}
					impl<T, Source> ConsumerReducerHack<Source> for T {
						default type Item = !;
						default fn run(&self, _source: Source, _i: &mut impl FnMut(Self::Item)) {
							unreachable!()
						}
					}
					impl<T, Source> ConsumerReducerHack<Source> for T
					where
						T: ConsumerMulti<Source>,
					{
						type Item = <Self as ConsumerMulti<Source>>::Item;
						fn run(&self, source: Source, i: &mut impl FnMut(Self::Item)) {
							ConsumerMulti::<Source>::run(self, source, i)
						}
					}
					ConsumerReducerHack::<&A::Item>::run(&b, &item, &mut |item| {
						i(Sum2::B(unsafe { type_transmute(item) }))
					});
					a.run(item, &mut |item| i(Sum2::A(item)));
				})
			}
		}

		let (iterator_a, reducer_a_a, reducer_a_b) = reducer_a.reducers();
		let (iterator_b, reducer_b_a, reducer_b_b) = reducer_b.reducers();
		Connect::<_, _, _, <IteratorB as DistributedIteratorMulti<&Self::Item>>::Task, _>(
			self,
			iterator_a,
			iterator_b,
			marker::PhantomData,
		)
		.reduce(
			pool,
			ReduceA2Factory(reducer_a_a, reducer_b_a),
			ReduceB2(reducer_a_b, reducer_b_b),
		)
	}

	fn for_each<F>(self, pool: &Pool, f: F)
	where
		F: FnMut(Self::Item) + Clone + Serialize + DeserializeOwned + 'static,
		Self::Task: Serialize + DeserializeOwned + 'static,
		Self::Item: 'static,
		Self: Sized,
	{
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::for_each(Identity, f),
		)
	}

	fn fold<ID, F, B>(self, pool: &Pool, identity: ID, op: F) -> B
	where
		ID: FnMut() -> B + Clone + Serialize + DeserializeOwned + 'static,
		B: Serialize + DeserializeOwned + Send + 'static,
		F: FnMut(B, Either<Self::Item, B>) -> B + Clone + Serialize + DeserializeOwned + 'static,
		Self::Task: Serialize + DeserializeOwned + 'static,
		Self::Item: 'static,
		Self: Sized,
	{
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::fold(Identity, identity, op),
		)
	}

	fn count(self, pool: &Pool) -> usize
	where
		Self::Task: Serialize + DeserializeOwned + 'static,
		Self::Item: 'static,
		Self: Sized,
	{
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::count(Identity),
		)
	}

	fn sum<S>(self, pool: &Pool) -> S
	where
		S: iter::Sum<Self::Item> + iter::Sum<S> + Serialize + DeserializeOwned + Send + 'static,
		Self::Task: Serialize + DeserializeOwned + 'static,
		Self::Item: 'static,
		Self: Sized,
	{
		self.single(pool, DistributedIteratorMulti::<Self::Item>::sum(Identity))
	}

	fn combine<F>(self, pool: &Pool, f: F) -> Option<Self::Item>
	where
		F: FnMut(Self::Item, Self::Item) -> Self::Item
			+ Clone
			+ Serialize
			+ DeserializeOwned
			+ 'static,
		Self::Item: Serialize + DeserializeOwned + Send + 'static,
		Self::Task: Serialize + DeserializeOwned + 'static,
		Self: Sized,
	{
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::combine(Identity, f),
		)
	}

	fn max(self, pool: &Pool) -> Option<Self::Item>
	where
		Self::Item: Ord + Serialize + DeserializeOwned + Send + 'static,
		Self::Task: Serialize + DeserializeOwned + 'static,
		Self: Sized,
	{
		self.single(pool, DistributedIteratorMulti::<Self::Item>::max(Identity))
	}

	fn max_by<F>(self, pool: &Pool, f: F) -> Option<Self::Item>
	where
		F: FnMut(&Self::Item, &Self::Item) -> Ordering
			+ Serialize
			+ DeserializeOwned
			+ Clone
			+ 'static,
		Self::Item: Serialize + DeserializeOwned + Send + 'static,
		Self::Task: Serialize + DeserializeOwned + 'static,
		Self: Sized,
	{
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::max_by(Identity, f),
		)
	}

	fn max_by_key<F, B>(self, pool: &Pool, f: F) -> Option<Self::Item>
	where
		F: FnMut(&Self::Item) -> B + Serialize + DeserializeOwned + Clone + 'static,
		B: Ord + 'static,
		Self::Item: Serialize + DeserializeOwned + Send + 'static,
		Self::Task: Serialize + DeserializeOwned + 'static,
		Self: Sized,
	{
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::max_by_key(Identity, f),
		)
	}

	fn min(self, pool: &Pool) -> Option<Self::Item>
	where
		Self::Item: Ord + Serialize + DeserializeOwned + Send + 'static,
		Self::Task: Serialize + DeserializeOwned + 'static,
		Self: Sized,
	{
		self.single(pool, DistributedIteratorMulti::<Self::Item>::min(Identity))
	}

	fn min_by<F>(self, pool: &Pool, f: F) -> Option<Self::Item>
	where
		F: FnMut(&Self::Item, &Self::Item) -> Ordering
			+ Serialize
			+ DeserializeOwned
			+ Clone
			+ 'static,
		Self::Item: Serialize + DeserializeOwned + Send + 'static,
		Self::Task: Serialize + DeserializeOwned + 'static,
		Self: Sized,
	{
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::min_by(Identity, f),
		)
	}

	fn min_by_key<F, B>(self, pool: &Pool, f: F) -> Option<Self::Item>
	where
		F: FnMut(&Self::Item) -> B + Serialize + DeserializeOwned + Clone + 'static,
		B: Ord + 'static,
		Self::Item: Serialize + DeserializeOwned + Send + 'static,
		Self::Task: Serialize + DeserializeOwned + 'static,
		Self: Sized,
	{
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::min_by_key(Identity, f),
		)
	}

	fn most_frequent(
		self, pool: &Pool, n: usize, probability: f64, tolerance: f64,
	) -> ::streaming_algorithms::Top<Self::Item, usize>
	where
		Self::Task: Serialize + DeserializeOwned + 'static,
		Self::Item: Hash + Eq + Clone + Serialize + DeserializeOwned + Send + 'static,
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
	}

	fn most_distinct<A, B>(
		self, pool: &Pool, n: usize, probability: f64, tolerance: f64, error_rate: f64,
	) -> ::streaming_algorithms::Top<A, streaming_algorithms::HyperLogLogMagnitude<B>>
	where
		Self::Task: Serialize + DeserializeOwned + 'static,
		Self: DistributedIterator<Item = (A, B)>,
		A: Hash + Eq + Clone + Serialize + DeserializeOwned + Send + 'static,
		B: Hash + 'static,
		Self: Sized,
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
	}

	fn sample_unstable(
		self, pool: &Pool, samples: usize,
	) -> ::streaming_algorithms::SampleUnstable<Self::Item>
	where
		Self::Task: Serialize + DeserializeOwned + 'static,
		Self::Item: Serialize + DeserializeOwned + Send + 'static,
		Self: Sized,
	{
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::sample_unstable(Identity, samples),
		)
	}

	fn all<F>(self, pool: &Pool, f: F) -> bool
	where
		F: FnMut(Self::Item) -> bool + Clone + Serialize + DeserializeOwned + 'static,
		Self::Task: Serialize + DeserializeOwned + 'static,
		Self::Item: 'static,
		Self: Sized,
	{
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::all(Identity, f),
		)
	}

	fn collect<B>(self, pool: &Pool) -> B
	where
		B: FromDistributedIterator<Self::Item>,
		Self::Task: Serialize + DeserializeOwned + 'static,
		B::ReduceA: Serialize + DeserializeOwned + 'static,
		<B::ReduceA as Reducer>::Output: Serialize + DeserializeOwned + Send,
		Self: Sized,
	{
		// B::from_dist_iter(self, pool)
		self.single(
			pool,
			DistributedIteratorMulti::<Self::Item>::collect(Identity),
		)
	}
}

pub trait DistributedIteratorMulti<Source> {
	type Item;
	type Task: ConsumerMulti<Source, Item = Self::Item>;

	fn task(&self) -> Self::Task;

	fn for_each<F>(self, f: F) -> ForEach<Self, F>
	where
		F: FnMut(Self::Item),
		Self: Sized,
	{
		ForEach::new(self, f)
	}

	fn inspect<F>(self, f: F) -> Inspect<Self, F>
	where
		F: FnMut(&Self::Item),
		Self: Sized,
	{
		Inspect::new(self, f)
	}

	fn update<F>(self, f: F) -> Update<Self, F>
	where
		F: FnMut(&mut Self::Item),
		Self: Sized,
	{
		Update::new(self, f)
	}

	fn map<B, F>(self, f: F) -> Map<Self, F>
	where
		F: FnMut(Self::Item) -> B,
		Self: Sized,
	{
		Map::new(self, f)
	}

	fn flat_map<B, F>(self, f: F) -> FlatMap<Self, F>
	where
		F: FnMut(Self::Item) -> B,
		Self: Sized,
	{
		FlatMap::new(self, f)
	}

	fn filter<F>(self, f: F) -> Filter<Self, F>
	where
		F: FnMut(&Self::Item) -> bool,
		Self: Sized,
	{
		Filter::new(self, f)
	}

	fn chain<C>(self, chain: C) -> Chain<Self, C::Iter>
	where
		C: IntoDistributedIterator<Item = Self::Item>,
		Self: Sized,
	{
		Chain::new(self, chain.into_dist_iter())
	}

	#[must_use]
	fn fold<ID, F, B>(self, identity: ID, op: F) -> Fold<Self, ID, F, B>
	where
		ID: FnMut() -> B + Clone,
		F: FnMut(B, Either<Self::Item, B>) -> B + Clone,
		Self: Sized,
	{
		Fold::new(self, identity, op)
	}

	#[must_use]
	fn count(self) -> Count<Self>
	where
		Self: Sized,
	{
		Count::new(self)
	}

	#[must_use]
	fn sum<B>(self) -> Sum<Self, B>
	where
		Self: Sized,
	{
		Sum::new(self)
	}

	#[must_use]
	fn combine<F>(self, f: F) -> Combine<Self, F>
	where
		F: FnMut(Self::Item, Self::Item) -> Self::Item + Clone,
		Self: Sized,
	{
		Combine::new(self, f)
	}

	#[must_use]
	fn max(self) -> Max<Self>
	where
		Self::Item: Ord,
		Self: Sized,
	{
		Max::new(self)
	}

	#[must_use]
	fn max_by<F>(self, f: F) -> MaxBy<Self, F>
	where
		Self: Sized,
		F: FnMut(&Self::Item, &Self::Item) -> Ordering,
	{
		MaxBy::new(self, f)
	}

	#[must_use]
	fn max_by_key<F, B>(self, f: F) -> MaxByKey<Self, F>
	where
		Self: Sized,
		F: FnMut(&Self::Item) -> B,
		B: Ord,
	{
		MaxByKey::new(self, f)
	}

	#[must_use]
	fn min(self) -> Min<Self>
	where
		Self::Item: Ord,
		Self: Sized,
	{
		Min::new(self)
	}

	#[must_use]
	fn min_by<F>(self, f: F) -> MinBy<Self, F>
	where
		Self: Sized,
		F: FnMut(&Self::Item, &Self::Item) -> Ordering,
	{
		MinBy::new(self, f)
	}

	#[must_use]
	fn min_by_key<F, B>(self, f: F) -> MinByKey<Self, F>
	where
		Self: Sized,
		F: FnMut(&Self::Item) -> B,
		B: Ord,
	{
		MinByKey::new(self, f)
	}

	#[must_use]
	fn most_frequent(self, n: usize, probability: f64, tolerance: f64) -> MostFrequent<Self>
	where
		Self::Item: Hash + Eq + Clone,
		Self: Sized,
	{
		MostFrequent::new(self, n, probability, tolerance)
	}

	#[must_use]
	fn most_distinct<A, B>(
		self, n: usize, probability: f64, tolerance: f64, error_rate: f64,
	) -> MostDistinct<Self>
	where
		Self: DistributedIteratorMulti<Source, Item = (A, B)>,
		A: Hash + Eq + Clone,
		B: Hash,
		Self: Sized,
	{
		MostDistinct::new(self, n, probability, tolerance, error_rate)
	}

	#[must_use]
	fn sample_unstable(self, samples: usize) -> SampleUnstable<Self>
	where
		Self: Sized,
	{
		SampleUnstable::new(self, samples)
	}

	#[must_use]
	fn all<F>(self, f: F) -> All<Self, F>
	where
		F: FnMut(Self::Item) -> bool + Clone,
		Self: Sized,
	{
		All::new(self, f)
	}

	#[must_use]
	fn collect<B>(self) -> Collect<Self, B>
	where
		Self: Sized,
	{
		Collect::new(self)
	}

	#[must_use]
	fn cloned<'a, Source1: 'a, T: 'a>(self) -> Cloned<Self, Source, T>
	where
		Self: Sized,
		Self: DistributedIteratorMulti<&'a Source1, Item = &'a T>,
		T: Clone,
	{
		Cloned::new(self)
	}
}

pub trait Consumer {
	type Item;
	fn run(self, i: &mut impl FnMut(Self::Item));
}
pub trait ConsumerMulti<Source> {
	type Item;
	fn run(&self, source: Source, i: &mut impl FnMut(Self::Item));
}

pub trait Reducer {
	type Item;
	type Output;
	fn push(&mut self, item: Self::Item);
	fn ret(self) -> Self::Output;
}

pub trait ReduceFactory {
	type Reducer: Reducer;
	fn make(&self) -> Self::Reducer;
}

pub trait DistributedReducer<I: DistributedIteratorMulti<Source>, Source, B> {
	type ReduceAFactory: ReduceFactory<Reducer = Self::ReduceA>;
	type ReduceA: Reducer<Item = <I as DistributedIteratorMulti<Source>>::Item>;
	type ReduceB: Reducer<Item = <Self::ReduceA as Reducer>::Output, Output = B>;
	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB);
}

unsafe fn type_transmute<T1, T2>(t1: T1) -> T2 {
	assert_eq!(
		(
			::std::intrinsics::type_name::<T1>(),
			::std::mem::size_of::<T1>(),
			::std::mem::align_of::<T1>()
		),
		(
			::std::intrinsics::type_name::<T2>(),
			::std::mem::size_of::<T2>(),
			::std::mem::align_of::<T2>()
		)
	);
	let ret = ::std::mem::transmute_copy(&t1);
	::std::mem::forget(t1);
	ret
}
