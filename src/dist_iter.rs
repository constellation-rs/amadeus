use either::Either;
use into_dist_iter::IntoDistributedIterator;
use serde::{de::DeserializeOwned, ser::Serialize};
use std::{hash::Hash, iter, marker, ops, vec};

pub trait DistributedIterator {
	type Item;
	type Task: Consumer<Item = Self::Item>;
	fn size_hint(&self) -> (usize, Option<usize>);
	fn next_task(&mut self) -> Option<Self::Task>;

	fn map<B, F>(self, f: F) -> Map<Self, F>
	where
		F: FnMut(Self::Item) -> B,
		Self: Sized,
	{
		Map { i: self, f }
	}

	fn flat_map<B, F>(self, f: F) -> FlatMap<Self, F>
	where
		F: FnMut(Self::Item) -> B,
		Self: Sized,
	{
		FlatMap { i: self, f }
	}

	fn chain<C>(self, chain: C) -> Chain<Self, C::Iter>
	where
		C: IntoDistributedIterator<Item = Self::Item>,
		Self: Sized,
	{
		Chain {
			a: self,
			b: chain.into_dist_iter(),
		}
	}

	fn reduce<A, B, F1, F2>(
		mut self, pool: &::process_pool::ProcessPool, reduce1: F1, mut reduce2: F2,
	) -> B
	where
		F1: Reducer<Item = Self::Item, Output = A>
			+ Clone
			+ Serialize
			+ DeserializeOwned
			+ Send
			+ Sync
			+ 'static,
		F2: Reducer<Item = A, Output = B>,
		A: Serialize + DeserializeOwned + Send + Sync + 'static,
		B: Serialize + DeserializeOwned + Send + Sync + 'static,
		Self::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
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
				let reduce1 = reduce1.clone();
				pool.spawn(FnOnce!([tasks,reduce1] move || -> A {
					let mut reduce1: F1 = reduce1;
					let tasks: Vec<Self::Task> = tasks;
					for task in tasks {
						task.run(&mut |item| reduce1.push(item));
					};
					reduce1.ret()
				}))
			})
			.collect::<Vec<_>>();
		for x in handles
			.into_iter()
			.map(FnMut!(|x: ::process_pool::JoinHandle<A>| -> A { x.join() }))
		{
			reduce2.push(x)
		}
		reduce2.ret()
	}

	// fn fold<ID, B, F>(self, pool: &::process_pool::ProcessPool, identity: ID, op: F) -> B
	// where
	// 	ID: FnMut() -> B + Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
	// 	B: Serialize + DeserializeOwned + Send + Sync + 'static,
	// 	F: FnMut(B, Either<Self::Item, B>) -> B
	// 		+ Clone
	// 		+ Serialize
	// 		+ DeserializeOwned
	// 		+ Send
	// 		+ Sync
	// 		+ 'static,
	// 	Self::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
	// 	Self::Item: 'static, // TODO: this shouldn't be needed?
	// 	Self: Sized,
	// {
	// 	let mut identity1 = identity.clone();
	// 	let op1 = op.clone();
	// 	#[derive(Serialize, Deserialize)]
	// 	#[serde(
	// 		bound(serialize = "ID: serde::ser::Serialize, F: serde::ser::Serialize"),
	// 		bound(deserialize = "ID: serde::de::Deserialize<'de>, F: serde::de::Deserialize<'de>")
	// 	)]
	// 	struct DistributedReducer<ID,F,B,Item> {
	// 		identity: ID,
	// 		op: F,
	// 		#[serde(skip)]
	// 		fold: Option<B>,
	// 		marker: marker::PhantomData<fn(Item)>,
	// 	}
	// 	impl<ID,F,B,Item> Clone for DistributedReducer<ID,F,B,Item> where ID: Clone, F: Clone {
	// 		fn clone(&self) -> Self {
	// 			assert!(self.fold.is_none());
	// 			DistributedReducer{identity:self.identity.clone(),op:self.op.clone(),fold:None,marker:marker::PhantomData}
	// 		}
	// 	}
	// 	impl<ID,F,B,Item> Reducer for DistributedReducer<ID,F,B,Item> where ID: FnMut() -> B + Clone + Serialize + DeserializeOwned + Send + Sync + 'static, F: FnMut(B, Either<Item, B>) -> B
	// 		+ Clone
	// 		+ Serialize
	// 		+ DeserializeOwned
	// 		+ Send
	// 		+ Sync
	// 		+ 'static {
	// 		type Item = Item;
	// 		type Output = B;
	// 		fn push(&mut self, item: Self::Item) {
	// 			if self.fold.is_none() {
	// 				self.fold = Some((self.identity)());
	// 			}
	// 			unimplemented!()
	// 		}
	// 		fn ret(self) -> Self::Output {
	// 			self.fold.unwrap_or_else(self.identity)
	// 		}
	// 	}
	// 	// FnMut!([identity,op]move|elems: Reducer<_,_>| { let identity: &mut ID = identity; let op: &mut F = op; elems.map(|x| Either::Left(x)).fold(identity(), op)})
	// 	self.reduce(pool, DistributedReducer{identity,op,fold:None,marker:marker::PhantomData},
	// 		DistributedReducer::<ID,F,B,B>{identity,op,fold:None,marker:marker::PhantomData}//|elems: ReduceB<B>| elems.map(|x| Either::Right(x)).fold(identity1(), op1)
	// 	)
	// 	// let handles = tasks
	// 	// 	.into_iter()
	// 	// 	.map(|tasks| {
	// 	// 		let (identity, op) = (identity.clone(), op.clone());
	// 	// 		pool.spawn(FnOnce!([tasks,identity,op] move || -> B {
	// 	// 			let mut identity = identity;
	// 	// 			tasks.into_iter().map(|x| Either::Left(x())).fold(identity(), op)
	// 	// 		}))
	// 	// 	}).collect::<Vec<_>>();
	// 	// handles
	// 	// 	.into_iter()
	// 	// 	.map(|x| Either::Right(x.join()))
	// 	// 	.fold(identity(), op)
	// }

	// fn count(self, pool: &::process_pool::ProcessPool) -> usize
	// where
	// 	Self::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
	// 	Self: Sized,
	// 	Self::Item: 'static, // TODO: this shouldn't be needed?
	// {
	// 	self.reduce(
	// 		pool,
	// 		FnMut!(|elems: Reducer<_, _>| elems.count()),
	// 		|elems: ReduceB<usize>| elems.sum(),
	// 	)
	// }

	// fn sum<S>(self, pool: &::process_pool::ProcessPool) -> S
	// where
	// 	S: iter::Sum<Self::Item>
	// 		+ iter::Sum<S>
	// 		+ Serialize
	// 		+ DeserializeOwned
	// 		+ Send
	// 		+ Sync
	// 		+ 'static,
	// 	Self::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
	// 	Self: Sized,
	// 	Self::Item: 'static, // TODO: this shouldn't be needed?
	// {
	// 	self.reduce(
	// 		pool,
	// 		FnMut!(|elems: Reducer<_, _>| elems.sum()),
	// 		|elems: ReduceB<S>| elems.sum(),
	// 	)
	// 	// self.fold(
	// 	// 	FnMut!(|| iter::empty::<Self::Item>().sum()),
	// 	// 	FnMut!(|a, b| {
	// 	// 		let b = match b {
	// 	// 			Either::Right(b) => b,
	// 	// 			Either::Left(b) => iter::once(b).sum(),
	// 	// 		};
	// 	// 		iter::once(a).chain(iter::once(b)).sum()
	// 	// 	}),
	// 	// 	pool,
	// 	// )
	// 	// let handles = tasks
	// 	// 	.into_iter()
	// 	// 	.map(|tasks| {
	// 	// 		pool.spawn(FnOnce!([tasks]move||->S{
	// 	// 		tasks.into_iter().map(|x|x()).sum()
	// 	// 	}))
	// 	// 	}).collect::<Vec<_>>();
	// 	// handles.into_iter().map(|x| x.join()).sum()
	// }

	// fn most_frequent<B, F>(
	// 	self, pool: &::process_pool::ProcessPool, n: usize, f: F,
	// ) -> ::streaming_algorithms::MostFrequent<B>
	// where
	// 	F: FnMut(Self::Item, &mut ::streaming_algorithms::MostFrequent<B>)
	// 		+ Clone
	// 		+ Serialize
	// 		+ DeserializeOwned
	// 		+ Send
	// 		+ Sync
	// 		+ 'static,
	// 	B: Eq + Clone + Hash + Serialize + DeserializeOwned + Send + Sync + 'static,
	// 	Self: Sized,
	// 	Self::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
	// 	Self::Item: 'static, // TODO: this shouldn't be needed?
	// {
	// 	self.reduce(
	// 		pool,
	// 		FnMut!([n,f] move |elems: Reducer<_,_>| {
	// 			let n: &mut usize = n;
	// 			let f: &mut F = f;
	// 			let mut most_frequent = ::streaming_algorithms::MostFrequent::new(*n);
	// 			for elem in elems {
	// 				f(elem, &mut most_frequent);
	// 			}
	// 			most_frequent
	// 		}),
	// 		|elems: ReduceB<::streaming_algorithms::MostFrequent<B>>| elems.sum(),
	// 	)
	// }

	// fn sample_unstable<B, F>(
	// 	self, pool: &::process_pool::ProcessPool, n: usize, f: F,
	// ) -> ::streaming_algorithms::SampleUnstable<B>
	// where
	// 	F: FnMut(Self::Item, &mut ::streaming_algorithms::SampleUnstable<B>)
	// 		+ Clone
	// 		+ Serialize
	// 		+ DeserializeOwned
	// 		+ Send
	// 		+ Sync
	// 		+ 'static,
	// 	B: Eq + Clone + Hash + Serialize + DeserializeOwned + Send + Sync + 'static,
	// 	Self: Sized,
	// 	Self::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
	// 	Self::Item: 'static, // TODO: this shouldn't be needed?
	// {
	// 	self.reduce(
	// 		pool,
	// 		FnMut!([n,f] move |elems: Reducer<_,_>| {
	// 			let n: &mut usize = n;
	// 			let f: &mut F = f;
	// 			let mut sample_unstable = ::streaming_algorithms::SampleUnstable::new(*n);
	// 			for elem in elems {
	// 				f(elem, &mut sample_unstable);
	// 			}
	// 			sample_unstable
	// 		}),
	// 		|elems: ReduceB<::streaming_algorithms::SampleUnstable<B>>| elems.sum(),
	// 	)
	// }

	// fn collect<B>(self, pool: &::process_pool::ProcessPool) -> B
	// where
	// 	B: FromDistributedIterator<Self::Item>
	// 		// + iter::IntoIterator<Item = Self::Item>
	// 		+ Serialize
	// 		+ DeserializeOwned
	// 		+ Send
	// 		+ Sync
	// 		+ 'static,
	// 	Self::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
	// 	Self: Sized,
	// 	Self::Item: 'static, // TODO: this shouldn't be needed?
	// {
	// 	B::from_dist_iter(self, pool)
	// }

	fn multi<IteratorA, IteratorB, ReducerA, ReducerB, A, B>(
		self, pool: &::process_pool::ProcessPool, reducer_a: ReducerA, reducer_b: ReducerB,
	) -> (A, B)
	where
		IteratorA: DistributedIteratorMulti<Self::Item>,
		IteratorA::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
		IteratorB: for<'a> DistributedIteratorMulti<&'a Self::Item> + 'static,
		for<'a> <IteratorB as DistributedIteratorMulti<&'static Self::Item>>::Task:
			Serialize + DeserializeOwned + Send + Sync + 'static,

		ReducerA: DistributedReducer<IteratorA, Self::Item, A>,
		<ReducerA as DistributedReducer<IteratorA, Self::Item, A>>::ReduceA:
			Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
		<ReducerA as DistributedReducer<IteratorA, Self::Item, A>>::Mid:
			Serialize + DeserializeOwned + Send + Sync + 'static,
		for<'a> <ReducerB as DistributedReducer<IteratorB, &'static Self::Item, B>>::Mid:
			Serialize + DeserializeOwned + Send + Sync + 'static,
		for<'a> ReducerB: DistributedReducer<IteratorB, &'static Self::Item, B> + 'static,
		for<'a> <ReducerB as DistributedReducer<IteratorB, &'static Self::Item, B>>::ReduceA:
			Clone + Serialize + DeserializeOwned + Send + Sync + 'static,

		A: Serialize + DeserializeOwned + Send + Sync + 'static,
		B: Serialize + DeserializeOwned + Send + Sync + 'static,
		Self::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
		Self::Item: 'static, // TODO: this shouldn't be needed?
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
			type Item = Either<B::Item, CItem>;
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
		struct ConnectConsumer<A, B, C, CItem>(A, B, C, marker::PhantomData<fn(CItem)>);
		impl<A: Consumer, B, C, CItem> Consumer for ConnectConsumer<A, B, C, CItem>
		where
			B: ConsumerMulti<A::Item>,
		{
			type Item = Either<B::Item, CItem>;
			fn run(self, mut i: &mut impl FnMut(Self::Item)) {
				let a = self.1;
				let b = self.2;
				self.0.run(&mut |item| {
					let i = &mut i;
					trait ConsumerReducerHack<Source> {
						type Item;
						fn run(&self, Source, &mut impl FnMut(Self::Item));
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
						type Item = T::Item;
						fn run(&self, source: Source, i: &mut impl FnMut(Self::Item)) {
							ConsumerMulti::<Source>::run(self, source, i)
						}
					}
					ConsumerReducerHack::<&A::Item>::run(&b, &item, &mut |item| {
						i(Either::Right(unsafe { type_transmute(item) }))
					});
					a.run(item, &mut |item| i(Either::Left(item)));
				})
			}
		}

		#[derive(Clone, Serialize, Deserialize)]
		struct ReduceA<A, B>(A, B);
		impl<A: Reducer, B: Reducer> Reducer for ReduceA<A, B> {
			type Item = Either<A::Item, B::Item>;
			type Output = (A::Output, B::Output);
			fn push(&mut self, item: Self::Item) {
				match item {
					Either::Left(item) => self.0.push(item),
					Either::Right(item) => self.1.push(item),
				}
			}
			fn ret(self) -> Self::Output {
				(self.0.ret(), self.1.ret())
			}
		}
		struct ReduceB<A, B>(A, B);
		impl<A: Reducer, B: Reducer> Reducer for ReduceB<A, B> {
			type Item = (A::Item, B::Item);
			type Output = (A::Output, B::Output);
			fn push(&mut self, item: Self::Item) {
				self.0.push(item.0);
				self.1.push(item.1);
			}
			fn ret(self) -> Self::Output {
				(self.0.ret(), self.1.ret())
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
			ReduceA(reducer_a_a, reducer_b_a),
			ReduceB(reducer_a_b, reducer_b_b),
		)
	}
}

pub trait DistributedIteratorMulti<Source> {
	type Item;
	type Task: ConsumerMulti<Source, Item = Self::Item>;

	fn task(&self) -> Self::Task;

	fn map<B, F>(self, f: F) -> Map<Self, F>
	where
		F: FnMut(Self::Item) -> B,
		Self: Sized,
	{
		Map { i: self, f }
	}

	fn flat_map<B, F>(self, f: F) -> FlatMap<Self, F>
	where
		F: FnMut(Self::Item) -> B,
		Self: Sized,
	{
		FlatMap { i: self, f }
	}

	fn chain<C>(self, chain: C) -> Chain<Self, C::Iter>
	where
		C: IntoDistributedIterator<Item = Self::Item>,
		Self: Sized,
	{
		Chain {
			a: self,
			b: chain.into_dist_iter(),
		}
	}

	#[must_use]
	fn collect<B>(self) -> Collect<Self, B>
	where
		Self: Sized,
	{
		Collect {
			i: self,
			b: marker::PhantomData,
		}
	}
}

pub trait Consumer {
	type Item;
	fn run(self, &mut impl FnMut(Self::Item));
}
pub trait ConsumerMulti<Source> {
	type Item;
	fn run(&self, Source, &mut impl FnMut(Self::Item));
}

pub trait Reducer {
	type Item;
	type Output;
	fn push(&mut self, Self::Item);
	fn ret(self) -> Self::Output;
}

pub trait DistributedReducer<I: DistributedIteratorMulti<Source>, Source, B> {
	type Mid;
	type ReduceA: Reducer<Item = <I as DistributedIteratorMulti<Source>>::Item, Output = Self::Mid>;
	type ReduceB: Reducer<Item = Self::Mid, Output = B>;
	fn reducers(self) -> (I, Self::ReduceA, Self::ReduceB);
}

pub struct Identity;
impl<X> DistributedIteratorMulti<X> for Identity {
	type Item = X;
	type Task = IdentityMultiTask;
	fn task(&self) -> Self::Task {
		IdentityMultiTask
	}
}

#[derive(Serialize, Deserialize)]
pub struct IdentityMultiTask;
impl<X> ConsumerMulti<X> for IdentityMultiTask {
	type Item = X;
	fn run(&self, source: X, i: &mut impl FnMut(Self::Item)) {
		i(source)
	}
}

// pub trait FromDistributedIterator<T> {
// 	fn from_dist_iter<I>(dist_iter: I, pool: &::process_pool::ProcessPool) -> Self where I: IntoDistributedIterator<Item = T>, <<I as IntoDistributedIterator>::Iter as DistributedIterator>::Task: Serialize + DeserializeOwned + Send + Sync + 'static;
// }

// impl<T> FromDistributedIterator<T> for Vec<T> where T: Serialize + DeserializeOwned + Send + Sync + 'static {
// 	fn from_dist_iter<I>(dist_iter: I, pool: &::process_pool::ProcessPool) -> Self where I: IntoDistributedIterator<Item = T>, <<I as IntoDistributedIterator>::Iter as DistributedIterator>::Task: Serialize + DeserializeOwned + Send + Sync + 'static {
// 		dist_iter.into_dist_iter().reduce(
// 			pool,
// 			// FnMut!(|elems: ReduceA<_, _>| elems.collect::<B>()),
// 			CollectReducerA(Vec::new()),
// 			CollectReducerB(Vec::new()),
// 			// |elems: ReduceB<Vec<T>>| elems.flat_map(|x| x.into_iter()).collect(),
// 		)
// 	}
// }

#[derive(Serialize, Deserialize)]
pub struct CollectReducerA<T>(T);
impl<T> Clone for CollectReducerA<Vec<T>> {
	fn clone(&self) -> Self {
		assert!(self.0.is_empty());
		CollectReducerA(Vec::new())
	}
}
impl<T> Reducer for CollectReducerA<Vec<T>> {
	type Item = T;
	type Output = Vec<T>;
	fn push(&mut self, item: Self::Item) {
		self.0.push(item)
	}
	fn ret(self) -> Self::Output {
		self.0
	}
}

#[derive(Serialize, Deserialize)]
pub struct CollectReducerB<T>(T);
impl<T> Clone for CollectReducerB<Vec<T>> {
	fn clone(&self) -> Self {
		assert!(self.0.is_empty());
		CollectReducerB(Vec::new())
	}
}
impl<T> Reducer for CollectReducerB<Vec<T>> {
	type Item = Vec<T>;
	type Output = Vec<T>;
	fn push(&mut self, item: Self::Item) {
		self.0.extend(item)
	}
	fn ret(self) -> Self::Output {
		self.0
	}
}

#[must_use]
pub struct Collect<I, A> {
	i: I,
	b: marker::PhantomData<fn() -> A>,
}
impl<I: DistributedIteratorMulti<Source>, A, Source> DistributedReducer<I, Source, Vec<A>>
	for Collect<I, Vec<A>>
where
	CollectReducerA<Vec<A>>:
		Reducer<Item = <I as DistributedIteratorMulti<Source>>::Item, Output = Vec<A>>,
{
	type Mid = Vec<A>;
	type ReduceA = CollectReducerA<Vec<A>>;
	type ReduceB = CollectReducerB<Vec<A>>;
	fn reducers(self) -> (I, Self::ReduceA, Self::ReduceB) {
		(
			self.i,
			CollectReducerA(Vec::new()),
			CollectReducerB(Vec::new()),
		)
	}
}

// #[must_use]
// pub struct Sum<I, B> {
// 	i: I,
// 	b: marker::PhantomData<fn() -> B>,
// }
// impl<I: DistributedIteratorMulti<Source>, A, Source: DistributedIterator> DistributedReducer<I, A, A, Source>
// 	for Sum<I, A>
// where
// 	A: Send + Sync + Serialize + DeserializeOwned + 'static,
// 	I::Task: Send + Sync + Serialize + DeserializeOwned + 'static,
// 	A: iter::Sum<I::Item> + iter::Sum<A>,
// {
// 	type ReduceA = sc::FnMut<(), fn(&mut (), (ReduceA<I::Task, I::Item>,)) -> A>;
// 	type ReduceB = sc::FnMut<(), fn(&mut (), (ReduceB<A>,)) -> A>;
// 	fn reducers(self) -> (I, Self::ReduceA, Self::ReduceB) {
// 		(
// 			self.i,
// 			FnMut!(|elems: ReduceA<_, _>| elems.sum::<A>()),
// 			FnMut!(|elems: ReduceB<A>| elems.sum()), // TODO doesn't need to be sc
// 		)
// 	}
// }

#[derive(Serialize, Deserialize)]
pub struct InspectConsumer<T, F> {
	task: T,
	f: F,
}
impl<C: Consumer, F: FnMut(&C::Item)> Consumer for InspectConsumer<C, F> {
	type Item = C::Item;
	fn run(self, i: &mut impl FnMut(Self::Item)) {
		let (task, mut f) = (self.task, self.f);
		task.run(&mut |item| {
			f(&item);
			i(item)
		})
	}
}

pub struct Map<I, F> {
	i: I,
	f: F,
}
impl<I: DistributedIterator, F: FnMut(I::Item) -> R + Clone, R> DistributedIterator for Map<I, F> {
	type Item = R;
	type Task = MapConsumer<I::Task, F>;
	fn size_hint(&self) -> (usize, Option<usize>) {
		self.i.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.i.next_task().map(|task| {
			let f = self.f.clone();
			MapConsumer { task, f }
		})
	}
}
impl<I: DistributedIteratorMulti<Source>, F, R, Source> DistributedIteratorMulti<Source>
	for Map<I, F>
where
	F: FnMut(<I as DistributedIteratorMulti<Source>>::Item) -> R + Clone,
{
	type Item = F::Output;
	type Task = MapConsumer<I::Task, F>;
	fn task(&self) -> Self::Task {
		let task = self.i.task();
		let f = self.f.clone();
		MapConsumer { task, f }
	}
}
#[derive(Serialize, Deserialize)]
pub struct MapConsumer<T, F> {
	task: T,
	f: F,
}
impl<C: Consumer, F: FnMut(C::Item) -> R + Clone, R> Consumer for MapConsumer<C, F> {
	type Item = R;
	fn run(self, i: &mut impl FnMut(Self::Item)) {
		let (task, mut f) = (self.task, self.f);
		task.run(&mut |item| i(f(item)))
	}
}
impl<
		C: ConsumerMulti<Source>,
		F: FnMut(<C as ConsumerMulti<Source>>::Item) -> R + Clone,
		R,
		Source,
	> ConsumerMulti<Source> for MapConsumer<C, F>
{
	type Item = R;
	fn run(&self, source: Source, i: &mut impl FnMut(Self::Item)) {
		let (task, f) = (&self.task, &self.f);
		task.run(source, &mut |item| i(f.clone()(item)))
	}
}

pub struct Chain<A, B> {
	a: A,
	b: B,
}
impl<A: DistributedIterator, B: DistributedIterator<Item = A::Item>> DistributedIterator
	for Chain<A, B>
{
	type Item = A::Item;
	type Task = ChainConsumer<A::Task, B::Task>;
	fn size_hint(&self) -> (usize, Option<usize>) {
		let (a_lower, a_upper) = self.a.size_hint();
		let (b_lower, b_upper) = self.b.size_hint();
		(
			a_lower + b_lower,
			if let (Some(a), Some(b)) = (a_upper, b_upper) {
				Some(a + b)
			} else {
				None
			},
		)
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.a
			.next_task()
			.map(ChainConsumer::A)
			.or_else(|| self.b.next_task().map(ChainConsumer::B))
	}
}
#[derive(Serialize, Deserialize)]
pub enum ChainConsumer<A, B> {
	A(A),
	B(B),
}
impl<A: Consumer, B: Consumer<Item = A::Item>> Consumer for ChainConsumer<A, B> {
	type Item = A::Item;
	fn run(self, i: &mut impl FnMut(Self::Item)) {
		match self {
			ChainConsumer::A(a) => a.run(i),
			ChainConsumer::B(b) => b.run(i),
		}
	}
}

pub struct FlatMap<I, F> {
	i: I,
	f: F,
}
impl<I: DistributedIterator, F: FnMut(I::Item) -> R + Clone, R: IntoIterator> DistributedIterator
	for FlatMap<I, F>
{
	type Item = R::Item;
	type Task = FlatMapConsumer<I::Task, F>;
	fn size_hint(&self) -> (usize, Option<usize>) {
		self.i.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.i.next_task().map(|task| {
			let f = self.f.clone();
			FlatMapConsumer { task, f }
		})
	}
}

#[derive(Serialize, Deserialize)]
pub struct FlatMapConsumer<T, F> {
	task: T,
	f: F,
}
impl<C: Consumer, F: FnMut(C::Item) -> R + Clone, R: IntoIterator> Consumer
	for FlatMapConsumer<C, F>
{
	type Item = R::Item;
	fn run(self, i: &mut impl FnMut(Self::Item)) {
		let (task, mut f) = (self.task, self.f);
		task.run(&mut |item| {
			for x in f(item) {
				i(x)
			}
		})
	}
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
