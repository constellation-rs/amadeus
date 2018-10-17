// use serde_closure as sc;

use either::Either;
use serde::{de::DeserializeOwned, ser::Serialize};
use std::{hash::Hash, iter, marker, ops, slice, vec};

pub trait IteratorExt: Iterator + Sized {
	fn dist(self) -> IterIter<Self> {
		IterIter(self)
	}
}
impl<I: Iterator + Sized> IteratorExt for I {}

pub struct IterIter<I>(I);
impl<I: Iterator> DistributedIterator for IterIter<I> {
	type Item = I::Item;
	type Task = IterIterConsumer<I::Item>;
	fn size_hint(&self) -> (usize, Option<usize>) {
		self.0.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.0.next().map(IterIterConsumer)
	}
}
#[derive(Serialize, Deserialize)]
pub struct IterIterConsumer<T>(T);
impl<T> Consumer for IterIterConsumer<T> {
	type Item = T;
	fn run(self, i: &mut impl FnMut(Self::Item)) {
		// println!("A");
		i(self.0)
	}
}

pub trait IntoDistributedIterator {
	// where for <'a> &'a Self: IntoDistributedIterator, for <'a> &'a mut Self: IntoDistributedIterator {
	type Iter: DistributedIterator<Item = Self::Item>;
	type Item;
	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized;
	fn dist_iter_mut(&mut self) -> <&mut Self as IntoDistributedIterator>::Iter
	where
		for<'a> &'a mut Self: IntoDistributedIterator,
	{
		<&mut Self as IntoDistributedIterator>::into_dist_iter(self)
	}
	fn dist_iter(&self) -> <&Self as IntoDistributedIterator>::Iter
	where
		for<'a> &'a Self: IntoDistributedIterator,
	{
		<&Self as IntoDistributedIterator>::into_dist_iter(self)
	}
}
impl<T> IntoDistributedIterator for Vec<T> {
	type Iter = IntoIter<T>;
	type Item = T;
	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IntoIter(self.into_iter())
	}
}
impl<'a, T: Clone> IntoDistributedIterator for &'a Vec<T> {
	type Iter = IterRef<'a, T>;
	type Item = T;
	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterRef(self.iter())
	}
}
impl<T> IntoDistributedIterator for [T] {
	type Iter = Never;
	type Item = Never;
	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		unreachable!()
	}
}
impl DistributedIterator for Never {
	type Item = Never;
	type Task = Never;
	fn size_hint(&self) -> (usize, Option<usize>) {
		unreachable!()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		unreachable!()
	}
}
impl<'a, T: Clone> IntoDistributedIterator for &'a [T] {
	type Iter = IterRef<'a, T>;
	type Item = T;
	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterRef(self.iter())
	}
}

pub enum Never {}
impl Consumer for Never {
	type Item = Never;
	fn run(self, _: &mut impl FnMut(Self::Item)) {
		unreachable!()
	}
}

impl<T: DistributedIterator> IntoDistributedIterator for T {
	type Iter = T;
	type Item = T::Item;
	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		self
	}
}

trait Consumer {
	type Item;
	fn run(self, &mut impl FnMut(Self::Item));
}
trait ConsumerReducer<Source> {
	type Item;
	fn run(&self, Source, &mut impl FnMut(Self::Item));
}
// struct Xxx<C: ConsumerReducer + ?Sized>(marker::PhantomData<fn(C)>);
// impl<C: ConsumerReducer + ?Sized> ops::FnOnce<(<C as ConsumerReducer>::Source,)> for Xxx<C> {
// 	type Output = ();//Box<dyn ConsumerReducer<Source=C::Source,Item=C::Item>>;
// 	extern "rust-call" fn call_once(self, args: (<C as ConsumerReducer>::Source,)) -> () {
// 		unimplemented!()
// 	}
// }
// trait ConsumerReducerA: for <'a> ConsumerReducer<'a> {}
// impl<T> ConsumerReducerA for T where for <'a> T: ConsumerReducer<'a> {}

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
		F1: ReduceA<Item = Self::Item, Output = A>
			+ Clone
			+ Serialize
			+ DeserializeOwned
			+ Send
			+ Sync
			+ 'static,
		F2: ReduceA<Item = A, Output = B>, // FnOnce(/*impl Iterator<Item = A>*/ ReduceB<A>) -> B,
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
					// unimplemented!()
					// println!("{:?}", tasks.len());
					for task in tasks {
						// println!("run");
						task.run(&mut |item|{/*println!("ah");*/reduce1.push(item)});
					};
					reduce1.ret()
					// reduce1(ReduceA(tasks.into_iter().flat_map(FnMut!(|x: Self::Task| -> Vec<Self::Item> { x() }))))
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
	// 	struct Reducer<ID,F,B,Item> {
	// 		identity: ID,
	// 		op: F,
	// 		#[serde(skip)]
	// 		fold: Option<B>,
	// 		marker: marker::PhantomData<fn(Item)>,
	// 	}
	// 	impl<ID,F,B,Item> Clone for Reducer<ID,F,B,Item> where ID: Clone, F: Clone {
	// 		fn clone(&self) -> Self {
	// 			assert!(self.fold.is_none());
	// 			Reducer{identity:self.identity.clone(),op:self.op.clone(),fold:None,marker:marker::PhantomData}
	// 		}
	// 	}
	// 	impl<ID,F,B,Item> ReduceA for Reducer<ID,F,B,Item> where ID: FnMut() -> B + Clone + Serialize + DeserializeOwned + Send + Sync + 'static, F: FnMut(B, Either<Item, B>) -> B
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
	// 	// FnMut!([identity,op]move|elems: ReduceA<_,_>| { let identity: &mut ID = identity; let op: &mut F = op; elems.map(|x| Either::Left(x)).fold(identity(), op)})
	// 	self.reduce(pool, Reducer{identity,op,fold:None,marker:marker::PhantomData},
	// 		Reducer::<ID,F,B,B>{identity,op,fold:None,marker:marker::PhantomData}//|elems: ReduceB<B>| elems.map(|x| Either::Right(x)).fold(identity1(), op1)
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
	// 		FnMut!(|elems: ReduceA<_, _>| elems.count()),
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
	// 		FnMut!(|elems: ReduceA<_, _>| elems.sum()),
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
	// 		FnMut!([n,f] move |elems: ReduceA<_,_>| {
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
	// 		FnMut!([n,f] move |elems: ReduceA<_,_>| {
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

	fn execute_<B, B1, R, R1>(mut self, pool: &::process_pool::ProcessPool, x: R, y: R1) -> (B, B1)
	where
		B: Serialize + DeserializeOwned + Send + Sync + 'static,
		B1: Serialize + DeserializeOwned + Send + Sync + 'static,
		Self::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
		R: DistributedIteratorExecute<Self::Item>,
		R1: for<'a> DistributedIteratorExecute<&'a Self::Item>,
		R::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
		for<'a> <R1 as DistributedIteratorExecute<&'static Self::Item>>::Task:
			Serialize + DeserializeOwned + Send + Sync + 'static,
		// for<'a> <ExecutorB as DistributedIteratorExecute<&'a <Self as DistributedIterator>::Item>>::Task: ConsumerReducer<&'a <Self as DistributedIterator>::Item>,
		// <R as Reducer<ExecutorA, Self::Item, B>>::I: DistributedIteratorExecute<Source=Self::Item>,
		// <<R as Reducer<ExecutorA, Self::Item, B>>::I as DistributedIteratorExecute<Self::Item>>::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
		// for <'a> <<R1 as Reducer<ExecutorB, &'a Self::Item, B1>>::I as DistributedIteratorExecute<&'a Self::Item>>::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
		// Xxx<<<R1 as Reducer<ExecutorB, &'a Self::Item, B1>>::I as DistributedIteratorExecute>::Task>: FnOnce(&Self::Item),
		// <<R as Reducer<ExecutorA, A,B>>::I as DistributedIteratorExecute>::Task: ConsumerReducer<'a,Source=Self::Item>
		// <R as Reducer<ExecutorA, A,B>>::I: DistributedIteratorExecute<Source=Self::Item>,
		// for <'a> <R1 as Reducer<ExecutorB, A1,B1>>::I: DistributedIteratorExecute<Source=&'a Self::Item>,
		// I::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
		// <I2 as DistributedIteratorExecute<DistributedIteratorRef<'static,Self>>>::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
		// Self::Item: Clone,
		Self::Item: 'static, // TODO: this shouldn't be needed?
		// I2: 'static,
		// Self: 'static,
		// I::Item: 'static, // TODO: this shouldn't be needed?
		// <I2 as DistributedIteratorExecute<DistributedIteratorRef<'static,Self>>>::Item: 'static, // TODO: this shouldn't be needed?
		// R: Reducer<ExecutorA, Self::Item, B>,
		// <R as Reducer<ExecutorA, Self::Item, B>>::ReduceA:
		// 	Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
		// for <'a> R1: Reducer<ExecutorB, &'a Self::Item, B1> + 'static,
		// for <'a> <R1 as Reducer<ExecutorB, &'a Self::Item, B1>>::ReduceA:
		// 	Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
		Self: Sized,
	{
		unimplemented!()
	}

	fn execute<
		ExecutorA,
		ExecutorB,
		/*I: DistributedIteratorExecute<Self>, I2: for <'a> DistributedIteratorExecute<DistributedIteratorRef<'a,Self>>,*/ /*A, A1,*/
		B,
		B1,
		R,
		R1, /*, F1*/
	>(
		mut self, pool: &::process_pool::ProcessPool, x: R, y: R1,
	) -> (B, B1)
	where
		// F1: for<'a> FnOnce(ExecuteB<'a,Self::Item>) -> R1,
		<R as Reducer<ExecutorA, Self::Item, B>>::Mid:
			Serialize + DeserializeOwned + Send + Sync + 'static,
		B: Serialize + DeserializeOwned + Send + Sync + 'static,
		for<'a> <R1 as Reducer<ExecutorB, &'static Self::Item, B1>>::Mid:
			Serialize + DeserializeOwned + Send + Sync + 'static,
		B1: Serialize + DeserializeOwned + Send + Sync + 'static,
		Self::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
		// <R::I as DistributedIteratorExecute<Self::Item>>::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
		// for<'a> <<R1 as Reducer<ExecutorB, &'a Self::Item, B1>>::I as DistributedIteratorExecute<&'a Self::Item>>::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
		// for<'a> <R1 as Reducer<ExecutorB, &'a Self::Item, B1>>::I: DistributedIteratorExecute<&'a Self::Item>,
		ExecutorA: DistributedIteratorExecute<Self::Item>,
		ExecutorB: for<'a> DistributedIteratorExecute<&'a Self::Item> + 'static,
		ExecutorA::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
		for<'a> <ExecutorB as DistributedIteratorExecute<&'static Self::Item>>::Task:
			Serialize + DeserializeOwned + Send + Sync + 'static,
		// for<'a> <ExecutorB as DistributedIteratorExecute<&'a <Self as DistributedIterator>::Item>>::Task: ConsumerReducer<&'a <Self as DistributedIterator>::Item>,
		// <R as Reducer<ExecutorA, Self::Item, B>>::I: DistributedIteratorExecute<Source=Self::Item>,
		// <<R as Reducer<ExecutorA, Self::Item, B>>::I as DistributedIteratorExecute<Self::Item>>::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
		// for <'a> <<R1 as Reducer<ExecutorB, &'a Self::Item, B1>>::I as DistributedIteratorExecute<&'a Self::Item>>::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
		// Xxx<<<R1 as Reducer<ExecutorB, &'a Self::Item, B1>>::I as DistributedIteratorExecute>::Task>: FnOnce(&Self::Item),
		// <<R as Reducer<ExecutorA, A,B>>::I as DistributedIteratorExecute>::Task: ConsumerReducer<'a,Source=Self::Item>
		// <R as Reducer<ExecutorA, A,B>>::I: DistributedIteratorExecute<Source=Self::Item>,
		// for <'a> <R1 as Reducer<ExecutorB, A1,B1>>::I: DistributedIteratorExecute<Source=&'a Self::Item>,
		// I::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
		// <I2 as DistributedIteratorExecute<DistributedIteratorRef<'static,Self>>>::Task: Serialize + DeserializeOwned + Send + Sync + 'static,
		// Self::Item: Clone,
		Self::Item: 'static, // TODO: this shouldn't be needed?
		// I2: 'static,
		// Self: 'static,
		// I::Item: 'static, // TODO: this shouldn't be needed?
		// <I2 as DistributedIteratorExecute<DistributedIteratorRef<'static,Self>>>::Item: 'static, // TODO: this shouldn't be needed?
		R: Reducer<ExecutorA, Self::Item, B>,
		<R as Reducer<ExecutorA, Self::Item, B>>::ReduceA:
			Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
		for<'a> R1: Reducer<ExecutorB, &'static Self::Item, B1> + 'static,
		for<'a> <R1 as Reducer<ExecutorB, &'static Self::Item, B1>>::ReduceA:
			Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
		Self: Sized,
	{
		// unimplemented!()
		let reduce1 = x; //(ExecuteA(marker::PhantomData));
		let reduce2 = y; //(ExecuteB(marker::PhantomData));
		let (i, reduce_a, reduce_b) = reduce1.reducers();
		let (i1, reduce_a1, reduce_b1) = reduce2.reducers();
		// while let Some(task) = self.next_task() {
		// 	// let mut reducer = i.task();
		// 	// ConsumerReducer::run((), , &mut reducer);
		// 	task.run(&mut |item|{i.task().run(item, &mut |x|())});
		// }
		struct Connect<A, B, C, CTask, CItem>(A, B, C, marker::PhantomData<fn(CTask, CItem)>);
		impl<
				A: DistributedIterator,
				B: DistributedIteratorExecute<A::Item>,
				C: for<'a> DistributedIteratorExecute<&'a A::Item>, /*<DistributedIteratorRef<'a,A>>*/
				CTask,
				CItem,
			> DistributedIterator for Connect<A, B, C, CTask, CItem>
		where
		// for<'a> CTask: ConsumerReducer<&'a A::Item>,
		// B::Task: ConsumerReducer<A::Item>,
		// for<'a> C::Task: ConsumerReducer<&'a A::Item>,
		// Xxx<C::Task>: FnOnce(&A::Item),
		// A::Item: 'static,
		{
			type Item = Either<B::Item, CItem>; // <Self::Task as Consumer>::Item;// Either<B::Item,<C as DistributedIteratorExecuteHack<A::Item>>::ItemHack>;
			type Task = ConnectConsumer<A::Task, B::Task, CTask, CItem>;
			fn size_hint(&self) -> (usize, Option<usize>) {
				self.0.size_hint()
				// self.1.size_hint(&self.0)
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
				// self.1.next_task(&mut self.0)
			}
		}
		#[derive(Serialize, Deserialize)]
		// #[serde(
		// 	bound(serialize = "A: serde::ser::Serialize, B: serde::ser::Serialize, <C as DistributedIteratorExecuteHack<A::Item>>::TaskHack: serde::ser::Serialize"),
		// 	bound(deserialize = "A: serde::de::Deserialize<'de>, B: serde::de::Deserialize<'de>, <C as DistributedIteratorExecuteHack<A::Item>>::TaskHack: serde::de::Deserialize<'de>"),
		// )]
		struct ConnectConsumer<A, B, C, CItem>(A, B, C, marker::PhantomData<fn(CItem)>); //<C as DistributedIteratorExecuteHack<A::Item>>::TaskHack) where A: Consumer, C: for<'a> DistributedIteratorExecute<&'a A::Item>;
		impl<A: Consumer, B, C, CItem> Consumer for ConnectConsumer<A, B, C, CItem>
		where
			B: ConsumerReducer<A::Item>,
			// C: for<'a> ConsumerReducer<&'a A::Item>,
			// for<'a> C: ConsumerReducer<&'a A::Item,Item=O>,
			// Using FnOnce here is a hack to avoid the error: "binding for associated type `Source` references lifetime `'a`, which does not appear in the trait input types"
			// Infecting the ConsumerExecute trait with a lifetime is problematic as unfortunately combining HRTBs with associated types surfaces results in numerous compiler bugs and ICEs.
			// Xxx<C>: FnOnce(&A::Item),
			// A::Item: 'static
		{
			type Item = Either<B::Item, CItem>; //<C as DistributedIteratorExecuteHack<A::Item>>::ItemHack>;
			fn run(self, mut i: &mut impl FnMut(Self::Item)) {
				// unimplemented!()
				let mut a = self.1;
				let mut b = self.2;
				self.0.run(&mut |item| {
					let mut i = &mut i;
					// This is safe as source and dest types are guaranteed to be identical by the type system, the compiler just can't quite grok our proof.
					// assert_eq!(any::TypeId::of::<&A::Item>(), any::TypeId::of::<<C as ConsumerExecute>::Source>());
					// b.run(unsafe{ignore_lifetime(&item)}, &mut |item|i(Either::Right(item)));
					// b.run(unsafe { ::std::mem::transmute_copy(&&item) }, &mut |item|i(Either::Right(item)));
					trait ConsumerReducerHack<Source> {
						type Item;
						fn run(&self, Source, &mut impl FnMut(Self::Item));
					}
					impl<T, Source> ConsumerReducerHack<Source> for T {
						default type Item = !;
						default fn run(&self, source: Source, i: &mut impl FnMut(Self::Item)) {
							unreachable!()
						}
					}
					impl<T, Source> ConsumerReducerHack<Source> for T
					where
						T: ConsumerReducer<Source>,
					{
						type Item = T::Item;
						fn run(&self, source: Source, i: &mut impl FnMut(Self::Item)) {
							ConsumerReducer::<Source>::run(self, source, i)
						}
					}
					ConsumerReducerHack::<&A::Item>::run(&b, &item, &mut |item| {
						i(Either::Right(unsafe { type_transmute(item) }))
					}); //.run(&item, &mut |item|i(Either::Right(unsafe{type_transmute(item)})));
					a.run(item, &mut |item| i(Either::Left(item)));
					// b.run(&item, i)
					// unimplemented!()
				})
			}
		}
		// trait ConsumerReducerA: for<'a> ConsumerReducer<'a,Item=<Self as ConsumerReducerA>::Item> where {
		// 	type Item;
		// }
		// impl<T> ConsumerReducerA for T where T: for<'a> ConsumerReducer<'a> {
		// 	type Item = <T as ConsumerReducer<'a>>::Item;
		// }
		// Connect(self, i);
		// unimplemented!()
		// struct Connect<A, B>(A, B);
		// impl<A: DistributedIterator, B: DistributedIteratorExecute<A>> DistributedIterator
		// 	for Connect<A, B> where
		// {
		// 	type Item = B::Item;
		// 	type Task = B::Task;
		// 	fn size_hint(&self) -> (usize, Option<usize>) {
		// 		self.1.size_hint(&self.0)
		// 	}
		// 	fn next_task(&mut self) -> Option<Self::Task> {
		// 		self.1.next_task(&mut self.0)
		// 	}
		// }
		// let reduce = x(ExecuteA(marker::PhantomData));
		// let (i, reduce_a, reduce_b) = reduce.reducers();
		impl<A: ReduceA, B: ReduceA> ReduceA for (A, B) {
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
		struct ZZ<A, B>(A, B);
		impl<A: ReduceA, B: ReduceA> ReduceA for ZZ<A, B> {
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
		// Connect::size_hint(&Connect::<_,_,_,<ExecutorB as DistributedIteratorExecute<&Self::Item>>::Task,<<ExecutorB as DistributedIteratorExecute<&Self::Item>>::Task as ConsumerReducer<&Self::Item>>::Item>(unsafe{::std::mem::uninitialized()},unsafe{::std::mem::uninitialized()},unsafe{::std::mem::uninitialized()}, marker::PhantomData));
		// unimplemented!()
		DistributedIterator::reduce(
			Connect::<_, _, _, <ExecutorB as DistributedIteratorExecute<&Self::Item>>::Task, _>(
				self,
				i,
				i1,
				marker::PhantomData,
			),
			pool,
			(reduce_a, reduce_a1),
			ZZ(reduce_b, reduce_b1),
		)
	}

	// #[must_use]
	// fn execute_collect<B>(self) -> Collect<Self,B> where Self: Sized, {
	// 	Collect{i:self,b:marker::PhantomData}
	// }
}

pub struct Identity;
impl<X> DistributedIteratorExecute<X> for Identity {
	type Item = X;
	type Task = IdentityMultiTask;
	fn task(&self) -> Self::Task {
		IdentityMultiTask
	}
}
// impl<X> IteratorMultiHack<X> for Identity {
// 	type TaskHack = IdentityMultiTask;
// }
#[derive(Serialize, Deserialize)]
pub struct IdentityMultiTask;
impl<X> ConsumerReducer<X> for IdentityMultiTask {
	type Item = X;
	fn run(&self, source: X, i: &mut impl FnMut(Self::Item)) {
		i(source)
	}
}

// trait DistributedIteratorExecuteHack<Source> where for<'a> Self: DistributedIteratorExecute<&'a Source> {
// 	type TaskHack;
// 	type ItemHack;
// }
// impl<T,S> DistributedIteratorExecuteHack<S> for T where for<'a> T: DistributedIteratorExecute<&'a S> {
// 	default type TaskHack = !;//InvalidMultiTask;
// 	default type ItemHack = !;
// }
// // struct InvalidMultiTask;

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
	); // as *const str
	// println!("{} {}", ::std::intrinsics::type_name::<T1>(), ::std::intrinsics::type_name::<T2>());
	let ret = ::std::mem::transmute_copy(&t1);
	::std::mem::forget(t1);
	ret
}

unsafe fn ignore_lifetime<T>(a: &T) -> &'static T {
	&*(a as *const T)
}

// pub trait FromDistributedIterator<T> {
// 	fn from_dist_iter<I>(dist_iter: I, pool: &::process_pool::ProcessPool) -> Self where I: IntoDistributedIterator<Item = T>, <<I as IntoDistributedIterator>::Iter as DistributedIterator>::Task: Serialize + DeserializeOwned + Send + Sync + 'static;
// }

// impl<T> FromDistributedIterator<T> for Vec<T> where T: Serialize + DeserializeOwned + Send + Sync + 'static {
// 	fn from_dist_iter<I>(dist_iter: I, pool: &::process_pool::ProcessPool) -> Self where I: IntoDistributedIterator<Item = T>, <<I as IntoDistributedIterator>::Iter as DistributedIterator>::Task: Serialize + DeserializeOwned + Send + Sync + 'static {
// 		dist_iter.into_dist_iter().reduce(
// 			pool,
// 			// FnMut!(|elems: ReduceA<_, _>| elems.collect::<B>()),
// 			CollectReducer(Vec::new()),
// 			CollectReducer2(Vec::new()),
// 			// |elems: ReduceB<Vec<T>>| elems.flat_map(|x| x.into_iter()).collect(),
// 		)
// 	}
// }

#[derive(Serialize, Deserialize)]
pub struct CollectReducer<T>(T);
impl<T> Clone for CollectReducer<Vec<T>> {
	fn clone(&self) -> Self {
		assert!(self.0.is_empty());
		CollectReducer(Vec::new())
	}
}
impl<T> ReduceA for CollectReducer<Vec<T>> {
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
pub struct CollectReducer2<T>(T);
impl<T> Clone for CollectReducer2<Vec<T>> {
	fn clone(&self) -> Self {
		assert!(self.0.is_empty());
		CollectReducer2(Vec::new())
	}
}
impl<T> ReduceA for CollectReducer2<Vec<T>> {
	type Item = Vec<T>;
	type Output = Vec<T>;
	fn push(&mut self, item: Self::Item) {
		self.0.extend(item)
	}
	fn ret(self) -> Self::Output {
		self.0
	}
}

pub trait ReduceA {
	type Item;
	type Output;
	fn push(&mut self, Self::Item);
	fn ret(self) -> Self::Output;
}
pub trait DistributedIteratorExecute<Source> {
	// type Source;
	type Item;
	// type Task: Consumer<Item = Self::Item>;
	type Task: ConsumerReducer<Source, Item = Self::Item>;
	// type Task: ConsumerReducerA<Source = Self::Source, Item = Self::Item>;
	// fn size_hint(&self, source: &Source) -> (usize, Option<usize>);
	// fn next_task(&mut self, &mut Source) -> Option<Self::Task>;
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

pub trait Reducer<I: DistributedIteratorExecute<Source>, Source, B> {
	type Mid: 'static;
	// type I: DistributedIteratorExecute<Source>;
	// type Source: DistributedIterator;
	type ReduceA: ReduceA<
		Item = <I as DistributedIteratorExecute<Source>>::Item,
		Output = Self::Mid,
	>; // FnMut(/*impl Iterator<Item = Self::Item>*/ ReduceA<Self::I::Task, Self::I::Item>) -> Self::Mid;
	type ReduceB: ReduceA<Item = Self::Mid, Output = B>; //FnMut(/*impl Iterator<Item = Self::Item>*/ ReduceB<Self::Mid>) -> B;
	fn reducers(self) -> (I, Self::ReduceA, Self::ReduceB);
	// fn run<I: DistributedIterator>(self, i: I, pool: &::process_pool::ProcessPool) -> Self::Item;
}
#[must_use]
pub struct Collect<I, B> {
	i: I,
	b: marker::PhantomData<fn() -> B>,
}
impl<I: DistributedIteratorExecute<Source>, A, Source> Reducer<I, Source, Vec<A>>
	for Collect<I, Vec<A>>
where
	A: 'static, // Send + Sync + Serialize + DeserializeOwned + 'static,
	// I::Task: Send + Sync + Serialize + DeserializeOwned + 'static,
	// A: iter::FromIterator<I::Item> + iter::IntoIterator<Item = I::Item>,
	CollectReducer<Vec<A>>:
		ReduceA<Item = <I as DistributedIteratorExecute<Source>>::Item, Output = Vec<A>>,
{
	type Mid = Vec<A>;
	// type I = I;
	// type Source = Source;
	type ReduceA = CollectReducer<Vec<A>>; // sc::FnMut<(), fn(&mut (), (ReduceA<I::Task, I::Item>,)) -> A>;
	type ReduceB = CollectReducer2<Vec<A>>; // sc::FnMut<(), fn(&mut (), (ReduceA<I::Task, I::Item>,)) -> A>;
										 // type ReduceB = sc::FnMut<(), fn(&mut (), (ReduceB<Vec<A>>,)) -> Vec<A>>;
	fn reducers(self) -> (I, Self::ReduceA, Self::ReduceB) {
		(
			self.i,
			CollectReducer(Vec::new()), // FnMut!(|elems: ReduceA<_, _>| elems.collect::<A>()),
			CollectReducer2(Vec::new()), // FnMut!(|elems: ReduceA<_, _>| elems.collect::<A>()),
			                            // FnMut!(|elems: ReduceB<Vec<A>>| elems.flat_map(|x| x.into_iter()).collect()), // TODO doesn't need to be sc
		)
	}
}

// #[must_use]
// pub struct Sum<I, B> {
// 	i: I,
// 	b: marker::PhantomData<fn() -> B>,
// }
// impl<I: DistributedIteratorExecute<Source>, A, Source: DistributedIterator> Reducer<I, A, A, Source>
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

// pub struct ExecuteA<Source>(marker::PhantomData<fn(Source)>);
// impl<Source> DistributedIteratorExecute<Source> for ExecuteA<Source> {
// 	// type Source = Source;
// 	// type Item = Source;
// 	type Task = BiConsumerA<Source>;
// 	// fn size_hint(&self, source: &Source) -> (usize, Option<usize>) {
// 	// 	source.size_hint()
// 	// }
// 	// fn next_task(&mut self, source: &mut Source) -> Option<Self::Task> {
// 	// 	let ret = source.next_task();
// 	// 	// let x: Option<&Self::Task> = ret.as_ref();
// 	// 	ret.map(|task|BiConsumerA{task})
// 	// }
// 	fn task(&self) -> Self::Task {
// 		BiConsumerA{marker: marker::PhantomData}
// 		// unimplemented!();
// 		// let ret = source.task();
// 		// let x: Option<&Self::Task> = ret.as_ref();
// 		// ret.map(|task|BiConsumerA{task})
// 	}
// }

// #[derive(Serialize,Deserialize)]
// pub struct BiConsumerA<T> {
// 	// task: T,
// 	marker: marker::PhantomData<fn(T)>
// }
// // impl<C: Consumer> Consumer for BiConsumerA<C> {
// // 	type Item = C::Item;
// // 	fn run(self, i: &mut impl FnMut(Self::Item)) {
// // 		let task = self.task;
// // 		task.run(&mut |item|{ /*println!("D");*/ /*f(&item);*/ i(item) })
// // 	}
// // }
// impl<Source> ConsumerReducer<Source> for BiConsumerA<Source> {
// 	// type Source = Source;
// 	type Item = Source;
// 	fn run(&mut self, source: Source, i: &mut impl FnMut(Self::Item)) {
// 		// let task = self.task;
// 		i(source);
// 		// task.run(source, &mut |item|{ /*println!("D");*/ /*f(&item);*/ i(item) })
// 	}
// }

// pub struct ExecuteB<'a,Source>(marker::PhantomData<fn(Source,&'a ())>);
// impl<'a,Source:'a> DistributedIteratorExecute<&'a Source> for ExecuteB<'a,Source> {
// 	// type Source = Source;
// 	// type Item = Source;
// 	type Task = BiConsumerB<'a,Source>;
// 	// fn size_hint(&self, source: &Source) -> (usize, Option<usize>) {
// 	// 	source.size_hint()
// 	// }
// 	// fn next_task(&mut self, source: &mut Source) -> Option<Self::Task> {
// 	// 	let ret = source.next_task();
// 	// 	// let x: Option<&Self::Task> = ret.as_ref();
// 	// 	ret.map(|task|BiConsumerB{task})
// 	// }
// 	fn task(&self) -> Self::Task {
// 		BiConsumerB{marker: marker::PhantomData}
// 		// unimplemented!();
// 		// let ret = source.task();
// 		// let x: Option<&Self::Task> = ret.as_ref();
// 		// ret.map(|task|BiConsumerB{task})
// 	}
// }

// #[derive(Serialize,Deserialize)]
// pub struct BiConsumerB<'a,T> {
// 	// task: T,
// 	marker: marker::PhantomData<fn(T,&'a ())>
// }
// // impl<C: Consumer> Consumer for BiConsumerB<C> {
// // 	type Item = C::Item;
// // 	fn run(self, i: &mut impl FnMut(Self::Item)) {
// // 		let task = self.task;
// // 		task.run(&mut |item|{ /*println!("D");*/ /*f(&item);*/ i(item) })
// // 	}
// // }
// impl<'a,Source:'a> ConsumerReducer<&'a Source> for BiConsumerB<'a,Source> {
// 	// type Source = &'a Source;
// 	type Item = &'a Source;
// 	fn run(&mut self, source: Source, i: &mut impl FnMut(Self::Item)) {
// 		// let task = self.task;
// 		i(source);
// 		// task.run(source, &mut |item|{ /*println!("D");*/ /*f(&item);*/ i(item) })
// 	}
// }

// pub struct DistributedIteratorRef<'a,I:DistributedIterator>(marker::PhantomData<fn(I::Item)>,&'a ()) where I::Item: 'a;// Option<&'a I::Item>) where I::Item: 'a;//,marker::PhantomData<fn(I)>);
// impl<'a,I: DistributedIterator> DistributedIterator for DistributedIteratorRef<'a,I> where I::Item: 'a {
// 	type Item = &'a I::Item;
// 	type Task = TaskRef<'a, I::Task>;
// 	fn size_hint(&self) -> (usize, Option<usize>) {
// 		unreachable!()
// 	}
// 	fn next_task(&mut self) -> Option<Self::Task> {
// 		unreachable!()
// 	}
// }
// pub struct TaskRef<'a,C>(marker::PhantomData<fn(C,&'a ())>);
// impl<'a,C: Consumer> Consumer for TaskRef<'a,C> where C::Item: 'a {
// 	type Item = &'a C::Item;
// 	fn run(self, i: &mut impl FnMut(Self::Item)) {
// 		unreachable!()
// 		// let (task,mut f) = (self.task,self.f);
// 		// task.run(&mut |item|{ /*println!("D");*/ f(&item); i(item) })
// 	}
// }

// pub struct ExecuteB<'a,Source>(marker::PhantomData<fn(Source,&'a ())>);
// impl<'a, Source: DistributedIterator> DistributedIteratorExecute<Source> for ExecuteB<'a,Source> where Source::Item: 'a {
// 	type Item = &'a Source::Item;
// 	type Task = BiConsumerB<'a, Source::Task>;
// 	// fn size_hint(&self, source: &Source) -> (usize, Option<usize>) {
// 	// 	source.size_hint()
// 	// }
// 	// fn next_task(&mut self, source: &mut Source) -> Option<Self::Task> {
// 	// 	let ret = source.next_task();
// 	// 	// let x: Option<&Self::Task> = ret.as_ref();
// 	// 	ret.map(|task|BiConsumerB{task})
// 	// }
// 	fn task(&self) -> Self::Task {
// 		BiConsumerB{marker: marker::PhantomData}
// 		// unimplemented!();
// 		// let ret = source.task();
// 		// let x: Option<&Self::Task> = ret.as_ref();
// 		// ret.map(|task|BiConsumerB{task})
// 	}
// }

// #[derive(Serialize,Deserialize)]
// pub struct BiConsumerB<'a,T> {
// 	// task: T,
// 	marker: marker::PhantomData<fn(T,&'a ())>
// }
// // impl<C: Consumer> Consumer for BiConsumerB<C> {
// // 	type Item = C::Item;
// // 	fn run(self, i: &mut impl FnMut(Self::Item)) {
// // 		let task = self.task;
// // 		task.run(&mut |item|{ /*println!("D");*/ /*f(&item);*/ i(item) })
// // 	}
// // }
// impl<'a, C: Consumer> ConsumerReducer for BiConsumerB<'a,C> where C::Item: 'a {
// 	type Source = &'a C::Item;
// 	type Item = &'a C::Item;
// 	fn run(&mut self, source: Self::Source, i: &mut impl FnMut(Self::Item)) {
// 		// let task = self.task;
// 		i(source);
// 		// task.run(source, &mut |item|{ /*println!("D");*/ /*f(&item);*/ i(item) })
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
			/*println!("D");*/
			f(&item);
			i(item)
		})
	}
}

// pub struct ReduceA<Task, Item>(
// 	iter::FlatMap<
// 		vec::IntoIter<Task>,
// 		vec::Vec<Item>,
// 		sc::FnMut<(), for<'r> fn(&'r mut (), (Task,)) -> vec::Vec<Item>>,
// 	>,
// );
// impl<Task, Item> Iterator for ReduceA<Task, Item> {
// 	type Item = Item;
// 	fn next(&mut self) -> Option<Self::Item> {
// 		self.0.next()
// 	}
// 	fn size_hint(&self) -> (usize, Option<usize>) {
// 		self.0.size_hint()
// 	}
// }
// pub struct ReduceB<A: 'static>(
// 	iter::Map<
// 		vec::IntoIter<::process_pool::JoinHandle<A>>,
// 		sc::FnMut<(), for<'r> fn(&'r mut (), (::process_pool::JoinHandle<A>,)) -> A>,
// 	>,
// );
// impl<A: 'static> Iterator for ReduceB<A> {
// 	type Item = A;
// 	fn next(&mut self) -> Option<Self::Item> {
// 		self.0.next()
// 	}
// 	fn size_hint(&self) -> (usize, Option<usize>) {
// 		self.0.size_hint()
// 	}
// }

pub struct IntoIter<T>(vec::IntoIter<T>);
impl<T> DistributedIterator for IntoIter<T> {
	type Item = T;
	type Task = IntoIterConsumer<T>;
	fn size_hint(&self) -> (usize, Option<usize>) {
		(self.0.len(), Some(self.0.len()))
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.0.next().map(IntoIterConsumer)
	}
}
#[derive(Serialize, Deserialize)]
pub struct IntoIterConsumer<T>(T);
impl<T> Consumer for IntoIterConsumer<T> {
	type Item = T;
	fn run(self, i: &mut impl FnMut(Self::Item)) {
		i(self.0)
	}
}

pub struct IterRef<'a, T: 'a>(slice::Iter<'a, T>);
impl<'a, T: Clone + 'a> DistributedIterator for IterRef<'a, T> {
	type Item = T;
	type Task = IterRefConsumer<T>;
	fn size_hint(&self) -> (usize, Option<usize>) {
		(self.0.len(), Some(self.0.len()))
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.0.next().cloned().map(IterRefConsumer)
	}
}
#[derive(Serialize, Deserialize)]
pub struct IterRefConsumer<T>(T);
impl<T> Consumer for IterRefConsumer<T> {
	type Item = T;
	fn run(self, i: &mut impl FnMut(Self::Item)) {
		i(self.0)
	}
}

pub struct Map<I, F> {
	i: I,
	f: F,
}
impl<I: DistributedIterator, F: FnMut(I::Item) -> R + Clone, R> DistributedIterator for Map<I, F> {
	type Item = R;
	type Task = MapConsumer<I::Task, F>; // sc::FnOnce<(I::Task, F), fn((I::Task, F), ()) -> Vec<R>>;
	fn size_hint(&self) -> (usize, Option<usize>) {
		self.i.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.i.next_task().map(|task| {
			let f = self.f.clone();
			MapConsumer { task, f }
			// unimplemented!()
			// FnOnce!([t,f] move || {
			// 	let t: I::Task = t;
			// 	t().into_iter().map(f).collect()
			// })
		})
	}
}
impl<
		I: DistributedIteratorExecute<Source>,
		// F: FnMut(I::Item) -> R + Clone,
		F,
		R,
		Source, //: DistributedIterator,
	> DistributedIteratorExecute<Source> for Map<I, F>
where
	F: FnMut(<I as DistributedIteratorExecute<Source>>::Item) -> R + Clone,
{
	type Item = F::Output;
	// type Source = I::Source;
	// type Item = R;
	type Task = MapConsumer<I::Task, F>; // sc::FnOnce<(I::Task, F), fn((I::Task, F), ()) -> Vec<R>>;
									  // fn size_hint(&self, source: &Source) -> (usize, Option<usize>) {
									  // 	self.i.size_hint(source)
									  // }
	fn task(&self) -> Self::Task {
		let task = self.i.task();
		let f = self.f.clone();
		MapConsumer { task, f }
		// unimplemented!()
		// FnOnce!([t,f] move || {
		// 	let t: I::Task = t;
		// 	t().into_iter().map(f).collect()
		// })
		// })
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
		task.run(&mut |item| {
			/*println!("D");*/
			i(f(item))
		})
	}
}
impl<
		C: ConsumerReducer<Source>,
		F: FnMut(<C as ConsumerReducer<Source>>::Item) -> R + Clone,
		R,
		Source,
	> ConsumerReducer<Source> for MapConsumer<C, F>
{
	// type Source = <C as ConsumerReducer>::Source;
	type Item = R;
	fn run(&self, source: Source, i: &mut impl FnMut(Self::Item)) {
		let (task, f) = (&self.task, &self.f);
		task.run(source, &mut |item| {
			/*println!("D");*/
 /*f(&item);*/
			i(f.clone()(item))
		})
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
			.map(|a| {
				// let a =
				ChainConsumer::A(a)
				// FnOnce!([a]move||{
				// 	let a: ChainConsumer<A::Task,B::Task> = a;
				// 	a.left().unwrap()()
				// })
			})
			.or_else(|| {
				self.b.next_task().map(|b| {
					// let b =
					ChainConsumer::B(b)
					// FnOnce!([b]move||{
					// 	let b: ChainConsumer<A::Task,B::Task> = b;
					// 	b.right().unwrap()()
					// })
				})
			})
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
	type Task = FlatMapConsumer<I::Task, F>; // sc::FnOnce<(I::Task, F), fn((I::Task, F), ()) -> Vec<R::Item>>;
	fn size_hint(&self) -> (usize, Option<usize>) {
		self.i.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.i.next_task().map(|task| {
			let f = self.f.clone();
			// unimplemented!()
			// FnOnce!([t,f]move||{let mut f = f;let t: I::Task = t; t().into_iter().flat_map(f).collect()})
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
		// println!("B");
		let (task, mut f) = (self.task, self.f);
		task.run(&mut |item| {
			for x in f(item) {
				/*println!("C"); */
				i(x)
			}
		})
	}
}
