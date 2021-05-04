// TODO: remove the allocation

use multimap::MultiMap;
use pin_project::pin_project;
use serde_closure::FnMutNamed;
use std::{
	hash::Hash, pin::Pin, task::{Context, Poll}, vec
};

use super::{FilterMapSync, MapSync, ParallelPipe, ParallelStream};

#[pin_project]
#[must_use]
pub struct LeftJoin<P, K, V1, V2> {
	#[pin]
	right: MapSync<P, LeftJoinClosure<K, V1, V2>>,
}

impl<P, K, V1, V2> LeftJoin<P, K, V1, V2> {
	pub fn new(pipe: P, right: MultiMap<K, V2>) -> Self {
		Self {
			right: MapSync::new(pipe, LeftJoinClosure::new(right)),
		}
	}
}

impl_par_dist! {
	impl<P, K, V1, V2> ParallelStream for LeftJoin<P, K, V1, V2>
	where
		P: ParallelStream<Item = (K, V1)>,
		K: Eq + Hash + Clone + Send,
		V2: Clone + Send,
	{
		type Item = (K, V1, ImplIter<V2>);
		type Task = <MapSync<P, LeftJoinClosure<K, V1, V2>> as ParallelStream>::Task;

		fn size_hint(&self) -> (usize, Option<usize>) {
			self.right.size_hint()
		}
		fn next_task(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Task>> {
			self.project().right.next_task(cx)
		}
	}

	impl<P, K, V1, V2, Input> ParallelPipe<Input> for LeftJoin<P, K, V1, V2>
	where
		P: ParallelPipe<Input, Output = (K, V1)>,
		K: Eq + Hash + Clone + Send,
		V2: Clone + Send,
	{
		type Output = (K, V1, ImplIter<V2>);
		type Task = <MapSync<P, LeftJoinClosure<K, V1, V2>> as ParallelPipe<Input>>::Task;

		fn task(&self) -> Self::Task {
			self.right.task()
		}
	}
}

FnMutNamed! {
	pub type LeftJoinClosure<K, V1, V2> = |self, right: MultiMap<K, V2>|item=> (K, V1)| -> (K, V1, ImplIter<V2>) where ; where K: Eq, K: Hash, V2: Clone {
		let v2 = self.right.get_vec(&item.0).map_or_else(Vec::new, Clone::clone).into_iter();
		(item.0, item.1, ImplIter(v2))
	}
}

#[pin_project]
#[must_use]
pub struct InnerJoin<P, K, V1, V2> {
	#[pin]
	right: FilterMapSync<P, InnerJoinClosure<K, V1, V2>>,
}

impl<P, K, V1, V2> InnerJoin<P, K, V1, V2> {
	pub fn new(pipe: P, right: MultiMap<K, V2>) -> Self {
		Self {
			right: FilterMapSync::new(pipe, InnerJoinClosure::new(right)),
		}
	}
}

impl_par_dist! {
	impl<P, K, V1, V2> ParallelStream for InnerJoin<P, K, V1, V2>
	where
		P: ParallelStream<Item = (K, V1)>,
		K: Eq + Hash + Clone + Send,
		V2: Clone + Send,
	{
		type Item = (K, ImplIter<V1>, ImplIter<V2>);
		type Task = <FilterMapSync<P, InnerJoinClosure<K, V1, V2>> as ParallelStream>::Task;

		fn size_hint(&self) -> (usize, Option<usize>) {
			self.right.size_hint()
		}
		fn next_task(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Task>> {
			self.project().right.next_task(cx)
		}
	}

	impl<P, K, V1, V2, Input> ParallelPipe<Input> for InnerJoin<P, K, V1, V2>
	where
		P: ParallelPipe<Input, Output = (K, V1)>,
		K: Eq + Hash + Clone + Send,

		V2: Clone + Send,
	{
		type Output = (K, ImplIter<V1>, ImplIter<V2>);
		type Task = <FilterMapSync<P, InnerJoinClosure<K, V1, V2>> as ParallelPipe<Input>>::Task;

		fn task(&self) -> Self::Task {
			self.right.task()
		}
	}
}

FnMutNamed! {
	pub type InnerJoinClosure<K, V1, V2> = |self, right: MultiMap<K, V2>|item=> (K, V1)| -> Option<(K, ImplIter<V1>, ImplIter<V2>)> where ; where K: Eq, K: Hash, V2: Clone {
		self.right.get_vec(&item.0).map(|v2| {
			(item.0, ImplIter(vec![item.1].into_iter()), ImplIter(v2.clone().into_iter()))
		})
	}
}

pub struct ImplIter<T>(vec::IntoIter<T>);
impl<T> Iterator for ImplIter<T> {
	type Item = T;

	fn next(&mut self) -> Option<Self::Item> {
		self.0.next()
	}
}
