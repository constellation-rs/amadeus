use pin_project::{pin_project, project};
use serde::{Deserialize, Serialize};
use std::{
	pin::Pin, task::{Context, Poll}
};

use super::{Consumer, ConsumerAsync, DistributedIterator};
use crate::sink::Sink;

#[must_use]
pub struct Chain<A, B> {
	a: A,
	b: B,
}
impl<A, B> Chain<A, B> {
	pub(super) fn new(a: A, b: B) -> Self {
		Self { a, b }
	}
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

#[pin_project]
#[derive(Serialize, Deserialize)]
pub enum ChainConsumer<A, B> {
	A(#[pin] A),
	B(#[pin] B),
}
impl<A: Consumer, B: Consumer<Item = A::Item>> Consumer for ChainConsumer<A, B> {
	type Item = A::Item;
	type Async = ChainConsumer<A::Async, B::Async>;
	fn into_async(self) -> Self::Async {
		match self {
			ChainConsumer::A(a) => ChainConsumer::A(a.into_async()),
			ChainConsumer::B(b) => ChainConsumer::B(b.into_async()),
		}
	}
}
impl<A: ConsumerAsync, B: ConsumerAsync<Item = A::Item>> ConsumerAsync for ChainConsumer<A, B> {
	type Item = A::Item;

	#[project]
	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()> {
		#[project]
		match self.project() {
			ChainConsumer::A(a) => a.poll_run(cx, sink),
			ChainConsumer::B(b) => b.poll_run(cx, sink),
		}
	}
}
