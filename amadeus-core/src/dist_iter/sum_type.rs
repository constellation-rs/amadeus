use futures::Stream;
use std::{
	pin::Pin, task::{Context, Poll}
};
use sum::Sum2;

use super::{
	Consumer, ConsumerAsync, ConsumerMulti, ConsumerMultiAsync, DistributedIterator, DistributedIteratorMulti
};
use crate::sink::Sink;

impl<A: DistributedIterator, B: DistributedIterator<Item = A::Item>> DistributedIterator
	for Sum2<A, B>
{
	type Item = A::Item;
	type Task = Sum2<A::Task, B::Task>;

	fn size_hint(&self) -> (usize, Option<usize>) {
		match self {
			Self::A(i) => i.size_hint(),
			Self::B(i) => i.size_hint(),
		}
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		match self {
			Self::A(i) => i.next_task().map(Sum2::A),
			Self::B(i) => i.next_task().map(Sum2::B),
		}
	}
}

impl<
		A: DistributedIteratorMulti<Source>,
		B: DistributedIteratorMulti<Source, Item = A::Item>,
		Source,
	> DistributedIteratorMulti<Source> for Sum2<A, B>
{
	type Item = A::Item;
	type Task = Sum2<A::Task, B::Task>;

	fn task(&self) -> Self::Task {
		match self {
			Self::A(i) => Sum2::A(i.task()),
			Self::B(i) => Sum2::B(i.task()),
		}
	}
}

impl<A: Consumer, B: Consumer<Item = A::Item>> Consumer for Sum2<A, B> {
	type Item = A::Item;
	type Async = Sum2<A::Async, B::Async>;
	fn into_async(self) -> Self::Async {
		match self {
			Sum2::A(a) => Sum2::A(a.into_async()),
			Sum2::B(b) => Sum2::B(b.into_async()),
		}
	}
}
impl<A: ConsumerMulti<Source>, B: ConsumerMulti<Source, Item = A::Item>, Source>
	ConsumerMulti<Source> for Sum2<A, B>
{
	type Item = A::Item;
	type Async = Sum2<A::Async, B::Async>;
	fn into_async(self) -> Self::Async {
		match self {
			Sum2::A(a) => Sum2::A(a.into_async()),
			Sum2::B(b) => Sum2::B(b.into_async()),
		}
	}
}

impl<A: ConsumerAsync, B: ConsumerAsync<Item = A::Item>> ConsumerAsync for Sum2<A, B> {
	type Item = A::Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()> {
		match self.as_pin_mut() {
			Sum2::A(task) => task.poll_run(cx, sink),
			Sum2::B(task) => task.poll_run(cx, sink),
		}
	}
}

impl<A: ConsumerMultiAsync<Source>, B: ConsumerMultiAsync<Source, Item = A::Item>, Source>
	ConsumerMultiAsync<Source> for Sum2<A, B>
{
	type Item = A::Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
		sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()> {
		match self.as_pin_mut() {
			Sum2::A(task) => task.poll_run(cx, stream, sink),
			Sum2::B(task) => task.poll_run(cx, stream, sink),
		}
	}
}
