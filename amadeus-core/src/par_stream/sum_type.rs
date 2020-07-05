use futures::Stream;
use std::{
	pin::Pin, task::{Context, Poll}
};
use sum::Sum2;

use super::{ParallelPipe, ParallelStream, PipeTask, StreamTask};
use crate::pipe::Pipe;

impl_par_dist! {
	impl<A: ParallelStream, B: ParallelStream<Item = A::Item>> ParallelStream for Sum2<A, B> {
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

	impl<A: ParallelPipe<Source>, B: ParallelPipe<Source, Item = A::Item>, Source>
		ParallelPipe<Source> for Sum2<A, B>
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
}

impl<A: StreamTask, B: StreamTask<Item = A::Item>> StreamTask for Sum2<A, B> {
	type Item = A::Item;
	type Async = Sum2<A::Async, B::Async>;
	fn into_async(self) -> Self::Async {
		match self {
			Sum2::A(a) => Sum2::A(a.into_async()),
			Sum2::B(b) => Sum2::B(b.into_async()),
		}
	}
}
impl<A: PipeTask<Source>, B: PipeTask<Source, Item = A::Item>, Source> PipeTask<Source>
	for Sum2<A, B>
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

impl<A: Pipe<Source>, B: Pipe<Source, Item = A::Item>, Source> Pipe<Source> for Sum2<A, B> {
	type Item = A::Item;

	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
	) -> Poll<Option<Self::Item>> {
		match self.as_pin_mut() {
			Sum2::A(task) => task.poll_next(cx, stream),
			Sum2::B(task) => task.poll_next(cx, stream),
		}
	}
}
