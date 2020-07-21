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
		fn next_task(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Task>> {
			match self.as_pin_mut() {
				Sum2::A(i) => i.next_task(cx).map(|task| task.map(Sum2::A)),
				Sum2::B(i) => i.next_task(cx).map(|task| task.map(Sum2::B)),
			}
		}
	}

	impl<A: ParallelPipe<Input>, B: ParallelPipe<Input, Output = A::Output>, Input>
		ParallelPipe<Input> for Sum2<A, B>
	{
		type Output = A::Output;
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
impl<A: PipeTask<Input>, B: PipeTask<Input, Output = A::Output>, Input> PipeTask<Input>
	for Sum2<A, B>
{
	type Output = A::Output;
	type Async = Sum2<A::Async, B::Async>;

	fn into_async(self) -> Self::Async {
		match self {
			Sum2::A(a) => Sum2::A(a.into_async()),
			Sum2::B(b) => Sum2::B(b.into_async()),
		}
	}
}

impl<A: Pipe<Input>, B: Pipe<Input, Output = A::Output>, Input> Pipe<Input> for Sum2<A, B> {
	type Output = A::Output;

	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Input>>,
	) -> Poll<Option<Self::Output>> {
		match self.as_pin_mut() {
			Sum2::A(task) => task.poll_next(cx, stream),
			Sum2::B(task) => task.poll_next(cx, stream),
		}
	}
}
