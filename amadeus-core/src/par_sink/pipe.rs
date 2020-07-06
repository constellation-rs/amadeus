use derive_new::new;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};

use super::{DistributedPipe, DistributedSink, ParallelPipe, ParallelSink, PipeTask};
use crate::{
	par_stream::{ParallelStream, StreamTask}, pipe::{Pipe as _, PipePipe, StreamExt, StreamPipe}
};

#[derive(new)]
#[must_use]
pub struct Pipe<A, B> {
	a: A,
	b: B,
}

impl_par_dist! {
	impl<A: ParallelStream, B: ParallelPipe<A::Item>> ParallelStream for Pipe<A, B> {
		type Item = B::Item;
		type Task = JoinTask<A::Task, B::Task>;

		fn next_task(&mut self) -> Option<Self::Task> {
			self.a.next_task().map(|a| {
				let b = self.b.task();
				JoinTask { a, b }
			})
		}
		fn size_hint(&self) -> (usize, Option<usize>) {
			self.a.size_hint()
		}
	}
	impl<A: ParallelPipe<Source>, B: ParallelPipe<A::Item>, Source> ParallelPipe<Source> for Pipe<A, B> {
		type Item = B::Item;
		type Task = JoinTask<A::Task, B::Task>;

		fn task(&self) -> Self::Task {
			let a = self.a.task();
			let b = self.b.task();
			JoinTask { a, b }
		}
	}
}

impl<A: ParallelPipe<Source>, B: ParallelSink<A::Item>, Source> ParallelSink<Source>
	for Pipe<A, B>
{
	type Output = B::Output;
	type Pipe = Pipe<A, B::Pipe>;
	type ReduceA = B::ReduceA;
	type ReduceC = B::ReduceC;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceC) {
		let (a, b, c) = self.b.reducers();
		(Pipe::new(self.a, a), b, c)
	}
}
impl<A: DistributedPipe<Source>, B: DistributedSink<A::Item>, Source> DistributedSink<Source>
	for Pipe<A, B>
{
	type Output = B::Output;
	type Pipe = Pipe<A, B::Pipe>;
	type ReduceA = B::ReduceA;
	type ReduceB = B::ReduceB;
	type ReduceC = B::ReduceC;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		let (a, b, c, d) = self.b.reducers();
		(Pipe::new(self.a, a), b, c, d)
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct JoinTask<A, B> {
	#[pin]
	a: A,
	#[pin]
	b: B,
}

impl<A: StreamTask, B: PipeTask<A::Item>> StreamTask for JoinTask<A, B> {
	type Item = B::Item;
	type Async = StreamPipe<A::Async, B::Async>;

	fn into_async(self) -> Self::Async {
		self.a.into_async().pipe(self.b.into_async())
	}
}

impl<A: PipeTask<Source>, B: PipeTask<A::Item>, Source> PipeTask<Source> for JoinTask<A, B> {
	type Item = B::Item;
	type Async = PipePipe<A::Async, B::Async>;

	fn into_async(self) -> Self::Async {
		self.a.into_async().pipe(self.b.into_async())
	}
}
