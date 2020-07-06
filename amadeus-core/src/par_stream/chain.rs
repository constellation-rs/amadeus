use derive_new::new;
use futures::Stream;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	pin::Pin, task::{Context, Poll}
};

use super::{ParallelStream, StreamTask};

#[derive(new)]
#[must_use]
pub struct Chain<A, B> {
	a: A,
	b: B,
}

impl_par_dist! {
	impl<A: ParallelStream, B: ParallelStream<Item = A::Item>> ParallelStream for Chain<A, B> {
		type Item = A::Item;
		type Task = ChainTask<A::Task, B::Task>;

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
				.map(ChainTask::A)
				.or_else(|| self.b.next_task().map(ChainTask::B))
		}
	}
}

#[pin_project(project = ChainTaskProj)]
#[derive(Serialize, Deserialize)]
pub enum ChainTask<A, B> {
	A(#[pin] A),
	B(#[pin] B),
}
impl<A: StreamTask, B: StreamTask<Item = A::Item>> StreamTask for ChainTask<A, B> {
	type Item = A::Item;
	type Async = ChainTask<A::Async, B::Async>;
	fn into_async(self) -> Self::Async {
		match self {
			ChainTask::A(a) => ChainTask::A(a.into_async()),
			ChainTask::B(b) => ChainTask::B(b.into_async()),
		}
	}
}
impl<A: Stream, B: Stream<Item = A::Item>> Stream for ChainTask<A, B> {
	type Item = A::Item;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		match self.project() {
			ChainTaskProj::A(a) => a.poll_next(cx),
			ChainTaskProj::B(b) => b.poll_next(cx),
		}
	}
}
