use derive_new::new;
use futures::Stream;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use serde_closure::traits::FnMut;
use std::{
	pin::Pin, task::{Context, Poll}
};

use super::{ParallelPipe, ParallelStream, PipeTask, StreamTask};

#[pin_project]
#[derive(new)]
#[must_use]
pub struct FlatMap<P, F> {
	#[pin]
	pipe: P,
	f: F,
}

impl_par_dist! {
	impl<P: ParallelStream, F, R: Stream> ParallelStream for FlatMap<P, F>
	where
		F: FnMut<(P::Item,), Output = R> + Clone + Send,
	{
		type Item = R::Item;
		type Task = FlatMapTask<P::Task, F>;

		fn size_hint(&self) -> (usize, Option<usize>) {
			(0, None)
		}
		fn next_task(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Task>> {
			let self_ = self.project();
			let f = self_.f;
			self_.pipe.next_task(cx).map(|task| {
				task.map(|task| {
					let f = f.clone();
					FlatMapTask { task, f }
				})
			})
		}
	}

	impl<P: ParallelPipe<Input>, F, R: Stream, Input> ParallelPipe<Input> for FlatMap<P, F>
	where
		F: FnMut<(P::Output,), Output = R> + Clone + Send,
	{
		type Output = R::Item;
		type Task = FlatMapTask<P::Task, F>;

		fn task(&self) -> Self::Task {
			let task = self.pipe.task();
			let f = self.f.clone();
			FlatMapTask { task, f }
		}
	}
}

#[derive(Serialize, Deserialize)]
pub struct FlatMapTask<C, F> {
	task: C,
	f: F,
}
impl<C: StreamTask, F: FnMut<(C::Item,), Output = R> + Clone, R: Stream> StreamTask
	for FlatMapTask<C, F>
{
	type Item = R::Item;
	type Async = crate::pipe::FlatMap<C::Async, F, R>;

	fn into_async(self) -> Self::Async {
		crate::pipe::FlatMap::new(self.task.into_async(), self.f)
	}
}
impl<C: PipeTask<Input>, F: FnMut<(C::Output,), Output = R> + Clone, R: Stream, Input>
	PipeTask<Input> for FlatMapTask<C, F>
{
	type Output = R::Item;
	type Async = crate::pipe::FlatMap<C::Async, F, R>;

	fn into_async(self) -> Self::Async {
		crate::pipe::FlatMap::new(self.task.into_async(), self.f)
	}
}
