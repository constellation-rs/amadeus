use derive_new::new;
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
pub struct FlatMapSync<P, F> {
	#[pin]
	pipe: P,
	f: F,
}

impl_par_dist! {
	impl<P: ParallelStream, F, R: Iterator> ParallelStream for FlatMapSync<P, F>
	where
		F: FnMut<(P::Item,), Output = R> + Clone + Send + 'static,
	{
		type Item = R::Item;
		type Task = FlatMapSyncTask<P::Task, F>;

		fn size_hint(&self) -> (usize, Option<usize>) {
			(0, None)
		}
		fn next_task(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Task>> {
			let self_ = self.project();
			let f = self_.f;
			self_.pipe.next_task(cx).map(|task| {
				task.map(|task| {
					let f = f.clone();
					FlatMapSyncTask { task, f }
				})
			})
		}
	}

	impl<P: ParallelPipe<Input>, F, R: Iterator, Input> ParallelPipe<Input> for FlatMapSync<P, F>
	where
		F: FnMut<(P::Output,), Output = R> + Clone + Send + 'static,
	{
		type Output = R::Item;
		type Task = FlatMapSyncTask<P::Task, F>;

		fn task(&self) -> Self::Task {
			let task = self.pipe.task();
			let f = self.f.clone();
			FlatMapSyncTask { task, f }
		}
	}
}

#[derive(Serialize, Deserialize)]
pub struct FlatMapSyncTask<C, F> {
	task: C,
	f: F,
}
impl<C: StreamTask, F: FnMut<(C::Item,), Output = R> + Clone, R: Iterator> StreamTask
	for FlatMapSyncTask<C, F>
{
	type Item = R::Item;
	type Async = crate::pipe::FlatMapSync<C::Async, F, R>;

	fn into_async(self) -> Self::Async {
		crate::pipe::FlatMapSync::new(self.task.into_async(), self.f)
	}
}
impl<C: PipeTask<Input>, F: FnMut<(C::Output,), Output = R> + Clone, R: Iterator, Input>
	PipeTask<Input> for FlatMapSyncTask<C, F>
{
	type Output = R::Item;
	type Async = crate::pipe::FlatMapSync<C::Async, F, R>;

	fn into_async(self) -> Self::Async {
		crate::pipe::FlatMapSync::new(self.task.into_async(), self.f)
	}
}
