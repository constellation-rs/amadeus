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
pub struct Filter<P, F> {
	#[pin]
	pipe: P,
	f: F,
}

impl_par_dist! {
	impl<P: ParallelStream, F> ParallelStream for Filter<P, F>
	where
		F: for<'a> FnMut<(&'a P::Item,), Output = bool> + Clone + Send,
	{
		type Item = P::Item;
		type Task = FilterTask<P::Task, F>;

		fn size_hint(&self) -> (usize, Option<usize>) {
			(0, self.pipe.size_hint().1)
		}
		fn next_task(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Task>> {
			let self_ = self.project();
			let f = self_.f;
			self_.pipe.next_task(cx).map(|task| {
				task.map(|task| {
					let f = f.clone();
					FilterTask { task, f }
				})
			})
		}
	}

	impl<P: ParallelPipe<Input>, F, Input> ParallelPipe<Input> for Filter<P, F>
	where
		F: for<'a> FnMut<(&'a P::Output,), Output = bool> + Clone + Send,
	{
		type Output = P::Output;
		type Task = FilterTask<P::Task, F>;

		fn task(&self) -> Self::Task {
			let task = self.pipe.task();
			let f = self.f.clone();
			FilterTask { task, f }
		}
	}
}

#[derive(Serialize, Deserialize)]
pub struct FilterTask<C, F> {
	task: C,
	f: F,
}

impl<C: StreamTask, F> StreamTask for FilterTask<C, F>
where
	F: for<'a> FnMut<(&'a C::Item,), Output = bool>,
{
	type Item = C::Item;
	type Async = crate::pipe::Filter<C::Async, F>;

	fn into_async(self) -> Self::Async {
		crate::pipe::Filter::new(self.task.into_async(), self.f)
	}
}
impl<C: PipeTask<Input>, F, Input> PipeTask<Input> for FilterTask<C, F>
where
	F: for<'a> FnMut<(&'a C::Output,), Output = bool>,
{
	type Output = C::Output;
	type Async = crate::pipe::Filter<C::Async, F>;

	fn into_async(self) -> Self::Async {
		crate::pipe::Filter::new(self.task.into_async(), self.f)
	}
}
