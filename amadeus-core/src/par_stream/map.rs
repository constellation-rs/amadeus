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
pub struct Map<P, F> {
	#[pin]
	pipe: P,
	f: F,
}

impl_par_dist! {
	impl<P: ParallelStream, F, R> ParallelStream for Map<P, F>
	where
		F: FnMut<(P::Item,), Output = R> + Clone + Send,
	{
		type Item = R;
		type Task = MapTask<P::Task, F>;

		fn size_hint(&self) -> (usize, Option<usize>) {
			self.pipe.size_hint()
		}
		fn next_task(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Task>> {
			let self_ = self.project();
			let f = self_.f;
			self_.pipe.next_task(cx).map(|task| {
				task.map(|task| {
					let f = f.clone();
					MapTask { task, f }
				})
			})
		}
	}

	impl<P: ParallelPipe<Input>, F, R, Input> ParallelPipe<Input> for Map<P, F>
	where
		F: FnMut<(P::Output,), Output = R> + Clone + Send,
	{
		type Output = R;
		type Task = MapTask<P::Task, F>;

		fn task(&self) -> Self::Task {
			let task = self.pipe.task();
			let f = self.f.clone();
			MapTask { task, f }
		}
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct MapTask<C, F> {
	#[pin]
	task: C,
	f: F,
}

impl<C: StreamTask, F, R> StreamTask for MapTask<C, F>
where
	F: FnMut<(C::Item,), Output = R> + Clone,
{
	type Item = R;
	type Async = crate::pipe::Map<C::Async, F>;

	fn into_async(self) -> Self::Async {
		crate::pipe::Map::new(self.task.into_async(), self.f)
	}
}
impl<C: PipeTask<Input>, F, R, Input> PipeTask<Input> for MapTask<C, F>
where
	F: FnMut<(C::Output,), Output = R> + Clone,
{
	type Output = R;
	type Async = crate::pipe::Map<C::Async, F>;

	fn into_async(self) -> Self::Async {
		crate::pipe::Map::new(self.task.into_async(), self.f)
	}
}
