use derive_new::new;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use serde_closure::traits::FnMut;

use super::{ParallelPipe, ParallelStream, PipeTask, StreamTask};

#[derive(new)]
#[must_use]
pub struct Map<I, F> {
	i: I,
	f: F,
}

impl_par_dist! {
	impl<I: ParallelStream, F, R> ParallelStream for Map<I, F>
	where
		F: FnMut<(I::Item,), Output = R> + Clone + Send + 'static,
	{
		type Item = R;
		type Task = MapTask<I::Task, F>;

		fn size_hint(&self) -> (usize, Option<usize>) {
			self.i.size_hint()
		}
		fn next_task(&mut self) -> Option<Self::Task> {
			self.i.next_task().map(|task| {
				let f = self.f.clone();
				MapTask { task, f }
			})
		}
	}

	impl<I: ParallelPipe<Source>, F, R, Source> ParallelPipe<Source> for Map<I, F>
	where
		F: FnMut<(I::Item,), Output = R> + Clone + Send + 'static,
	{
		type Item = R;
		type Task = MapTask<I::Task, F>;

		fn task(&self) -> Self::Task {
			let task = self.i.task();
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
	type Async = crate::pipe::Map<C::Async, F, R>;

	fn into_async(self) -> Self::Async {
		crate::pipe::Map::new(self.task.into_async(), self.f)
	}
}
impl<C: PipeTask<Source>, F, R, Source> PipeTask<Source> for MapTask<C, F>
where
	F: FnMut<(C::Item,), Output = R> + Clone,
{
	type Item = R;
	type Async = crate::pipe::Map<C::Async, F, R>;

	fn into_async(self) -> Self::Async {
		crate::pipe::Map::new(self.task.into_async(), self.f)
	}
}
