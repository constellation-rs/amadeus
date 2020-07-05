use derive_new::new;
use serde::{Deserialize, Serialize};

use super::{ParallelPipe, ParallelStream, PipeTask, StreamTask};
use crate::pipe::{Pipe, StreamExt};

#[derive(new)]
#[must_use]
pub struct Filter<I, F> {
	i: I,
	f: F,
}

impl_par_dist! {
	impl<I: ParallelStream, F> ParallelStream for Filter<I, F>
	where
		F: FnMut(&I::Item) -> bool + Clone + Send + 'static,
	{
		type Item = I::Item;
		type Task = FilterTask<I::Task, F>;

		fn size_hint(&self) -> (usize, Option<usize>) {
			(0, self.i.size_hint().1)
		}
		fn next_task(&mut self) -> Option<Self::Task> {
			self.i.next_task().map(|task| {
				let f = self.f.clone();
				FilterTask { task, f }
			})
		}
	}

	impl<I: ParallelPipe<Source>, F, Source> ParallelPipe<Source> for Filter<I, F>
	where
		F: FnMut(&I::Item) -> bool + Clone + Send + 'static,
	{
		type Item = I::Item;
		type Task = FilterTask<I::Task, F>;

		fn task(&self) -> Self::Task {
			let task = self.i.task();
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
	F: FnMut(&C::Item) -> bool,
{
	type Item = C::Item;
	type Async = crate::pipe::Filter<C::Async, F>;

	fn into_async(self) -> Self::Async {
		self.task.into_async().filter(self.f)
	}
}
impl<C: PipeTask<Source>, F, Source> PipeTask<Source> for FilterTask<C, F>
where
	F: FnMut(&C::Item) -> bool,
{
	type Item = C::Item;
	type Async = crate::pipe::Filter<C::Async, F>;

	fn into_async(self) -> Self::Async {
		self.task.into_async().filter(self.f)
	}
}
