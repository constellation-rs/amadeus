use derive_new::new;
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_closure::traits::FnMut;

use super::{ParallelPipe, ParallelStream, PipeTask, StreamTask};

#[derive(new)]
#[must_use]
pub struct FlatMap<I, F> {
	i: I,
	f: F,
}

impl_par_dist! {
	impl<I: ParallelStream, F, R: Stream> ParallelStream for FlatMap<I, F>
	where
		F: FnMut<(I::Item,), Output = R> + Clone + Send + 'static,
	{
		type Item = R::Item;
		type Task = FlatMapTask<I::Task, F>;

		fn size_hint(&self) -> (usize, Option<usize>) {
			(0, None)
		}
		fn next_task(&mut self) -> Option<Self::Task> {
			self.i.next_task().map(|task| {
				let f = self.f.clone();
				FlatMapTask { task, f }
			})
		}
	}

	impl<I: ParallelPipe<Source>, F, R: Stream, Source> ParallelPipe<Source> for FlatMap<I, F>
	where
		F: FnMut<(I::Item,), Output = R> + Clone + Send + 'static,
	{
		type Item = R::Item;
		type Task = FlatMapTask<I::Task, F>;

		fn task(&self) -> Self::Task {
			let task = self.i.task();
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
impl<C: PipeTask<Source>, F: FnMut<(C::Item,), Output = R> + Clone, R: Stream, Source>
	PipeTask<Source> for FlatMapTask<C, F>
{
	type Item = R::Item;
	type Async = crate::pipe::FlatMap<C::Async, F, R>;

	fn into_async(self) -> Self::Async {
		crate::pipe::FlatMap::new(self.task.into_async(), self.f)
	}
}
