use derive_new::new;
use futures::Stream;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	pin::Pin, task::{Context, Poll}
};

use super::{ParallelPipe, ParallelStream, PipeTask, StreamTask};
use crate::pipe::Pipe;

#[derive(new)]
#[must_use]
pub struct Map<I, F> {
	i: I,
	f: F,
}

impl_par_dist! {
	impl<I: ParallelStream, F, R> ParallelStream for Map<I, F>
	where
		F: FnMut(I::Item) -> R + Clone + Send + 'static,
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
		F: FnMut(I::Item) -> R + Clone + Send + 'static,
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
	F: FnMut(C::Item) -> R + Clone,
{
	type Item = R;
	type Async = MapTask<C::Async, F>;
	fn into_async(self) -> Self::Async {
		MapTask {
			task: self.task.into_async(),
			f: self.f,
		}
	}
}
impl<C: PipeTask<Source>, F, R, Source> PipeTask<Source> for MapTask<C, F>
where
	F: FnMut(C::Item) -> R + Clone,
{
	type Item = R;
	type Async = MapTask<C::Async, F>;
	fn into_async(self) -> Self::Async {
		MapTask {
			task: self.task.into_async(),
			f: self.f,
		}
	}
}

impl<C: Stream, F, R> Stream for MapTask<C, F>
where
	F: FnMut(C::Item) -> R + Clone,
{
	type Item = R;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		task.poll_next(cx).map(|t| t.map(f))
	}
}

impl<C: Pipe<Source>, F, R, Source> Pipe<Source> for MapTask<C, F>
where
	F: FnMut(C::Item) -> R + Clone,
{
	type Item = R;

	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
	) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		task.poll_next(cx, stream).map(|t| t.map(f))
	}
}
