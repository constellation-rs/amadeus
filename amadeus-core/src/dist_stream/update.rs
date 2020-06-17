use derive_new::new;
use futures::{pin_mut, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	pin::Pin, task::{Context, Poll}
};

use super::{ParallelPipe, ParallelStream, PipeTask, PipeTaskAsync, StreamTask, StreamTaskAsync};
use crate::sink::{Sink, SinkMap};

#[derive(new)]
#[must_use]
pub struct Update<I, F> {
	i: I,
	f: F,
}

impl_par_dist! {
	impl<I: ParallelStream, F> ParallelStream for Update<I, F>
	where
		F: FnMut(&mut I::Item) + Clone + Send + 'static,
	{
		type Item = I::Item;
		type Task = UpdateTask<I::Task, F>;

		fn size_hint(&self) -> (usize, Option<usize>) {
			self.i.size_hint()
		}
		fn next_task(&mut self) -> Option<Self::Task> {
			self.i.next_task().map(|task| {
				let f = self.f.clone();
				UpdateTask { task, f }
			})
		}
	}

	impl<I: ParallelPipe<Source>, F, Source> ParallelPipe<Source> for Update<I, F>
	where
		F: FnMut(&mut <I as ParallelPipe<Source>>::Item) + Clone + Send + 'static,
	{
		type Item = I::Item;
		type Task = UpdateTask<I::Task, F>;

		fn task(&self) -> Self::Task {
			let task = self.i.task();
			let f = self.f.clone();
			UpdateTask { task, f }
		}
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct UpdateTask<T, F> {
	#[pin]
	task: T,
	f: F,
}

impl<C: StreamTask, F> StreamTask for UpdateTask<C, F>
where
	F: FnMut(&mut C::Item) + Clone,
{
	type Item = C::Item;
	type Async = UpdateTask<C::Async, F>;
	fn into_async(self) -> Self::Async {
		UpdateTask {
			task: self.task.into_async(),
			f: self.f,
		}
	}
}
impl<C: PipeTask<Source>, F, Source> PipeTask<Source> for UpdateTask<C, F>
where
	F: FnMut(&mut <C as PipeTask<Source>>::Item) + Clone,
{
	type Item = C::Item;
	type Async = UpdateTask<C::Async, F>;
	fn into_async(self) -> Self::Async {
		UpdateTask {
			task: self.task.into_async(),
			f: self.f,
		}
	}
}

impl<C: StreamTaskAsync, F> StreamTaskAsync for UpdateTask<C, F>
where
	F: FnMut(&mut C::Item) + Clone,
{
	type Item = C::Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, sink: Pin<&mut impl Sink<Item = Self::Item>>,
	) -> Poll<()> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		let sink = SinkMap::new(sink, |mut item| {
			f(&mut item);
			item
		});
		pin_mut!(sink);
		task.poll_run(cx, sink)
	}
}

impl<C: PipeTaskAsync<Source>, F, Source> PipeTaskAsync<Source> for UpdateTask<C, F>
where
	F: FnMut(&mut <C as PipeTaskAsync<Source>>::Item) + Clone,
{
	type Item = C::Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
		sink: Pin<&mut impl Sink<Item = Self::Item>>,
	) -> Poll<()> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		let sink = SinkMap::new(sink, |mut item| {
			f(&mut item);
			item
		});
		pin_mut!(sink);
		task.poll_run(cx, stream, sink)
	}
}
