use derive_new::new;
use futures::{pin_mut, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	pin::Pin, task::{Context, Poll}
};

use super::{ParallelPipe, ParallelStream, PipeTask, PipeTaskAsync, StreamTask, StreamTaskAsync};
use crate::sink::{Sink, SinkFlatMap};

#[derive(new)]
#[must_use]
pub struct FlatMap<I, F> {
	i: I,
	f: F,
}

impl_par_dist! {
	impl<I: ParallelStream, F, R: Stream> ParallelStream for FlatMap<I, F>
	where
		F: FnMut(I::Item) -> R + Clone + Send + 'static,
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
		F: FnMut(<I as ParallelPipe<Source>>::Item) -> R + Clone + Send + 'static,
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
impl<C: StreamTask, F: FnMut(C::Item) -> R + Clone, R: Stream> StreamTask for FlatMapTask<C, F> {
	type Item = R::Item;
	type Async = FlatMapStreamTaskAsync<C::Async, F, R>;
	fn into_async(self) -> Self::Async {
		FlatMapStreamTaskAsync {
			task: self.task.into_async(),
			f: self.f,
			fut: None,
		}
	}
}
impl<C: PipeTask<Source>, F: FnMut(C::Item) -> R + Clone, R: Stream, Source> PipeTask<Source>
	for FlatMapTask<C, F>
{
	type Item = R::Item;
	type Async = FlatMapStreamTaskAsync<C::Async, F, R>;
	fn into_async(self) -> Self::Async {
		FlatMapStreamTaskAsync {
			task: self.task.into_async(),
			f: self.f,
			fut: None,
		}
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct FlatMapStreamTaskAsync<C, F, Fut> {
	#[pin]
	task: C,
	f: F,
	#[pin]
	fut: Option<Fut>,
}

impl<C: StreamTaskAsync, F: FnMut(C::Item) -> R + Clone, R: Stream> StreamTaskAsync
	for FlatMapStreamTaskAsync<C, F, R>
{
	type Item = R::Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, sink: Pin<&mut impl Sink<Item = Self::Item>>,
	) -> Poll<()> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		let sink = SinkFlatMap::new(self_.fut, sink, |item| f(item));
		pin_mut!(sink);
		task.poll_run(cx, sink)
	}
}

impl<C: PipeTaskAsync<Source>, F, R: Stream, Source> PipeTaskAsync<Source>
	for FlatMapStreamTaskAsync<C, F, R>
where
	F: FnMut(<C as PipeTaskAsync<Source>>::Item) -> R + Clone,
{
	type Item = R::Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
		sink: Pin<&mut impl Sink<Item = Self::Item>>,
	) -> Poll<()> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		let sink = SinkFlatMap::new(self_.fut, sink, |item| f(item));
		pin_mut!(sink);
		task.poll_run(cx, stream, sink)
	}
}
