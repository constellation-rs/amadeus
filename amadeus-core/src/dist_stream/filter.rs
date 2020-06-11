use futures::{pin_mut, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	future::Future, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedPipe, DistributedStream, PipeTask, PipeTaskAsync, StreamTask, StreamTaskAsync
};
use crate::{
	pool::ProcessSend, sink::{Sink, SinkFilter, SinkFilterState}
};

#[must_use]
pub struct Filter<I, F> {
	i: I,
	f: F,
}
impl<I, F> Filter<I, F> {
	pub(crate) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedStream, F, Fut> DistributedStream for Filter<I, F>
where
	F: FnMut(&I::Item) -> Fut + Clone + ProcessSend,
	Fut: Future<Output = bool>,
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

impl<I: DistributedPipe<Source>, F, Fut, Source> DistributedPipe<Source> for Filter<I, F>
where
	F: FnMut(&<I as DistributedPipe<Source>>::Item) -> Fut + Clone + ProcessSend,
	Fut: Future<Output = bool>,
{
	type Item = I::Item;
	type Task = FilterTask<I::Task, F>;

	fn task(&self) -> Self::Task {
		let task = self.i.task();
		let f = self.f.clone();
		FilterTask { task, f }
	}
}

#[derive(Serialize, Deserialize)]
pub struct FilterTask<C, F> {
	task: C,
	f: F,
}

impl<C: StreamTask, F, Fut> StreamTask for FilterTask<C, F>
where
	F: FnMut(&C::Item) -> Fut + Clone,
	Fut: Future<Output = bool>,
{
	type Item = C::Item;
	type Async = FilterStreamTaskAsync<C::Async, F, Self::Item, Fut>;
	fn into_async(self) -> Self::Async {
		FilterStreamTaskAsync {
			task: self.task.into_async(),
			f: self.f,
			state: SinkFilterState::None,
		}
	}
}
impl<C: PipeTask<Source>, F, Fut, Source> PipeTask<Source> for FilterTask<C, F>
where
	F: FnMut(&C::Item) -> Fut + Clone,
	Fut: Future<Output = bool>,
{
	type Item = C::Item;
	type Async = FilterStreamTaskAsync<C::Async, F, Self::Item, Fut>;
	fn into_async(self) -> Self::Async {
		FilterStreamTaskAsync {
			task: self.task.into_async(),
			f: self.f,
			state: SinkFilterState::None,
		}
	}
}

#[pin_project]
pub struct FilterStreamTaskAsync<C, F, T, Fut> {
	#[pin]
	task: C,
	f: F,
	#[pin]
	state: SinkFilterState<T, Fut>,
}

impl<C: StreamTaskAsync, F, Fut> StreamTaskAsync for FilterStreamTaskAsync<C, F, C::Item, Fut>
where
	F: FnMut(&C::Item) -> Fut + Clone,
	Fut: Future<Output = bool>,
{
	type Item = C::Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		let sink = SinkFilter::new(self_.state, sink, |item: &_| f(item));
		pin_mut!(sink);
		task.poll_run(cx, sink)
	}
}

impl<C: PipeTaskAsync<Source>, F, Fut, Source> PipeTaskAsync<Source>
	for FilterStreamTaskAsync<C, F, C::Item, Fut>
where
	F: FnMut(&<C as PipeTaskAsync<Source>>::Item) -> Fut + Clone,
	Fut: Future<Output = bool>,
{
	type Item = C::Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
		sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		let sink = SinkFilter::new(self_.state, sink, |item: &_| f(item));
		pin_mut!(sink);
		task.poll_run(cx, stream, sink)
	}
}
