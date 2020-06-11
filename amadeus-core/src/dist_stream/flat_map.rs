use futures::{pin_mut, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedPipe, DistributedStream, PipeTask, PipeTaskAsync, StreamTask, StreamTaskAsync
};
use crate::{
	pool::ProcessSend, sink::{Sink, SinkFlatMap}
};

#[must_use]
pub struct FlatMap<I, F> {
	i: I,
	f: F,
}
impl<I, F> FlatMap<I, F> {
	pub(crate) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedStream, F, R: Stream> DistributedStream for FlatMap<I, F>
where
	F: FnMut(I::Item) -> R + Clone + ProcessSend,
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

impl<I: DistributedPipe<Source>, F, R: Stream, Source> DistributedPipe<Source> for FlatMap<I, F>
where
	F: FnMut(<I as DistributedPipe<Source>>::Item) -> R + Clone + ProcessSend,
{
	type Item = R::Item;
	type Task = FlatMapTask<I::Task, F>;

	fn task(&self) -> Self::Task {
		let task = self.i.task();
		let f = self.f.clone();
		FlatMapTask { task, f }
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
		self: Pin<&mut Self>, cx: &mut Context, sink: Pin<&mut impl Sink<Self::Item>>,
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
		sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		let sink = SinkFlatMap::new(self_.fut, sink, |item| f(item));
		pin_mut!(sink);
		task.poll_run(cx, stream, sink)
	}
}
