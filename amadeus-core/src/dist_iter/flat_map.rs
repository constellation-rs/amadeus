use futures::Stream;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	pin::Pin, task::{Context, Poll}
};

use super::{
	Consumer, ConsumerAsync, ConsumerMulti, ConsumerMultiAsync, DistributedIterator, DistributedIteratorMulti
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
	pub(super) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedIterator, F, R: Stream> DistributedIterator for FlatMap<I, F>
where
	F: FnMut(I::Item) -> R + Clone + ProcessSend,
{
	type Item = R::Item;
	type Task = FlatMapConsumer<I::Task, F>;

	fn size_hint(&self) -> (usize, Option<usize>) {
		(0, None)
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.i.next_task().map(|task| {
			let f = self.f.clone();
			FlatMapConsumer { task, f }
		})
	}
}

impl<I: DistributedIteratorMulti<Source>, F, R: Stream, Source> DistributedIteratorMulti<Source>
	for FlatMap<I, F>
where
	F: FnMut(<I as DistributedIteratorMulti<Source>>::Item) -> R + Clone + ProcessSend,
{
	type Item = R::Item;
	type Task = FlatMapConsumer<I::Task, F>;

	fn task(&self) -> Self::Task {
		let task = self.i.task();
		let f = self.f.clone();
		FlatMapConsumer { task, f }
	}
}

#[derive(Serialize, Deserialize)]
pub struct FlatMapConsumer<C, F> {
	task: C,
	f: F,
}
impl<C: Consumer, F: FnMut(C::Item) -> R + Clone, R: Stream> Consumer for FlatMapConsumer<C, F> {
	type Item = R::Item;
	type Async = FlatMapConsumerAsync<C::Async, F, R>;
	fn into_async(self) -> Self::Async {
		FlatMapConsumerAsync {
			task: self.task.into_async(),
			f: self.f,
			fut: None,
		}
	}
}
impl<C: ConsumerMulti<Source>, F: FnMut(C::Item) -> R + Clone, R: Stream, Source>
	ConsumerMulti<Source> for FlatMapConsumer<C, F>
{
	type Item = R::Item;
	type Async = FlatMapConsumerAsync<C::Async, F, R>;
	fn into_async(self) -> Self::Async {
		FlatMapConsumerAsync {
			task: self.task.into_async(),
			f: self.f,
			fut: None,
		}
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct FlatMapConsumerAsync<C, F, Fut> {
	#[pin]
	task: C,
	f: F,
	#[pin]
	fut: Option<Fut>,
}

impl<C: ConsumerAsync, F: FnMut(C::Item) -> R + Clone, R: Stream> ConsumerAsync
	for FlatMapConsumerAsync<C, F, R>
{
	type Item = R::Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, sink: &mut impl Sink<Self::Item>,
	) -> Poll<bool> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		task.poll_run(cx, &mut SinkFlatMap::new(self_.fut, sink, |item| f(item)))
		// let (task, mut f) = (self.task, self.f);
		// task.poll_run(cx, &mut |item| {
		// 	for x in f(item) {
		// 		if !i(x) {
		// 			return false;
		// 		}
		// 	}
		// 	true
		// })
	}
}

impl<C: ConsumerMultiAsync<Source>, F, R: Stream, Source> ConsumerMultiAsync<Source>
	for FlatMapConsumerAsync<C, F, R>
where
	F: FnMut(<C as ConsumerMultiAsync<Source>>::Item) -> R + Clone,
{
	type Item = R::Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, source: Option<Source>,
		sink: &mut impl Sink<Self::Item>,
	) -> Poll<bool> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		task.poll_run(
			cx,
			source,
			&mut SinkFlatMap::new(self_.fut, sink, |item| f(item)),
		)
		// 	let (task, f) = (&self.task, &self.f);
		// 	task.poll_run(cx, source, &mut |item| {
		// 		for x in f.clone()(item) {
		// 			if !i(x) {
		// 				return false;
		// 			}
		// 		}
		// 		true
		// 	})
	}
}
