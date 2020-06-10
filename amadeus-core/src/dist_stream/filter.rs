use futures::{pin_mut, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	future::Future, pin::Pin, task::{Context, Poll}
};

use super::{
	Consumer, ConsumerAsync, ConsumerMulti, ConsumerMultiAsync, DistributedStream, DistributedStreamMulti
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
	pub(super) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedStream, F, Fut> DistributedStream for Filter<I, F>
where
	F: FnMut(&I::Item) -> Fut + Clone + ProcessSend,
	Fut: Future<Output = bool>,
{
	type Item = I::Item;
	type Task = FilterConsumer<I::Task, F>;

	fn size_hint(&self) -> (usize, Option<usize>) {
		(0, self.i.size_hint().1)
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.i.next_task().map(|task| {
			let f = self.f.clone();
			FilterConsumer { task, f }
		})
	}
}

impl<I: DistributedStreamMulti<Source>, F, Fut, Source> DistributedStreamMulti<Source>
	for Filter<I, F>
where
	F: FnMut(&<I as DistributedStreamMulti<Source>>::Item) -> Fut + Clone + ProcessSend,
	Fut: Future<Output = bool>,
{
	type Item = I::Item;
	type Task = FilterConsumer<I::Task, F>;

	fn task(&self) -> Self::Task {
		let task = self.i.task();
		let f = self.f.clone();
		FilterConsumer { task, f }
	}
}

#[derive(Serialize, Deserialize)]
pub struct FilterConsumer<C, F> {
	task: C,
	f: F,
}

impl<C: Consumer, F, Fut> Consumer for FilterConsumer<C, F>
where
	F: FnMut(&C::Item) -> Fut + Clone,
	Fut: Future<Output = bool>,
{
	type Item = C::Item;
	type Async = FilterConsumerAsync<C::Async, F, Self::Item, Fut>;
	fn into_async(self) -> Self::Async {
		FilterConsumerAsync {
			task: self.task.into_async(),
			f: self.f,
			state: SinkFilterState::None,
		}
	}
}
impl<C: ConsumerMulti<Source>, F, Fut, Source> ConsumerMulti<Source> for FilterConsumer<C, F>
where
	F: FnMut(&C::Item) -> Fut + Clone,
	Fut: Future<Output = bool>,
{
	type Item = C::Item;
	type Async = FilterConsumerAsync<C::Async, F, Self::Item, Fut>;
	fn into_async(self) -> Self::Async {
		FilterConsumerAsync {
			task: self.task.into_async(),
			f: self.f,
			state: SinkFilterState::None,
		}
	}
}

#[pin_project]
pub struct FilterConsumerAsync<C, F, T, Fut> {
	#[pin]
	task: C,
	f: F,
	#[pin]
	state: SinkFilterState<T, Fut>,
}

impl<C: ConsumerAsync, F, Fut> ConsumerAsync for FilterConsumerAsync<C, F, C::Item, Fut>
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

impl<C: ConsumerMultiAsync<Source>, F, Fut, Source> ConsumerMultiAsync<Source>
	for FilterConsumerAsync<C, F, C::Item, Fut>
where
	F: FnMut(&<C as ConsumerMultiAsync<Source>>::Item) -> Fut + Clone,
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
