// TODO: document why this is sound
#![allow(unsafe_code)]

use derive_new::new;
use futures::{pin_mut, stream, Stream, StreamExt as _};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	marker::PhantomData, pin::Pin, task::{Context, Poll}
};
use sum::Sum2;

use super::{
	DistributedPipe, DistributedSink, ParallelPipe, ParallelSink, PipeTask, ReduceA2, ReduceC2
};
use crate::{
	par_stream::{ParallelStream, StreamTask}, pipe::Pipe, util::transmute
};

#[derive(new)]
#[must_use]
pub struct Fork<A, B, C, RefAItem> {
	a: A,
	b: B,
	c: C,
	marker: PhantomData<fn() -> RefAItem>,
}

impl_par_dist! {
	impl<A, B, C, RefAItem> ParallelStream for Fork<A, B, C, RefAItem>
	where
		A: ParallelStream,
		B: ParallelPipe<A::Item>,
		C: ParallelPipe<RefAItem>,
		RefAItem: 'static,
	{
		type Item = Sum2<B::Item, C::Item>;
		type Task = JoinTask<A::Task, B::Task, C::Task, RefAItem>;

		fn size_hint(&self) -> (usize, Option<usize>) {
			self.a.size_hint()
		}
		fn next_task(&mut self) -> Option<Self::Task> {
			self.a
				.next_task()
				.map(|task| JoinTask{stream:task, pipe:self.b.task(), pipe_ref:self.c.task(), marker:PhantomData})
		}
	}
	impl<A,B,C, Source, RefAItem> ParallelPipe<Source> for Fork<A, B, C, RefAItem>
	where
		A: ParallelPipe<Source>,
		B: ParallelPipe<A::Item>,
		C: ParallelPipe<RefAItem>,
		RefAItem: 'static,
	{
		type Item = Sum2<B::Item, C::Item>;
		type Task = JoinTask<A::Task, B::Task, C::Task, RefAItem>;

		fn task(&self) -> Self::Task {
			let stream = self.a.task();
			let pipe = self.b.task();
			let pipe_ref = self.c.task();
			JoinTask { stream, pipe, pipe_ref, marker: PhantomData }
		}
	}
}

impl<A, B, C, Source, RefAItem> ParallelSink<Source> for Fork<A, B, C, RefAItem>
where
	A: ParallelPipe<Source>,
	B: ParallelSink<A::Item>,
	C: ParallelSink<RefAItem>,
	RefAItem: 'static,
{
	type Output = (B::Output, C::Output);
	type Pipe = Fork<A, B::Pipe, C::Pipe, RefAItem>;
	type ReduceA = ReduceA2<B::ReduceA, C::ReduceA>;
	type ReduceC = ReduceC2<B::ReduceC, C::ReduceC>;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceC) {
		let (iterator_a, reducer_a_a, reducer_a_c) = self.b.reducers();
		let (iterator_b, reducer_b_a, reducer_b_c) = self.c.reducers();
		(
			Fork::new(self.a, iterator_a, iterator_b),
			ReduceA2::new(reducer_a_a, reducer_b_a),
			ReduceC2::new(reducer_a_c, reducer_b_c),
		)
	}
}
impl<A, B, C, Source, RefAItem> DistributedSink<Source> for Fork<A, B, C, RefAItem>
where
	A: DistributedPipe<Source>,
	B: DistributedSink<A::Item>,
	C: DistributedSink<RefAItem>,
	RefAItem: 'static,
{
	type Output = (B::Output, C::Output);
	type Pipe = Fork<A, B::Pipe, C::Pipe, RefAItem>;
	type ReduceA = ReduceA2<B::ReduceA, C::ReduceA>;
	type ReduceB = ReduceC2<B::ReduceB, C::ReduceB>;
	type ReduceC = ReduceC2<B::ReduceC, C::ReduceC>;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		let (iterator_a, reducer_a_a, reducer_a_b, reducer_a_c) = self.b.reducers();
		let (iterator_b, reducer_b_a, reducer_b_b, reducer_b_c) = self.c.reducers();
		(
			Fork::new(self.a, iterator_a, iterator_b),
			ReduceA2::new(reducer_a_a, reducer_b_a),
			ReduceC2::new(reducer_a_b, reducer_b_b),
			ReduceC2::new(reducer_a_c, reducer_b_c),
		)
	}
}

#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "A: Serialize, B: Serialize, C: Serialize"),
	bound(deserialize = "A: Deserialize<'de>, B: Deserialize<'de>, C: Deserialize<'de>")
)]
pub struct JoinTask<A, B, C, RefAItem> {
	stream: A,
	pipe: B,
	pipe_ref: C,
	marker: PhantomData<fn() -> RefAItem>,
}
impl<A, B, C, RefAItem> StreamTask for JoinTask<A, B, C, RefAItem>
where
	A: StreamTask,
	B: PipeTask<A::Item>,
	C: PipeTask<RefAItem>,
{
	type Item = Sum2<B::Item, C::Item>;
	type Async = JoinStreamTaskAsync<A::Async, B::Async, C::Async, RefAItem, A::Item>;

	fn into_async(self) -> Self::Async {
		JoinStreamTaskAsync {
			stream: self.stream.into_async(),
			pipe: self.pipe.into_async(),
			pipe_ref: self.pipe_ref.into_async(),
			ref_given: false,
			pending: None,
			marker: PhantomData,
		}
	}
}
impl<A, B, C, Source, RefAItem> PipeTask<Source> for JoinTask<A, B, C, RefAItem>
where
	A: PipeTask<Source>,
	B: PipeTask<A::Item>,
	C: PipeTask<RefAItem>,
{
	type Item = Sum2<B::Item, C::Item>;
	type Async = JoinStreamTaskAsync<A::Async, B::Async, C::Async, RefAItem, A::Item>;

	fn into_async(self) -> Self::Async {
		JoinStreamTaskAsync {
			stream: self.stream.into_async(),
			pipe: self.pipe.into_async(),
			pipe_ref: self.pipe_ref.into_async(),
			ref_given: false,
			pending: None,
			marker: PhantomData,
		}
	}
}

#[pin_project]
pub struct JoinStreamTaskAsync<A, B, C, RefAItem, T> {
	#[pin]
	stream: A,
	#[pin]
	pipe: B,
	#[pin]
	pipe_ref: C,
	ref_given: bool,
	pending: Option<Option<T>>,
	marker: PhantomData<fn() -> RefAItem>,
}

impl<A, B, C, RefAItem> Stream for JoinStreamTaskAsync<A, B, C, RefAItem, A::Item>
where
	A: Stream,
	B: Pipe<A::Item>,
	C: Pipe<RefAItem>,
{
	type Item = Sum2<B::Item, C::Item>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		loop {
			if self_.pending.is_none() {
				*self_.pending = match self_.stream.as_mut().poll_next(cx) {
					Poll::Ready(x) => Some(x),
					Poll::Pending => None,
				};
			}
			let ref_given = &mut *self_.ref_given;
			let pending = &mut *self_.pending;
			let mut progress = false;
			{
				let stream = stream::poll_fn(|_cx| match pending {
					Some(x) if !*ref_given => {
						*ref_given = true;
						progress = true;
						Poll::Ready(unsafe { transmute(x.as_ref()) })
					}
					_ => Poll::Pending,
				})
				.fuse();
				pin_mut!(stream);
				if let Poll::Ready(item) = self_.pipe_ref.as_mut().poll_next(cx, stream) {
					return Poll::Ready(item.map(|item| Sum2::B(unsafe { transmute(item) })));
				}
			}
			{
				let stream = stream::poll_fn(|_cx| {
					if !*ref_given {
						return Poll::Pending;
					}
					match pending.take() {
						Some(x) => {
							*ref_given = false;
							progress = true;
							Poll::Ready(x)
						}
						None => Poll::Pending,
					}
				})
				.fuse();
				pin_mut!(stream);
				if let Poll::Ready(item) = self_.pipe.as_mut().poll_next(cx, stream) {
					return Poll::Ready(item.map(Sum2::A));
				}
			}
			if !progress {
				break Poll::Pending;
			}
		}
	}
}

impl<A, B, C, Source, RefAItem> Pipe<Source> for JoinStreamTaskAsync<A, B, C, RefAItem, A::Item>
where
	A: Pipe<Source>,
	B: Pipe<A::Item>,
	C: Pipe<RefAItem>,
{
	type Item = Sum2<B::Item, C::Item>;

	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Source>>,
	) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		loop {
			if self_.pending.is_none() {
				*self_.pending = match self_.stream.as_mut().poll_next(cx, stream.as_mut()) {
					Poll::Ready(x) => Some(x),
					Poll::Pending => None,
				};
			}
			let ref_given = &mut *self_.ref_given;
			let pending = &mut *self_.pending;
			let mut progress = false;
			{
				let stream = stream::poll_fn(|_cx| match pending {
					Some(x) if !*ref_given => {
						*ref_given = true;
						progress = true;
						Poll::Ready(unsafe { transmute(x.as_ref()) })
					}
					_ => Poll::Pending,
				})
				.fuse();
				pin_mut!(stream);
				if let Poll::Ready(item) = self_.pipe_ref.as_mut().poll_next(cx, stream) {
					return Poll::Ready(item.map(|item| Sum2::B(unsafe { transmute(item) })));
				}
			}
			{
				let stream = stream::poll_fn(|_cx| {
					if !*ref_given {
						return Poll::Pending;
					}
					match pending.take() {
						Some(x) => {
							*ref_given = false;
							progress = true;
							Poll::Ready(x)
						}
						None => Poll::Pending,
					}
				})
				.fuse();
				pin_mut!(stream);
				if let Poll::Ready(item) = self_.pipe.as_mut().poll_next(cx, stream) {
					return Poll::Ready(item.map(Sum2::A));
				}
			}
			if !progress {
				break Poll::Pending;
			}
		}
	}
}
