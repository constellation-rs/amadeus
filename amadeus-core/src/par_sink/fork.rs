// TODO: document why this is sound
#![allow(unsafe_code)]
#![allow(clippy::type_complexity, clippy::too_many_lines)]

use derive_new::new;
use futures::{pin_mut, ready, stream, Stream, StreamExt as _};
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

#[pin_project]
#[derive(new)]
#[must_use]
pub struct Fork<A, B, C, RefAItem> {
	#[pin]
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
		type Item = Sum2<B::Output, C::Output>;
		type Task = JoinTask<A::Task, B::Task, C::Task, RefAItem>;

		#[inline(always)]
		fn size_hint(&self) -> (usize, Option<usize>) {
			self.a.size_hint()
		}
		#[inline(always)]
		fn next_task(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Task>> {
			let self_ = self.project();
			let b = self_.b;
			let c = self_.c;
			self_.a.next_task(cx).map(|task| {
				task.map(|task| JoinTask {
					stream: task,
					pipe: b.task(),
					pipe_ref: c.task(),
					marker: PhantomData,
				})
			})
		}
	}
	impl<A, B, C, Input, RefAItem> ParallelPipe<Input> for Fork<A, B, C, RefAItem>
	where
		A: ParallelPipe<Input>,
		B: ParallelPipe<A::Output>,
		C: ParallelPipe<RefAItem>,
		RefAItem: 'static,
	{
		type Output = Sum2<B::Output, C::Output>;
		type Task = JoinTask<A::Task, B::Task, C::Task, RefAItem>;

		#[inline(always)]
		fn task(&self) -> Self::Task {
			let stream = self.a.task();
			let pipe = self.b.task();
			let pipe_ref = self.c.task();
			JoinTask {
				stream,
				pipe,
				pipe_ref,
				marker: PhantomData,
			}
		}
	}
}

impl<A, B, C, Input, RefAItem> ParallelSink<Input> for Fork<A, B, C, RefAItem>
where
	A: ParallelPipe<Input>,
	B: ParallelSink<A::Output>,
	C: ParallelSink<RefAItem>,
	RefAItem: 'static,
{
	type Done = (B::Done, C::Done);
	type Pipe = Fork<A, B::Pipe, C::Pipe, RefAItem>;
	type ReduceA = ReduceA2<B::ReduceA, C::ReduceA>;
	type ReduceC = ReduceC2<B::ReduceC, C::ReduceC>;

	#[inline(always)]
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
impl<A, B, C, Input, RefAItem> DistributedSink<Input> for Fork<A, B, C, RefAItem>
where
	A: DistributedPipe<Input>,
	B: DistributedSink<A::Output>,
	C: DistributedSink<RefAItem>,
	RefAItem: 'static,
{
	type Done = (B::Done, C::Done);
	type Pipe = Fork<A, B::Pipe, C::Pipe, RefAItem>;
	type ReduceA = ReduceA2<B::ReduceA, C::ReduceA>;
	type ReduceB = ReduceC2<B::ReduceB, C::ReduceB>;
	type ReduceC = ReduceC2<B::ReduceC, C::ReduceC>;

	#[inline(always)]
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
	type Item = Sum2<B::Output, C::Output>;
	type Async = JoinStreamTaskAsync<A::Async, B::Async, C::Async, RefAItem, A::Item>;

	fn into_async(self) -> Self::Async {
		JoinStreamTaskAsync {
			stream: self.stream.into_async(),
			pipe: Some(self.pipe.into_async()),
			pipe_ref: Some(self.pipe_ref.into_async()),
			ref_given: false,
			pending: None,
			marker: PhantomData,
		}
	}
}
impl<A, B, C, Input, RefAItem> PipeTask<Input> for JoinTask<A, B, C, RefAItem>
where
	A: PipeTask<Input>,
	B: PipeTask<A::Output>,
	C: PipeTask<RefAItem>,
{
	type Output = Sum2<B::Output, C::Output>;
	type Async = JoinStreamTaskAsync<A::Async, B::Async, C::Async, RefAItem, A::Output>;

	fn into_async(self) -> Self::Async {
		JoinStreamTaskAsync {
			stream: self.stream.into_async(),
			pipe: Some(self.pipe.into_async()),
			pipe_ref: Some(self.pipe_ref.into_async()),
			ref_given: false,
			pending: None,
			marker: PhantomData,
		}
	}
}

#[pin_project(project = JoinStreamTaskAsyncProj)]
pub struct JoinStreamTaskAsync<A, B, C, RefAItem, T> {
	#[pin]
	stream: A,
	#[pin]
	pipe: Option<B>,
	#[pin]
	pipe_ref: Option<C>,
	ref_given: bool,
	pending: Option<Option<T>>,
	marker: PhantomData<fn() -> RefAItem>,
}

impl<'a, A, B, C, AItem, RefAItem> JoinStreamTaskAsyncProj<'a, A, B, C, RefAItem, AItem>
where
	B: Pipe<AItem>,
	C: Pipe<RefAItem>,
{
	// TODO: fairness
	fn poll(&mut self, cx: &mut Context) -> Option<Poll<Option<Sum2<B::Output, C::Output>>>> {
		if let pending @ Some(_) = self.pending.as_mut().unwrap() {
			let ref_given = &mut *self.ref_given;
			{
				let waker = cx.waker();
				let stream = stream::poll_fn(|cx| {
					if !*ref_given {
						*ref_given = true;
						return Poll::Ready(Some(unsafe { transmute(pending.as_ref()) }));
					}
					let waker_ = cx.waker();
					if !waker.will_wake(waker_) {
						waker_.wake_by_ref();
					}
					Poll::Pending
				})
				.fuse();
				pin_mut!(stream);
				match self
					.pipe_ref
					.as_mut()
					.as_pin_mut()
					.map(|pipe_ref| pipe_ref.poll_next(cx, stream))
				{
					Some(Poll::Ready(Some(item))) => {
						return Some(Poll::Ready(Some(Sum2::B(unsafe { transmute(item) }))))
					}
					Some(Poll::Ready(None)) | None => {
						self.pipe_ref.set(None);
						*ref_given = true
					}
					Some(Poll::Pending) => (),
				}
			}
			let mut item_given = false;
			{
				let waker = cx.waker();
				let stream = stream::poll_fn(|cx| {
					if *ref_given {
						item_given = true;
						return Poll::Ready(Some(pending.take().unwrap()));
					}
					let waker_ = cx.waker();
					if !waker.will_wake(waker_) {
						waker_.wake_by_ref();
					}
					Poll::Pending
				})
				.fuse();
				pin_mut!(stream);
				let res = self
					.pipe
					.as_mut()
					.as_pin_mut()
					.map(|pipe| pipe.poll_next(cx, stream));
				if item_given || matches!(res, Some(Poll::Ready(None)) | None) {
					*ref_given = false;
					*self.pending = None;
				}
				match res {
					Some(Poll::Ready(Some(item))) => {
						return Some(Poll::Ready(Some(Sum2::A(item))));
					}
					Some(Poll::Ready(None)) | None => self.pipe.set(None),
					Some(Poll::Pending) => (),
				}
			}
			if self.pending.is_some() {
				return Some(Poll::Pending);
			}
			None
		} else {
			let stream = stream::empty();
			pin_mut!(stream);
			match self
				.pipe_ref
				.as_mut()
				.as_pin_mut()
				.map(|pipe_ref| pipe_ref.poll_next(cx, stream))
			{
				Some(Poll::Ready(Some(item))) => {
					return Some(Poll::Ready(Some(Sum2::B(unsafe { transmute(item) }))))
				}
				Some(Poll::Ready(None)) => self.pipe_ref.set(None),
				Some(Poll::Pending) | None => (),
			}
			let stream = stream::empty();
			pin_mut!(stream);
			match self
				.pipe
				.as_mut()
				.as_pin_mut()
				.map(|pipe| pipe.poll_next(cx, stream))
			{
				Some(Poll::Ready(Some(item))) => return Some(Poll::Ready(Some(Sum2::A(item)))),
				Some(Poll::Ready(None)) => self.pipe.set(None),
				Some(Poll::Pending) | None => (),
			}
			Some(if self.pipe_ref.is_none() && self.pipe.is_none() {
				Poll::Ready(None)
			} else {
				Poll::Pending
			})
		}
	}
}

impl<A, B, C, RefAItem> Stream for JoinStreamTaskAsync<A, B, C, RefAItem, A::Item>
where
	A: Stream,
	B: Pipe<A::Item>,
	C: Pipe<RefAItem>,
{
	type Item = Sum2<B::Output, C::Output>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		loop {
			if self_.pending.is_none() {
				*self_.pending = Some(ready!(self_.stream.as_mut().poll_next(cx)));
			}
			if let Some(ret) = self_.poll(cx) {
				break ret;
			}
		}
	}
}

impl<A, B, C, Input, RefAItem> Pipe<Input> for JoinStreamTaskAsync<A, B, C, RefAItem, A::Output>
where
	A: Pipe<Input>,
	B: Pipe<A::Output>,
	C: Pipe<RefAItem>,
{
	type Output = Sum2<B::Output, C::Output>;

	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Input>>,
	) -> Poll<Option<Self::Output>> {
		let mut self_ = self.project();
		loop {
			if self_.pending.is_none() {
				*self_.pending = Some(ready!(self_.stream.as_mut().poll_next(cx, stream.as_mut())));
			}
			if let Some(ret) = self_.poll(cx) {
				break ret;
			}
		}
	}
}
