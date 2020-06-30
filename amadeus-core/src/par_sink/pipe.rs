use derive_new::new;
use futures::{pin_mut, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedPipe, DistributedSink, ParallelPipe, ParallelSink, PipeTask, PipeTaskAsync
};
use crate::sink::Sink;

#[derive(new)]
#[must_use]
pub struct Pipe<A, B> {
	a: A,
	b: B,
}

impl_par_dist! {
	impl<A: ParallelPipe<Source>, B: ParallelPipe<A::Item>, Source> ParallelPipe<Source> for Pipe<A, B> {
		type Item = B::Item;
		type Task = JoinTask<A::Task, B::Task>;

		fn task(&self) -> Self::Task {
			let a = self.a.task();
			let b = self.b.task();
			JoinTask { a, b }
		}
	}
}

impl<A: ParallelPipe<Source>, B: ParallelSink<A::Item>, Source> ParallelSink<Source>
	for Pipe<A, B>
{
	type Output = B::Output;
	type Pipe = Pipe<A, B::Pipe>;
	type ReduceA = B::ReduceA;
	type ReduceC = B::ReduceC;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceC) {
		let (a, b, c) = self.b.reducers();
		(Pipe::new(self.a, a), b, c)
	}
}
impl<A: DistributedPipe<Source>, B: DistributedSink<A::Item>, Source> DistributedSink<Source>
	for Pipe<A, B>
{
	type Output = B::Output;
	type Pipe = Pipe<A, B::Pipe>;
	type ReduceA = B::ReduceA;
	type ReduceB = B::ReduceB;
	type ReduceC = B::ReduceC;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		let (a, b, c, d) = self.b.reducers();
		(Pipe::new(self.a, a), b, c, d)
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct JoinTask<A, B> {
	#[pin]
	a: A,
	#[pin]
	b: B,
}

impl<A: PipeTask<Source>, B: PipeTask<A::Item>, Source> PipeTask<Source> for JoinTask<A, B> {
	type Item = B::Item;
	type Async = JoinTask<A::Async, B::Async>;
	fn into_async(self) -> Self::Async {
		JoinTask {
			a: self.a.into_async(),
			b: self.b.into_async(),
		}
	}
}

impl<A: PipeTaskAsync<Source>, B: PipeTaskAsync<A::Item>, Source> PipeTaskAsync<Source>
	for JoinTask<A, B>
{
	type Item = B::Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
		sink: Pin<&mut impl Sink<Item = Self::Item>>,
	) -> Poll<()> {
		#[pin_project]
		struct Proxy<'a, I, B, Item>(#[pin] I, Pin<&'a mut B>, PhantomData<fn() -> Item>);
		impl<'a, I, B, Item> Sink for Proxy<'a, I, B, Item>
		where
			I: Sink<Item = B::Item>,
			B: PipeTaskAsync<Item>,
		{
			type Item = Item;

			fn poll_forward(
				self: Pin<&mut Self>, cx: &mut Context,
				stream: Pin<&mut impl Stream<Item = Self::Item>>,
			) -> Poll<()> {
				let self_ = self.project();
				self_.1.as_mut().poll_run(cx, stream, self_.0)
			}
		}
		let self_ = self.project();
		let sink = Proxy(sink, self_.b, PhantomData);
		pin_mut!(sink);
		self_.a.poll_run(cx, stream, sink)
	}
}
