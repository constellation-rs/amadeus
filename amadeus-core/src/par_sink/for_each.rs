use derive_new::new;
use educe::Educe;
use futures::{ready, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedPipe, DistributedSink, ParallelPipe, ParallelSink, PushReducer, Reducer, ReducerAsync, ReducerProcessSend, ReducerSend
};
use crate::pool::ProcessSend;

#[derive(new)]
#[must_use]
pub struct ForEach<I, F> {
	i: I,
	f: F,
}

impl<I: ParallelPipe<Source>, Source, F> ParallelSink<Source> for ForEach<I, F>
where
	F: FnMut(I::Item) + Clone + Send + 'static,
{
	type Output = ();
	type Pipe = I;
	type ReduceA = ForEachReducer<I::Item, F>;
	type ReduceC = PushReducer<()>;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceC) {
		(
			self.i,
			ForEachReducer(self.f, PhantomData),
			PushReducer::new(),
		)
	}
}
impl<I: DistributedPipe<Source>, Source, F> DistributedSink<Source> for ForEach<I, F>
where
	F: FnMut(I::Item) + Clone + ProcessSend + 'static,
{
	type Output = ();
	type Pipe = I;
	type ReduceA = ForEachReducer<I::Item, F>;
	type ReduceB = PushReducer<()>;
	type ReduceC = PushReducer<()>;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		(
			self.i,
			ForEachReducer(self.f, PhantomData),
			PushReducer::new(),
			PushReducer::new(),
		)
	}
}

#[pin_project]
#[derive(Educe, Serialize, Deserialize)]
#[educe(Clone(bound = "F: Clone"))]
#[serde(
	bound(serialize = "F: Serialize"),
	bound(deserialize = "F: Deserialize<'de>")
)]
pub struct ForEachReducer<A, F>(F, PhantomData<fn() -> A>);

impl<A, F> Reducer for ForEachReducer<A, F>
where
	F: FnMut(A) + Clone,
{
	type Item = A;
	type Output = ();
	type Async = Self;

	fn into_async(self) -> Self::Async {
		self
	}
}
impl<A, F> ReducerProcessSend for ForEachReducer<A, F>
where
	F: FnMut(A) + Clone,
{
	type Output = ();
}
impl<A, F> ReducerSend for ForEachReducer<A, F>
where
	F: FnMut(A) + Clone,
{
	type Output = ();
}

impl<A, F> ReducerAsync for ForEachReducer<A, F>
where
	F: FnMut(A) + Clone,
{
	type Item = A;
	type Output = ();

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			self_.0(item);
		}
		Poll::Ready(())
	}
	fn poll_output(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
		Poll::Ready(())
	}
}
