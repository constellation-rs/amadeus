use derive_new::new;
use educe::Educe;
use futures::{ready, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use serde_closure::traits::FnMut;
use std::{
	future::Future, marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedPipe, DistributedSink, ParallelPipe, ParallelSink, PushReducer, Reducer, ReducerAsync, ReducerProcessSend, ReducerSend
};
use crate::{pipe::Sink, pool::ProcessSend};

#[derive(new)]
#[must_use]
pub struct ForEach<I, F> {
	i: I,
	f: F,
}

impl<I: ParallelPipe<Source>, Source, F> ParallelSink<Source> for ForEach<I, F>
where
	F: FnMut<(I::Item,), Output = ()> + Clone + Send + 'static,
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
	F: FnMut<(I::Item,), Output = ()> + Clone + ProcessSend + 'static,
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

impl<A, F> Reducer<A> for ForEachReducer<A, F>
where
	F: FnMut<(A,), Output = ()> + Clone,
{
	type Output = ();
	type Async = Self;

	fn into_async(self) -> Self::Async {
		self
	}
}
impl<A, F> ReducerProcessSend<A> for ForEachReducer<A, F>
where
	F: FnMut<(A,), Output = ()> + Clone,
{
	type Output = ();
}
impl<A, F> ReducerSend<A> for ForEachReducer<A, F>
where
	F: FnMut<(A,), Output = ()> + Clone,
{
	type Output = ();
}

impl<A, F> Sink<A> for ForEachReducer<A, F>
where
	F: FnMut<(A,), Output = ()> + Clone,
{
	#[inline(always)]
	fn poll_pipe(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = A>>,
	) -> Poll<()> {
		let self_ = self.project();
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			self_.0.call_mut((item,));
		}
		Poll::Ready(())
	}
}
impl<A, F> ReducerAsync<A> for ForEachReducer<A, F>
where
	F: FnMut<(A,), Output = ()> + Clone,
{
	type Output = ();

	fn output<'a>(self: Pin<&'a mut Self>) -> Pin<Box<dyn Future<Output = Self::Output> + 'a>> {
		Box::pin(async move {})
	}
}
