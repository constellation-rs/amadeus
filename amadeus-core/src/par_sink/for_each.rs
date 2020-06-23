use derive_new::new;
use futures::{ready, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{
	DefaultReducerFactory, DistributedPipe, DistributedSink, Factory, ParallelPipe, ParallelSink, PushReducer, Reducer, ReducerAsync, ReducerProcessSend, ReducerSend
};
use crate::pool::ProcessSend;

#[derive(new)]
#[must_use]
pub struct ForEach<I, F> {
	i: I,
	f: F,
}

impl<I: DistributedPipe<Source>, Source, F> DistributedSink<Source> for ForEach<I, F>
where
	F: FnMut(I::Item) + Clone + ProcessSend + 'static,
{
	type Output = ();
	type Pipe = I;
	type ReduceAFactory = ForEachReducerFactory<I::Item, F>;
	type ReduceBFactory = DefaultReducerFactory<Self::ReduceB>;
	type ReduceA = ForEachReducer<I::Item, F>;
	type ReduceB = PushReducer<()>;
	type ReduceC = PushReducer<()>;

	fn reducers(
		self,
	) -> (
		Self::Pipe,
		Self::ReduceAFactory,
		Self::ReduceBFactory,
		Self::ReduceC,
	) {
		(
			self.i,
			ForEachReducerFactory(self.f, PhantomData),
			DefaultReducerFactory::new(),
			PushReducer::new(()),
		)
	}
}
impl<I: ParallelPipe<Source>, Source, F> ParallelSink<Source> for ForEach<I, F>
where
	F: FnMut(I::Item) + Clone + Send + 'static,
{
	type Output = ();
	type Pipe = I;
	type ReduceAFactory = ForEachReducerFactory<I::Item, F>;
	type ReduceA = ForEachReducer<I::Item, F>;
	type ReduceC = PushReducer<()>;

	fn reducers(self) -> (Self::Pipe, Self::ReduceAFactory, Self::ReduceC) {
		(
			self.i,
			ForEachReducerFactory(self.f, PhantomData),
			PushReducer::new(()),
		)
	}
}

#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "F: Serialize"),
	bound(deserialize = "F: Deserialize<'de>")
)]
pub struct ForEachReducerFactory<A, F>(F, PhantomData<fn() -> A>);
impl<A, F> Factory for ForEachReducerFactory<A, F>
where
	F: FnMut(A) + Clone,
{
	type Item = ForEachReducer<A, F>;
	fn make(&self) -> Self::Item {
		ForEachReducer(self.0.clone(), PhantomData)
	}
}
impl<A, F> Clone for ForEachReducerFactory<A, F>
where
	F: Clone,
{
	fn clone(&self) -> Self {
		Self(self.0.clone(), PhantomData)
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
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
