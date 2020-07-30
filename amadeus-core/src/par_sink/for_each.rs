use derive_new::new;
use educe::Educe;
use futures::{ready, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use serde_closure::traits::FnMut;
use std::{
	marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedPipe, DistributedSink, ParallelPipe, ParallelSink, PushReducer, Reducer, ReducerProcessSend, ReducerSend
};
use crate::{pipe::Sink, pool::ProcessSend};

#[derive(new)]
#[must_use]
pub struct ForEach<P, F> {
	pipe: P,
	f: F,
}

impl<P: ParallelPipe<Item>, Item, F> ParallelSink<Item> for ForEach<P, F>
where
	F: FnMut<(P::Output,), Output = ()> + Clone + Send,
{
	type Done = ();
	type Pipe = P;
	type ReduceA = ForEachReducer<P::Output, F>;
	type ReduceC = PushReducer<()>;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceC) {
		(
			self.pipe,
			ForEachReducer(self.f, PhantomData),
			PushReducer::new(),
		)
	}
}
impl<P: DistributedPipe<Item>, Item, F> DistributedSink<Item> for ForEach<P, F>
where
	F: FnMut<(P::Output,), Output = ()> + Clone + ProcessSend,
{
	type Done = ();
	type Pipe = P;
	type ReduceA = ForEachReducer<P::Output, F>;
	type ReduceB = PushReducer<()>;
	type ReduceC = PushReducer<()>;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		(
			self.pipe,
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
pub struct ForEachReducer<Item, F>(F, PhantomData<fn() -> Item>);

impl<Item, F> Reducer<Item> for ForEachReducer<Item, F>
where
	F: FnMut<(Item,), Output = ()>,
{
	type Done = ();
	type Async = Self;

	fn into_async(self) -> Self::Async {
		self
	}
}
impl<Item, F> ReducerProcessSend<Item> for ForEachReducer<Item, F>
where
	F: FnMut<(Item,), Output = ()>,
{
	type Done = ();
}
impl<Item, F> ReducerSend<Item> for ForEachReducer<Item, F>
where
	F: FnMut<(Item,), Output = ()>,
{
	type Done = ();
}

impl<Item, F> Sink<Item> for ForEachReducer<Item, F>
where
	F: FnMut<(Item,), Output = ()>,
{
	type Done = ();

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<Self::Done> {
		let self_ = self.project();
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			self_.0.call_mut((item,));
		}
		Poll::Ready(())
	}
}
