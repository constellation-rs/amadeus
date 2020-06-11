mod all;
mod any;
mod collect;
mod combine;
mod count;
mod fold;
mod for_each;
mod max;
mod sample;
mod sum;
mod tuple;

use futures::Stream;
use std::{
	pin::Pin, task::{Context, Poll}
};

use crate::pool::ProcessSend;

use super::dist_pipe::*;

pub use self::{
	all::*, any::*, collect::*, combine::*, count::*, fold::*, for_each::*, max::*, sample::*, sum::*, tuple::*
};

#[must_use]
pub trait Reducer {
	type Item;
	type Output;
	type Async: ReducerAsync<Item = Self::Item, Output = Self::Output>;

	fn into_async(self) -> Self::Async;
}
#[must_use]
pub trait ReducerAsync {
	type Item;
	type Output;

	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()>;
	fn poll_output(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output>;
}
pub trait ReducerSend: Reducer<Output = <Self as ReducerSend>::Output> {
	type Output: Send + 'static;
}
pub trait ReducerProcessSend: ReducerSend<Output = <Self as ReducerProcessSend>::Output> {
	type Output: ProcessSend;
}

pub trait Factory {
	type Item;

	fn make(&self) -> Self::Item;
}

#[must_use]
pub trait DistributedSink<I: DistributedPipe<Source>, Source, B> {
	type ReduceAFactory: Factory<Item = Self::ReduceA> + Clone + ProcessSend;
	type ReduceBFactory: Factory<Item = Self::ReduceB>;
	type ReduceA: ReducerSend<Item = <I as DistributedPipe<Source>>::Item> + ProcessSend;
	type ReduceB: ReducerProcessSend<Item = <Self::ReduceA as Reducer>::Output> + ProcessSend;
	type ReduceC: Reducer<Item = <Self::ReduceB as Reducer>::Output, Output = B>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceBFactory, Self::ReduceC);
}

#[inline(always)]
pub(crate) fn assert_distributed_sink<
	T,
	R: DistributedSink<I, Source, T>,
	I: DistributedPipe<Source>,
	Source,
>(
	r: R,
) -> R {
	r
}
