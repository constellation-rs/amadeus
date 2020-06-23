mod all;
mod any;
mod collect;
mod combine;
mod combiner;
mod count;
mod fold;
mod folder;
mod for_each;
mod group_by;
mod max;
mod pipe;
mod sample;
mod sum;
mod tuple;

use futures::Stream;
use std::{
	ops::DerefMut, pin::Pin, task::{Context, Poll}
};

use crate::pool::ProcessSend;

use super::par_pipe::*;

pub use self::{
	all::*, any::*, collect::*, combine::*, combiner::*, count::*, fold::*, folder::*, for_each::*, group_by::*, max::*, pipe::*, sample::*, sum::*, tuple::*
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
	type Output: ProcessSend + 'static;
}

impl<P> ReducerAsync for Pin<P>
where
	P: DerefMut + Unpin,
	P::Target: ReducerAsync,
{
	type Item = <P::Target as ReducerAsync>::Item;
	type Output = <P::Target as ReducerAsync>::Output;

	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		self.get_mut().as_mut().poll_forward(cx, stream)
	}
	fn poll_output(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		self.get_mut().as_mut().poll_output(cx)
	}
}
impl<T: ?Sized> ReducerAsync for &mut T
where
	T: ReducerAsync + Unpin,
{
	type Item = T::Item;
	type Output = T::Output;

	fn poll_forward(
		mut self: Pin<&mut Self>, cx: &mut Context,
		stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		Pin::new(&mut **self).poll_forward(cx, stream)
	}
	fn poll_output(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		Pin::new(&mut **self).poll_output(cx)
	}
}

pub trait Factory {
	type Item;

	fn make(&self) -> Self::Item;
}

#[must_use]
pub trait DistributedSink<Source> {
	type Output;
	type Pipe: DistributedPipe<Source>;
	type ReduceAFactory: Factory<Item = Self::ReduceA> + Clone + ProcessSend;
	type ReduceBFactory: Factory<Item = Self::ReduceB>;
	type ReduceA: ReducerSend<Item = <Self::Pipe as DistributedPipe<Source>>::Item> + Send;
	type ReduceB: ReducerProcessSend<Item = <Self::ReduceA as Reducer>::Output> + ProcessSend;
	type ReduceC: Reducer<Item = <Self::ReduceB as Reducer>::Output, Output = Self::Output>;

	fn reducers(
		self,
	) -> (
		Self::Pipe,
		Self::ReduceAFactory,
		Self::ReduceBFactory,
		Self::ReduceC,
	);
}

#[inline(always)]
pub(crate) fn assert_distributed_sink<R: DistributedSink<Source>, Source>(r: R) -> R {
	r
}

#[must_use]
pub trait ParallelSink<Source> {
	type Output;
	type Pipe: ParallelPipe<Source>;
	type ReduceAFactory: Factory<Item = Self::ReduceA>;
	type ReduceA: ReducerSend<Item = <Self::Pipe as ParallelPipe<Source>>::Item> + Send;
	type ReduceC: Reducer<Item = <Self::ReduceA as Reducer>::Output, Output = Self::Output>;

	fn reducers(self) -> (Self::Pipe, Self::ReduceAFactory, Self::ReduceC);
}

#[inline(always)]
pub(crate) fn assert_parallel_sink<R: ParallelSink<Source>, Source>(r: R) -> R {
	r
}
