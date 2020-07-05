mod all;
mod any;
mod collect;
mod combine;
mod combiner;
mod count;
mod fold;
mod folder;
mod for_each;
mod fork;
mod group_by;
mod histogram;
mod max;
mod pipe;
mod sample;
mod sum;
mod tuple;

use std::{future::Future, ops::DerefMut, pin::Pin};

use crate::{pipe::Sink, pool::ProcessSend};

use super::par_pipe::*;

pub use self::{
	all::*, any::*, collect::*, combine::*, combiner::*, count::*, fold::*, folder::*, for_each::*, fork::*, group_by::*, histogram::*, max::*, pipe::*, sample::*, sum::*, tuple::*
};

#[must_use]
pub trait Reducer<Source> {
	type Output;
	type Async: ReducerAsync<Source, Output = Self::Output>;

	fn into_async(self) -> Self::Async;
}
pub trait ReducerSend<Source>:
	Reducer<Source, Output = <Self as ReducerSend<Source>>::Output>
{
	type Output: Send + 'static;
}
pub trait ReducerProcessSend<Source>:
	ReducerSend<Source, Output = <Self as ReducerProcessSend<Source>>::Output>
{
	type Output: ProcessSend + 'static;
}
#[must_use]
pub trait ReducerAsync<Source>: Sink<Source> {
	type Output;

	fn output<'a>(self: Pin<&'a mut Self>) -> Pin<Box<dyn Future<Output = Self::Output> + 'a>>;
}

impl<P, Source> ReducerAsync<Source> for Pin<P>
where
	P: DerefMut + Unpin,
	P::Target: ReducerAsync<Source>,
{
	type Output = <P::Target as ReducerAsync<Source>>::Output;

	fn output<'a>(self: Pin<&'a mut Self>) -> Pin<Box<dyn Future<Output = Self::Output> + 'a>> {
		self.get_mut().as_mut().output()
	}
}
impl<T: ?Sized, Source> ReducerAsync<Source> for &mut T
where
	T: ReducerAsync<Source> + Unpin,
{
	type Output = T::Output;

	fn output<'a>(mut self: Pin<&'a mut Self>) -> Pin<Box<dyn Future<Output = Self::Output> + 'a>> {
		Box::pin(async move { Pin::new(&mut **self).output().await })
	}
}

#[must_use]
pub trait ParallelSink<Source> {
	type Output;
	type Pipe: ParallelPipe<Source>;
	type ReduceA: ReducerSend<<Self::Pipe as ParallelPipe<Source>>::Item> + Clone + Send;
	type ReduceC: Reducer<
		<Self::ReduceA as ReducerSend<<Self::Pipe as ParallelPipe<Source>>::Item>>::Output,
		Output = Self::Output,
	>;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceC);
}

#[inline(always)]
pub(crate) fn assert_parallel_sink<R: ParallelSink<Source>, Source>(r: R) -> R {
	r
}

#[must_use]
pub trait DistributedSink<Source> {
	type Output;
	type Pipe: DistributedPipe<Source>;
	type ReduceA: ReducerSend<<Self::Pipe as DistributedPipe<Source>>::Item>
		+ Clone
		+ ProcessSend
		+ Send;
	type ReduceB: ReducerProcessSend<
			<Self::ReduceA as ReducerSend<<Self::Pipe as DistributedPipe<Source>>::Item>>::Output,
		> + Clone
		+ ProcessSend;
	type ReduceC: Reducer<
		<Self::ReduceB as ReducerProcessSend<
			<Self::ReduceA as ReducerSend<<Self::Pipe as DistributedPipe<Source>>::Item>>::Output,
		>>::Output,
		Output = Self::Output,
	>;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceB, Self::ReduceC);
}

#[inline(always)]
pub(crate) fn assert_distributed_sink<R: DistributedSink<Source>, Source>(r: R) -> R {
	r
}
