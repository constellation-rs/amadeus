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
mod stats;

use super::par_pipe::*;
use crate::{pipe::Sink, pool::ProcessSend};

pub use self::{
	all::*, any::*, collect::*, combine::*, combiner::*, count::*, fold::*, folder::*, for_each::*, fork::*, group_by::*, histogram::*, max::*, pipe::*, sample::*, sum::*, tuple::*
};

#[must_use]
pub trait Reducer<Item> {
	type Done;
	type Async: Sink<Item, Done = Self::Done>;

	fn into_async(self) -> Self::Async;
}
pub trait ReducerSend<Item>: Reducer<Item, Done = <Self as ReducerSend<Item>>::Done> {
	type Done: Send + 'static;
}
pub trait ReducerProcessSend<Item>:
	ReducerSend<Item, Done = <Self as ReducerProcessSend<Item>>::Done>
{
	type Done: ProcessSend + 'static;
}

#[must_use]
pub trait ParallelSink<Item> {
	type Done;
	type Pipe: ParallelPipe<Item>;
	type ReduceA: ReducerSend<<Self::Pipe as ParallelPipe<Item>>::Output> + Clone + Send;
	type ReduceC: Reducer<
		<Self::ReduceA as ReducerSend<<Self::Pipe as ParallelPipe<Item>>::Output>>::Done,
		Done = Self::Done,
	>;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceC);
}

#[inline(always)]
pub(crate) fn assert_parallel_sink<R: ParallelSink<Item>, Item>(r: R) -> R {
	r
}

#[must_use]
pub trait DistributedSink<Item> {
	type Done;
	type Pipe: DistributedPipe<Item>;
	type ReduceA: ReducerSend<<Self::Pipe as DistributedPipe<Item>>::Output>
		+ Clone
		+ ProcessSend
		+ Send;
	type ReduceB: ReducerProcessSend<
			<Self::ReduceA as ReducerSend<<Self::Pipe as DistributedPipe<Item>>::Output>>::Done,
		> + Clone
		+ ProcessSend;
	type ReduceC: Reducer<
		<Self::ReduceB as ReducerProcessSend<
			<Self::ReduceA as ReducerSend<<Self::Pipe as DistributedPipe<Item>>::Output>>::Done,
		>>::Done,
		Done = Self::Done,
	>;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceB, Self::ReduceC);
}

#[inline(always)]
pub(crate) fn assert_distributed_sink<R: DistributedSink<Item>, Item>(r: R) -> R {
	r
}
