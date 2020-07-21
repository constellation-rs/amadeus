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

use super::par_pipe::*;
use crate::{pipe::Sink, pool::ProcessSend};

pub use self::{
	all::*, any::*, collect::*, combine::*, combiner::*, count::*, fold::*, folder::*, for_each::*, fork::*, group_by::*, histogram::*, max::*, pipe::*, sample::*, sum::*, tuple::*
};

#[must_use]
pub trait Reducer<Input> {
	type Done;
	type Async: Sink<Input, Done = Self::Done>;

	fn into_async(self) -> Self::Async;
}
pub trait ReducerSend<Input>: Reducer<Input, Done = <Self as ReducerSend<Input>>::Done> {
	type Done: Send + 'static;
}
pub trait ReducerProcessSend<Input>:
	ReducerSend<Input, Done = <Self as ReducerProcessSend<Input>>::Done>
{
	type Done: ProcessSend + 'static;
}

#[must_use]
pub trait ParallelSink<Input> {
	type Done;
	type Pipe: ParallelPipe<Input>;
	type ReduceA: ReducerSend<<Self::Pipe as ParallelPipe<Input>>::Output> + Clone + Send;
	type ReduceC: Reducer<
		<Self::ReduceA as ReducerSend<<Self::Pipe as ParallelPipe<Input>>::Output>>::Done,
		Done = Self::Done,
	>;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceC);
}

#[inline(always)]
pub(crate) fn assert_parallel_sink<R: ParallelSink<Input>, Input>(r: R) -> R {
	r
}

#[must_use]
pub trait DistributedSink<Input> {
	type Done;
	type Pipe: DistributedPipe<Input>;
	type ReduceA: ReducerSend<<Self::Pipe as DistributedPipe<Input>>::Output>
		+ Clone
		+ ProcessSend
		+ Send;
	type ReduceB: ReducerProcessSend<
			<Self::ReduceA as ReducerSend<<Self::Pipe as DistributedPipe<Input>>::Output>>::Done,
		> + Clone
		+ ProcessSend;
	type ReduceC: Reducer<
		<Self::ReduceB as ReducerProcessSend<
			<Self::ReduceA as ReducerSend<<Self::Pipe as DistributedPipe<Input>>::Output>>::Done,
		>>::Done,
		Done = Self::Done,
	>;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceB, Self::ReduceC);
}

#[inline(always)]
pub(crate) fn assert_distributed_sink<R: DistributedSink<Input>, Input>(r: R) -> R {
	r
}
