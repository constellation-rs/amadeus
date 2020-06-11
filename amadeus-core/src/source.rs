use crate::{
	dist_pipe::DistributedPipe, dist_sink::DistributedSink, dist_stream::DistributedStream
};

pub trait Source {
	type Item;
	type Error: std::error::Error;

	type DistStream: DistributedStream<Item = Result<Self::Item, Self::Error>>;

	fn dist_stream(self) -> Self::DistStream;
}

pub trait Destination<I>
where
	I: DistributedPipe<Self::Item>,
{
	type Item;
	type Error: std::error::Error;

	type DistSink: DistributedSink<I, Self::Item, Result<(), Self::Error>>;

	fn dist_sink(self) -> Self::DistSink;
}
