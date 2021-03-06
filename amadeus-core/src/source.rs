use std::{error::Error, fmt::Debug};

use crate::{
	par_sink::{DistributedSink, ParallelSink}, par_stream::{DistributedStream, ParallelStream}
};

pub trait Source: Clone + Debug {
	type Item;
	type Error: Error;

	type ParStream: ParallelStream<Item = Result<Self::Item, Self::Error>>;
	type DistStream: DistributedStream<Item = Result<Self::Item, Self::Error>>;

	fn par_stream(self) -> Self::ParStream;
	fn dist_stream(self) -> Self::DistStream;
}

pub trait Destination: Clone + Debug {
	type Item;
	type Error: Error;

	type ParSink: ParallelSink<Self::Item>;
	type DistSink: DistributedSink<Self::Item>;

	fn par_sink(self) -> Self::ParSink;
	fn dist_sink(self) -> Self::DistSink;
}
