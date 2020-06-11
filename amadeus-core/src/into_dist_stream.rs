use crate::dist_stream::{DistributedStream, StreamTask, StreamTaskAsync};

mod collections;
mod iterator;
mod slice;
pub use self::{collections::*, iterator::*, slice::*};

pub trait IntoDistributedStream {
	type DistStream: DistributedStream<Item = Self::Item>;
	type Item;
	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized;
	fn dist_stream_mut(&mut self) -> <&mut Self as IntoDistributedStream>::DistStream
	where
		for<'a> &'a mut Self: IntoDistributedStream,
	{
		<&mut Self as IntoDistributedStream>::into_dist_stream(self)
	}
	fn dist_stream(&self) -> <&Self as IntoDistributedStream>::DistStream
	where
		for<'a> &'a Self: IntoDistributedStream,
	{
		<&Self as IntoDistributedStream>::into_dist_stream(self)
	}
}

impl<T: DistributedStream> IntoDistributedStream for T {
	type DistStream = Self;
	type Item = <Self as DistributedStream>::Item;
	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		self
	}
}
