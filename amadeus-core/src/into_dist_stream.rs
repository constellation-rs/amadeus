use crate::dist_stream::{Consumer, ConsumerAsync, DistributedStream};

mod collections;
mod iterator;
mod slice;
pub use self::{collections::*, iterator::*, slice::*};

pub trait IntoDistributedStream {
	type Iter: DistributedStream<Item = Self::Item>;
	type Item; //: ProcessSend;
	fn into_dist_stream(self) -> Self::Iter
	where
		Self: Sized;
	fn dist_stream_mut(&mut self) -> <&mut Self as IntoDistributedStream>::Iter
	where
		for<'a> &'a mut Self: IntoDistributedStream,
	{
		<&mut Self as IntoDistributedStream>::into_dist_stream(self)
	}
	fn dist_stream(&self) -> <&Self as IntoDistributedStream>::Iter
	where
		for<'a> &'a Self: IntoDistributedStream,
	{
		<&Self as IntoDistributedStream>::into_dist_stream(self)
	}
}

impl<T: DistributedStream> IntoDistributedStream for T {
	type Iter = Self;
	type Item = <Self as DistributedStream>::Item;
	fn into_dist_stream(self) -> Self::Iter
	where
		Self: Sized,
	{
		self
	}
}
