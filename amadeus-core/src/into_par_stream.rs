use crate::par_stream::{DistributedStream, ParallelStream, StreamTask};

mod collections;
mod iterator;
mod slice;
pub use self::{collections::*, iterator::*, slice::*};

impl_par_dist_rename! {
	pub trait IntoParallelStream {
		type ParStream: ParallelStream<Item = Self::Item>;
		type Item;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized;
		#[inline(always)]
		fn par_stream_mut(&mut self) -> <&mut Self as IntoParallelStream>::ParStream
		where
			for<'a> &'a mut Self: IntoParallelStream,
		{
			<&mut Self as IntoParallelStream>::into_par_stream(self)
		}
		#[inline(always)]
		fn par_stream(&self) -> <&Self as IntoParallelStream>::ParStream
		where
			for<'a> &'a Self: IntoParallelStream,
		{
			<&Self as IntoParallelStream>::into_par_stream(self)
		}
	}

	impl<T: ParallelStream> IntoParallelStream for T {
		type ParStream = Self;
		type Item = <Self as ParallelStream>::Item;

		#[inline(always)]
		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			self
		}
	}
}
