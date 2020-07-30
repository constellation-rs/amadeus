use futures::Stream;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
	convert::Infallible, iter, pin::Pin, slice, task::{Context, Poll}
};

use super::{
	DistributedStream, IntoDistributedStream, IntoParallelStream, IterDistStream, IterParStream, ParallelStream, StreamTask
};
use crate::pool::ProcessSend;

impl_par_dist_rename! {
	impl<T> IntoParallelStream for [T]
	where
		T: Send,
	{
		type ParStream = Never;
		type Item = Never;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			unreachable!()
		}
	}

	impl<'a, T: Clone> IntoParallelStream for &'a [T]
	where
		T: Send,
	{
		type ParStream = IterParStream<iter::Cloned<slice::Iter<'a, T>>>;
		type Item = T;

		#[inline]
		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.iter().cloned())
		}
	}

	impl ParallelStream for Never {
		type Item = Self;
		type Task = Self;

		fn size_hint(&self) -> (usize, Option<usize>) {
			unreachable!()
		}
		fn next_task(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Task>> {
			unreachable!()
		}
	}
}

pub struct Never(Infallible);

impl StreamTask for Never {
	type Item = Self;
	type Async = Self;

	fn into_async(self) -> Self::Async {
		self
	}
}
impl Stream for Never {
	type Item = Self;

	fn poll_next(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Item>> {
		unreachable!()
	}
}

impl Serialize for Never {
	fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		unreachable!()
	}
}
impl<'de> Deserialize<'de> for Never {
	fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		unreachable!()
	}
}
