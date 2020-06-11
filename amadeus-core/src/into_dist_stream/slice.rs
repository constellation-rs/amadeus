use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
	iter, pin::Pin, slice, task::{Context, Poll}
};

use super::{DistributedStream, IntoDistributedStream, IterIter, StreamTask, StreamTaskAsync};
use crate::{pool::ProcessSend, sink::Sink};

impl<T> IntoDistributedStream for [T]
where
	T: ProcessSend,
{
	type DistStream = Never;
	type Item = Never;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		unreachable!()
	}
}

impl<'a, T: Clone> IntoDistributedStream for &'a [T]
where
	T: ProcessSend,
{
	type DistStream = IterIter<iter::Cloned<slice::Iter<'a, T>>>;
	type Item = T;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.iter().cloned())
	}
}

pub struct Never(!);

impl DistributedStream for Never {
	type Item = Self;
	type Task = Self;

	fn size_hint(&self) -> (usize, Option<usize>) {
		unreachable!()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		unreachable!()
	}
}

impl StreamTask for Never {
	type Item = Self;
	type Async = Self;

	fn into_async(self) -> Self::Async {
		self
	}
}
impl StreamTaskAsync for Never {
	type Item = Self;

	fn poll_run(
		self: Pin<&mut Self>, _cx: &mut Context, _sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()> {
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
