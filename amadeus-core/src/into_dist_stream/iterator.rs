use futures::{pin_mut, stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	iter, ops::{Range, RangeFrom, RangeInclusive}, pin::Pin, task::{Context, Poll}
};

use super::{DistributedStream, IntoDistributedStream, StreamTask, StreamTaskAsync};
use crate::{pool::ProcessSend, sink::Sink};

pub trait IteratorExt: Iterator + Sized {
	fn dist(self) -> IterIter<Self> {
		IterIter(self)
	}
}
impl<I: Iterator + Sized> IteratorExt for I {}

pub struct IterIter<I>(pub(super) I);

impl<I: Iterator> DistributedStream for IterIter<I>
where
	I::Item: ProcessSend,
{
	type Item = I::Item;
	type Task = IterIterTask<I::Item>;

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.0.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.0.next().map(IterIterTask::new)
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct IterIterTask<T>(Option<T>);
impl<T> IterIterTask<T> {
	fn new(t: T) -> Self {
		Self(Some(t))
	}
}

impl<T> StreamTask for IterIterTask<T> {
	type Item = T;
	type Async = IterIterTask<T>;

	fn into_async(self) -> Self::Async {
		self
	}
}
impl<T> StreamTaskAsync for IterIterTask<T> {
	type Item = T;

	fn poll_run(
		mut self: Pin<&mut Self>, cx: &mut Context, sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()> {
		let stream = stream::iter(iter::from_fn(|| self.0.take()));
		pin_mut!(stream);
		sink.poll_forward(cx, stream)
	}
}

impl<Idx> IntoDistributedStream for Range<Idx>
where
	Self: Iterator,
	<Self as Iterator>::Item: ProcessSend,
{
	type DistStream = IterIter<Self>;
	type Item = <Self as Iterator>::Item;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self)
	}
}

impl<Idx> IntoDistributedStream for RangeFrom<Idx>
where
	Self: Iterator,
	<Self as Iterator>::Item: ProcessSend,
{
	type DistStream = IterIter<Self>;
	type Item = <Self as Iterator>::Item;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self)
	}
}

impl<Idx> IntoDistributedStream for RangeInclusive<Idx>
where
	Self: Iterator,
	<Self as Iterator>::Item: ProcessSend,
{
	type DistStream = IterIter<Self>;
	type Item = <Self as Iterator>::Item;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self)
	}
}
