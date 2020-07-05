use futures::Stream;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	ops::{Range, RangeFrom, RangeInclusive}, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedStream, IntoDistributedStream, IntoParallelStream, ParallelStream, StreamTask
};
use crate::pool::ProcessSend;

pub trait IteratorExt: Iterator + Sized {
	fn par(self) -> IterParStream<Self> {
		IterParStream(self)
	}
	fn dist(self) -> IterDistStream<Self> {
		IterDistStream(self)
	}
}
impl<I: Iterator + Sized> IteratorExt for I {}

impl_par_dist_rename! {
	pub struct IterParStream<I>(pub(crate) I);

	impl<I: Iterator> ParallelStream for IterParStream<I>
	where
		I::Item: Send + 'static,
	{
		type Item = I::Item;
		type Task = IterStreamTask<I::Item>;

		fn size_hint(&self) -> (usize, Option<usize>) {
			self.0.size_hint()
		}
		fn next_task(&mut self) -> Option<Self::Task> {
			self.0.next().map(IterStreamTask::new)
		}
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct IterStreamTask<T>(Option<T>);
impl<T> IterStreamTask<T> {
	fn new(t: T) -> Self {
		Self(Some(t))
	}
}

impl<T> StreamTask for IterStreamTask<T> {
	type Item = T;
	type Async = IterStreamTask<T>;

	fn into_async(self) -> Self::Async {
		self
	}
}
impl<T> Stream for IterStreamTask<T> {
	type Item = T;

	fn poll_next(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Item>> {
		Poll::Ready(self.project().0.take())
	}
}

impl_par_dist_rename! {
	impl<Idx> IntoParallelStream for Range<Idx>
	where
		Self: Iterator,
		<Self as Iterator>::Item: Send + 'static,
	{
		type ParStream = IterParStream<Self>;
		type Item = <Self as Iterator>::Item;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self)
		}
	}

	impl<Idx> IntoParallelStream for RangeFrom<Idx>
	where
		Self: Iterator,
		<Self as Iterator>::Item: Send + 'static,
	{
		type ParStream = IterParStream<Self>;
		type Item = <Self as Iterator>::Item;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self)
		}
	}

	impl<Idx> IntoParallelStream for RangeInclusive<Idx>
	where
		Self: Iterator,
		<Self as Iterator>::Item: Send + 'static,
	{
		type ParStream = IterParStream<Self>;
		type Item = <Self as Iterator>::Item;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self)
		}
	}
}
