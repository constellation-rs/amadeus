use futures::{pin_mut, stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	iter, ops::{Range, RangeFrom, RangeInclusive}, pin::Pin, task::{Context, Poll}
};

use super::{Consumer, ConsumerAsync, DistributedIterator, IntoDistributedIterator};
use crate::{pool::ProcessSend, sink::Sink};

pub trait IteratorExt: Iterator + Sized {
	fn dist(self) -> IterIter<Self> {
		IterIter(self)
	}
}
impl<I: Iterator + Sized> IteratorExt for I {}

pub struct IterIter<I>(pub(super) I);

impl<I: Iterator> DistributedIterator for IterIter<I>
where
	I::Item: ProcessSend,
{
	type Item = I::Item;
	type Task = IterIterConsumer<I::Item>;

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.0.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.0.next().map(IterIterConsumer::new)
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct IterIterConsumer<T>(Option<T>);
impl<T> IterIterConsumer<T> {
	fn new(t: T) -> Self {
		Self(Some(t))
	}
}

impl<T> Consumer for IterIterConsumer<T> {
	type Item = T;
	type Async = IterIterConsumer<T>;

	fn into_async(self) -> Self::Async {
		self
	}
}
impl<T> ConsumerAsync for IterIterConsumer<T> {
	type Item = T;

	fn poll_run(
		mut self: Pin<&mut Self>, cx: &mut Context, sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()> {
		let stream = stream::iter(iter::from_fn(|| self.0.take()));
		pin_mut!(stream);
		sink.poll_forward(cx, stream)
	}
}

impl<Idx> IntoDistributedIterator for Range<Idx>
where
	Self: Iterator,
	<Self as Iterator>::Item: ProcessSend,
{
	type Iter = IterIter<Self>;
	type Item = <Self as Iterator>::Item;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self)
	}
}

impl<Idx> IntoDistributedIterator for RangeFrom<Idx>
where
	Self: Iterator,
	<Self as Iterator>::Item: ProcessSend,
{
	type Iter = IterIter<Self>;
	type Item = <Self as Iterator>::Item;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self)
	}
}

impl<Idx> IntoDistributedIterator for RangeInclusive<Idx>
where
	Self: Iterator,
	<Self as Iterator>::Item: ProcessSend,
{
	type Iter = IterIter<Self>;
	type Item = <Self as Iterator>::Item;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self)
	}
}
