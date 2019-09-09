use serde::{Deserialize, Serialize};
use std::ops::{Range, RangeFrom, RangeInclusive};

use super::{Consumer, DistributedIterator, IntoDistributedIterator};
use crate::pool::ProcessSend;

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
		self.0.next().map(IterIterConsumer)
	}
}

#[derive(Serialize, Deserialize)]
pub struct IterIterConsumer<T>(T);

impl<T> Consumer for IterIterConsumer<T> {
	type Item = T;

	fn run(self, i: &mut impl FnMut(Self::Item) -> bool) -> bool {
		i(self.0)
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
