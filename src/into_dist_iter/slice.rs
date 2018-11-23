use super::{Consumer, DistributedIterator, IntoDistributedIterator, IterIter};
use std::{iter, slice};

impl<T> IntoDistributedIterator for [T] {
	type Iter = Never;
	type Item = Never;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		unreachable!()
	}
}

impl<'a, T: Clone> IntoDistributedIterator for &'a [T] {
	type Iter = IterIter<iter::Cloned<slice::Iter<'a, T>>>;
	type Item = T;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.iter().cloned())
	}
}

pub struct Never(!);

impl DistributedIterator for Never {
	type Item = Self;
	type Task = Self;

	fn size_hint(&self) -> (usize, Option<usize>) {
		unreachable!()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		unreachable!()
	}
}

impl Consumer for Never {
	type Item = Self;

	fn run(self, _: &mut impl FnMut(Self::Item)) {
		unreachable!()
	}
}
