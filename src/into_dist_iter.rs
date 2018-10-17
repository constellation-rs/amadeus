use dist_iter::*;
use std::{slice, vec};

pub trait IntoDistributedIterator {
	type Iter: DistributedIterator<Item = Self::Item>;
	type Item;
	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized;
	fn dist_iter_mut(&mut self) -> <&mut Self as IntoDistributedIterator>::Iter
	where
		for<'a> &'a mut Self: IntoDistributedIterator,
	{
		<&mut Self as IntoDistributedIterator>::into_dist_iter(self)
	}
	fn dist_iter(&self) -> <&Self as IntoDistributedIterator>::Iter
	where
		for<'a> &'a Self: IntoDistributedIterator,
	{
		<&Self as IntoDistributedIterator>::into_dist_iter(self)
	}
}
impl<T> IntoDistributedIterator for Vec<T> {
	type Iter = IntoIter<T>;
	type Item = T;
	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IntoIter(self.into_iter())
	}
}
impl<'a, T: Clone> IntoDistributedIterator for &'a Vec<T> {
	type Iter = IterRef<'a, T>;
	type Item = T;
	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterRef(self.iter())
	}
}
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
impl DistributedIterator for Never {
	type Item = Never;
	type Task = Never;
	fn size_hint(&self) -> (usize, Option<usize>) {
		unreachable!()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		unreachable!()
	}
}
impl<'a, T: Clone> IntoDistributedIterator for &'a [T] {
	type Iter = IterRef<'a, T>;
	type Item = T;
	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterRef(self.iter())
	}
}

pub enum Never {}
impl Consumer for Never {
	type Item = Never;
	fn run(self, _: &mut impl FnMut(Self::Item)) {
		unreachable!()
	}
}

impl<T: DistributedIterator> IntoDistributedIterator for T {
	type Iter = T;
	type Item = T::Item;
	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		self
	}
}

pub trait IteratorExt: Iterator + Sized {
	fn dist(self) -> IterIter<Self> {
		IterIter(self)
	}
}
impl<I: Iterator + Sized> IteratorExt for I {}

pub struct IterIter<I>(I);
impl<I: Iterator> DistributedIterator for IterIter<I> {
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
	fn run(self, i: &mut impl FnMut(Self::Item)) {
		i(self.0)
	}
}

pub struct IntoIter<T>(vec::IntoIter<T>);
impl<T> DistributedIterator for IntoIter<T> {
	type Item = T;
	type Task = IntoIterConsumer<T>;
	fn size_hint(&self) -> (usize, Option<usize>) {
		(self.0.len(), Some(self.0.len()))
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.0.next().map(IntoIterConsumer)
	}
}
#[derive(Serialize, Deserialize)]
pub struct IntoIterConsumer<T>(T);
impl<T> Consumer for IntoIterConsumer<T> {
	type Item = T;
	fn run(self, i: &mut impl FnMut(Self::Item)) {
		i(self.0)
	}
}

pub struct IterRef<'a, T: 'a>(slice::Iter<'a, T>);
impl<'a, T: Clone + 'a> DistributedIterator for IterRef<'a, T> {
	type Item = T;
	type Task = IterRefConsumer<T>;
	fn size_hint(&self) -> (usize, Option<usize>) {
		(self.0.len(), Some(self.0.len()))
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.0.next().cloned().map(IterRefConsumer)
	}
}
#[derive(Serialize, Deserialize)]
pub struct IterRefConsumer<T>(T);
impl<T> Consumer for IterRefConsumer<T> {
	type Item = T;
	fn run(self, i: &mut impl FnMut(Self::Item)) {
		i(self.0)
	}
}
