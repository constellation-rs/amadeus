use serde::{Deserialize, Serialize};

use crate::dist_iter::{Consumer, DistributedIterator};

mod collections;
mod iterator;
mod slice;
pub use self::{collections::*, iterator::*, slice::*};

pub trait IntoDistributedIterator {
	type Iter: DistributedIterator<Item = Self::Item>;
	type Item; //: Serialize + for<'de> Deserialize<'de> + 'static;
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

impl<T: DistributedIterator> IntoDistributedIterator for T {
	type Iter = Self;
	type Item = <Self as DistributedIterator>::Item;
	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		self
	}
}
