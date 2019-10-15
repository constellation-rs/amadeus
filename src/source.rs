use ::serde::{Deserialize, Serialize};

#[cfg(feature = "aws")]
#[doc(inline)]
pub use amadeus_aws::Cloudfront;
#[cfg(feature = "aws")]
pub mod aws {
	pub use crate::data::CloudfrontRow;
	#[doc(inline)]
	pub use amadeus_aws::{AwsError, AwsRegion, S3Directory, S3File};
}
#[cfg(feature = "commoncrawl")]
#[doc(inline)]
pub use amadeus_commoncrawl::{self as commoncrawl, CommonCrawl};
#[cfg(feature = "parquet")]
#[doc(inline)]
pub use amadeus_parquet::{self as parquet, Parquet, ParquetDirectory};
#[cfg(feature = "postgres")]
#[doc(inline)]
pub use amadeus_postgres::{self as postgres, Postgres};
#[cfg(feature = "amadeus-serde")]
#[doc(inline)]
pub use amadeus_serde::{self as serde, Csv, Json};

#[cfg(feature = "amadeus-serde")]
impl<File, Row> Source for Json<File, Row>
where
	File: amadeus_core::file::File,
	Row: super::data::Data,
{
	type Item = <Self as amadeus_core::Source>::Item;
	type Error = <Self as amadeus_core::Source>::Error;

	type DistIter = <Self as amadeus_core::Source>::DistIter;
	type Iter = <Self as amadeus_core::Source>::Iter;

	fn dist_iter(self) -> Self::DistIter {
		<Self as amadeus_core::Source>::dist_iter(self)
	}
	fn iter(self) -> Self::Iter {
		<Self as amadeus_core::Source>::iter(self)
	}
}
#[cfg(feature = "amadeus-serde")]
impl<File, Row> Source for Csv<File, Row>
where
	File: amadeus_core::file::File,
	Row: super::data::Data,
{
	type Item = <Self as amadeus_core::Source>::Item;
	type Error = <Self as amadeus_core::Source>::Error;

	type DistIter = <Self as amadeus_core::Source>::DistIter;
	type Iter = <Self as amadeus_core::Source>::Iter;

	fn dist_iter(self) -> Self::DistIter {
		<Self as amadeus_core::Source>::dist_iter(self)
	}
	fn iter(self) -> Self::Iter {
		<Self as amadeus_core::Source>::iter(self)
	}
}
#[cfg(feature = "parquet")]
impl<File, Row> Source for Parquet<File, Row>
where
	File: amadeus_core::file::File,
	Row: super::data::Data,
{
	type Item = <Self as amadeus_core::Source>::Item;
	type Error = <Self as amadeus_core::Source>::Error;

	type DistIter = <Self as amadeus_core::Source>::DistIter;
	type Iter = <Self as amadeus_core::Source>::Iter;

	fn dist_iter(self) -> Self::DistIter {
		<Self as amadeus_core::Source>::dist_iter(self)
	}
	fn iter(self) -> Self::Iter {
		<Self as amadeus_core::Source>::iter(self)
	}
}
#[cfg(feature = "postgres")]
impl<Row> Source for Postgres<Row>
where
	Row: super::data::Data,
{
	type Item = <Self as amadeus_core::Source>::Item;
	type Error = <Self as amadeus_core::Source>::Error;

	type DistIter = <Self as amadeus_core::Source>::DistIter;
	type Iter = <Self as amadeus_core::Source>::Iter;

	fn dist_iter(self) -> Self::DistIter {
		<Self as amadeus_core::Source>::dist_iter(self)
	}
	fn iter(self) -> Self::Iter {
		<Self as amadeus_core::Source>::iter(self)
	}
}
#[cfg(feature = "aws")]
impl Source for Cloudfront {
	type Item = crate::data::CloudfrontRow;
	type Error = <Self as amadeus_core::Source>::Error;

	type DistIter = IntoIter<<Self as amadeus_core::Source>::DistIter, Self::Item>;
	type Iter = IntoIter<<Self as amadeus_core::Source>::Iter, Self::Item>;

	fn dist_iter(self) -> Self::DistIter {
		IntoIter(<Self as amadeus_core::Source>::dist_iter(self), PhantomData)
	}
	fn iter(self) -> Self::Iter {
		IntoIter(<Self as amadeus_core::Source>::iter(self), PhantomData)
	}
}
#[cfg(feature = "commoncrawl")]
impl Source for CommonCrawl {
	type Item = amadeus_types::Webpage<'static>;
	type Error = <Self as amadeus_core::Source>::Error;

	type DistIter = IntoIter<<Self as amadeus_core::Source>::DistIter, Self::Item>;
	type Iter = IntoIter<<Self as amadeus_core::Source>::Iter, Self::Item>;

	fn dist_iter(self) -> Self::DistIter {
		IntoIter(<Self as amadeus_core::Source>::dist_iter(self), PhantomData)
	}
	fn iter(self) -> Self::Iter {
		IntoIter(<Self as amadeus_core::Source>::iter(self), PhantomData)
	}
}
use std::marker::PhantomData;
pub struct IntoIter<I, U>(I, PhantomData<fn() -> U>);
impl<I, T, E, U> crate::dist_iter::DistributedIterator for IntoIter<I, U>
where
	I: crate::dist_iter::DistributedIterator<Item = Result<T, E>>,
	T: Into<U>,
	U: 'static,
{
	type Item = Result<U, E>;
	type Task = IntoConsumer<I::Task, U>;

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.0.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.0.next_task().map(|task| IntoConsumer {
			task,
			marker: PhantomData,
		})
	}
}
#[derive(Serialize, Deserialize)]
pub struct IntoConsumer<I, U> {
	task: I,
	marker: PhantomData<fn() -> U>,
}
impl<I, T, E, U> amadeus_core::dist_iter::Consumer for IntoConsumer<I, U>
where
	I: amadeus_core::dist_iter::Consumer<Item = Result<T, E>>,
	T: Into<U>,
{
	type Item = Result<U, E>;

	fn run(self, i: &mut impl FnMut(Self::Item) -> bool) -> bool {
		self.task.run(&mut |item| i(item.map(Into::into)))
	}
}
impl<I, T, E, U> Iterator for IntoIter<I, U>
where
	I: Iterator<Item = Result<T, E>>,
	T: Into<U>,
{
	type Item = Result<U, E>;

	fn next(&mut self) -> Option<Self::Item> {
		self.0.next().map(|x| x.map(Into::into))
	}
}

pub trait Source {
	type Item: crate::data::Data;
	type Error: std::error::Error;

	type DistIter: crate::dist_iter::DistributedIterator<Item = Result<Self::Item, Self::Error>>;
	// type ParIter: ParallelIterator;
	type Iter: Iterator<Item = Result<Self::Item, Self::Error>>;

	fn dist_iter(self) -> Self::DistIter;
	// fn par_iter(self) -> Self::ParIter;
	fn iter(self) -> Self::Iter;
}

pub trait Sink<I>
where
	I: crate::dist_iter::DistributedIteratorMulti<Self::Item>,
{
	type Item: crate::data::Data;
	type Error: std::error::Error;

	type DistDest: crate::dist_iter::DistributedReducer<I, Self::Item, Result<(), Self::Error>>;
}
