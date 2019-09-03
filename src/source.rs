#[doc(inline)]
pub use amadeus_aws::{self as aws, Cloudfront};
#[doc(inline)]
pub use amadeus_commoncrawl::{self as commoncrawl, CommonCrawl};
#[doc(inline)]
pub use amadeus_parquet::{self as parquet, Parquet};
#[doc(inline)]
pub use amadeus_postgres::{self as postgres, Postgres};
#[doc(inline)]
pub use amadeus_serde::{self as serde, Csv, Json};

impl<Row> Source for Json<Row>
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
impl<Row> Source for Csv<Row>
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

// impl Source for Parquet {
// 	type Item =

pub trait Sink<I>
where
	I: crate::dist_iter::DistributedIteratorMulti<Self::Item>,
{
	type Item: crate::data::Data;
	type Error: std::error::Error;

	type DistDest: crate::dist_iter::DistributedReducer<I, Self::Item, Result<(), Self::Error>>;
}
