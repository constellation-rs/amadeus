mod cloudfront;
mod common_crawl;
pub mod csv;
pub mod json;
mod misc_serde;
pub mod parquet;
pub mod postgres;
pub use self::{
	cloudfront::*, common_crawl::*, csv::Csv, json::Json, parquet::Parquet, postgres::Postgres
};

pub trait Source {
	type Item: super::data::Data;
	type Error: std::error::Error;

	type DistIter: super::DistributedIterator<Item = Result<Self::Item, Self::Error>>;
	// type ParIter: ParallelIterator;
	type Iter: Iterator<Item = Result<Self::Item, Self::Error>>;

	fn dist_iter(self) -> Self::DistIter;
	// fn par_iter(self) -> Self::ParIter;
	fn iter(self) -> Self::Iter;
}

pub trait Sink<I>
where
	I: super::dist_iter::DistributedIteratorMulti<Self::Item>,
{
	type Item: super::data::Data;
	type Error: std::error::Error;

	type DistDest: super::dist_iter::DistributedReducer<I, Self::Item, Result<(), Self::Error>>;
}

pub struct ResultExpand<T, E>(Result<T, E>); // TODO: unpub
impl<T, E> IntoIterator for ResultExpand<T, E>
where
	T: IntoIterator,
{
	type Item = Result<T::Item, E>;
	type IntoIter = ResultExpandIter<T::IntoIter, E>;
	fn into_iter(self) -> Self::IntoIter {
		ResultExpandIter(self.0.map(IntoIterator::into_iter).map_err(Some))
	}
}
pub struct ResultExpandIter<T, E>(Result<T, Option<E>>);
impl<T, E> Iterator for ResultExpandIter<T, E>
where
	T: Iterator,
{
	type Item = Result<T::Item, E>;
	fn next(&mut self) -> Option<Self::Item> {
		transpose(self.0.as_mut().map(Iterator::next).map_err(Option::take))
	}
}
fn transpose<T, E>(result: Result<Option<T>, Option<E>>) -> Option<Result<T, E>> {
	match result {
		Ok(Some(x)) => Some(Ok(x)),
		Err(Some(e)) => Some(Err(e)),
		Ok(None) | Err(None) => None,
	}
}
