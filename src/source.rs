mod cloudfront;
mod common_crawl;
pub mod csv;
pub mod json;
pub mod parquet;
pub mod postgres;
pub use self::{
	cloudfront::*, common_crawl::*, csv::Csv, json::Json, parquet::Parquet, postgres::Postgres
};

pub trait Source {
	type Item: super::data::Data;
	type DistIter: super::DistributedIterator<Item = Self::Item>;
	// type ParIter: ParallelIterator;
	type Iter: Iterator<Item = Self::Item>;

	fn dist_iter(self) -> Self::DistIter;
	// fn par_iter(self) -> Self::ParIter;
	fn iter(self) -> Self::Iter;
}

struct ResultExpand<T, E>(Result<T, E>);
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
struct ResultExpandIter<T, E>(Result<T, Option<E>>);
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
