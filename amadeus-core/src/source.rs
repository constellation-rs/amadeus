pub trait Source {
	type Item; //: crate::data::Data;
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
	type Item; //: crate::data::Data;
	type Error: std::error::Error;

	type DistDest: crate::dist_iter::DistributedReducer<I, Self::Item, Result<(), Self::Error>>;
}
