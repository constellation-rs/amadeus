pub trait Source {
	type Item; //: crate::data::Data;
	type Error: std::error::Error;

	type DistStream: crate::dist_stream::DistributedStream<Item = Result<Self::Item, Self::Error>>;
	// type ParIter: ParallelIterator;
	type Iter: Iterator<Item = Result<Self::Item, Self::Error>>;

	fn dist_stream(self) -> Self::DistStream;
	// fn par_iter(self) -> Self::ParIter;
	fn iter(self) -> Self::Iter;
}

pub trait Destination<I>
where
	I: crate::dist_stream::DistributedStreamMulti<Self::Item>,
{
	type Item; //: crate::data::Data;
	type Error: std::error::Error;

	type DistDest: crate::dist_stream::DistributedReducer<I, Self::Item, Result<(), Self::Error>>;
}
