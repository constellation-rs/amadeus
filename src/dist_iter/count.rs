use super::{
	DistributedIteratorMulti, DistributedReducer, ReduceFactory, Reducer, ReducerA, SumReducer
};
use std::marker::PhantomData;

#[must_use]
pub struct Count<I> {
	i: I,
}
impl<I> Count<I> {
	pub(super) fn new(i: I) -> Self {
		Self { i }
	}
}

impl<I: DistributedIteratorMulti<Source>, Source> DistributedReducer<I, Source, usize> for Count<I>
where
	I::Item: 'static,
{
	type ReduceAFactory = CountReducerFactory<I::Item>;
	type ReduceA = CountReducer<I::Item>;
	type ReduceB = SumReducer<usize, usize>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			CountReducerFactory(PhantomData),
			SumReducer(0, PhantomData),
		)
	}
}

pub struct CountReducerFactory<A>(PhantomData<fn(A)>);

impl<A> ReduceFactory for CountReducerFactory<A> {
	type Reducer = CountReducer<A>;
	fn make(&self) -> Self::Reducer {
		CountReducer(0, PhantomData)
	}
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct CountReducer<A>(usize, PhantomData<fn(A)>);

impl<A> Reducer for CountReducer<A> {
	type Item = A;
	type Output = usize;

	#[inline(always)]
	fn push(&mut self, _item: Self::Item) -> bool {
		self.0 += 1;
		true
	}
	fn ret(self) -> Self::Output {
		self.0
	}
}
impl<A> ReducerA for CountReducer<A>
where
	A: 'static,
{
	type Output = usize;
}
