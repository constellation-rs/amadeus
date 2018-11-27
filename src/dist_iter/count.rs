use super::{DistributedIteratorMulti, DistributedReducer, ReduceFactory, Reducer, SumReducer};
use std::marker;

#[must_use]
pub struct Count<I> {
	i: I,
}
impl<I> Count<I> {
	pub(super) fn new(i: I) -> Self {
		Self { i }
	}
}

impl<I: DistributedIteratorMulti<Source>, Source> DistributedReducer<I, Source, usize>
	for Count<I>
{
	type ReduceAFactory = CountReducerFactory<I::Item>;
	type ReduceA = CountReducer<I::Item>;
	type ReduceB = SumReducer<usize, usize>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			CountReducerFactory(marker::PhantomData),
			SumReducer(0, marker::PhantomData),
		)
	}
}

pub struct CountReducerFactory<A>(marker::PhantomData<fn(A)>);

impl<A> ReduceFactory for CountReducerFactory<A> {
	type Reducer = CountReducer<A>;
	fn make(&self) -> Self::Reducer {
		CountReducer(0, marker::PhantomData)
	}
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct CountReducer<A>(usize, marker::PhantomData<fn(A)>);

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
