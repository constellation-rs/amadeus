use super::{DistributedIteratorMulti, DistributedReducer, ReduceFactory, Reducer, SumReducer};
use rand::thread_rng;
use serde::{de::Deserialize, ser::Serialize};
use std::marker;

use std::hash::Hash;

#[must_use]
pub struct SampleUnstable<I> {
	i: I,
	samples: usize,
}
impl<I> SampleUnstable<I> {
	pub(super) fn new(i: I, samples: usize) -> Self {
		Self { i, samples }
	}
}

impl<I: DistributedIteratorMulti<Source>, Source>
	DistributedReducer<I, Source, streaming_algorithms::SampleUnstable<I::Item>>
	for SampleUnstable<I>
{
	type ReduceAFactory = SampleUnstableReducerFactory<I::Item>;
	type ReduceA = SampleUnstableReducer<I::Item>;
	type ReduceB = SumReducer<
		streaming_algorithms::SampleUnstable<I::Item>,
		streaming_algorithms::SampleUnstable<I::Item>,
	>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			SampleUnstableReducerFactory(self.samples, marker::PhantomData),
			SumReducer(
				streaming_algorithms::SampleUnstable::new(self.samples),
				marker::PhantomData,
			),
		)
	}
}

pub struct SampleUnstableReducerFactory<A>(usize, marker::PhantomData<fn(A)>);

impl<A> ReduceFactory for SampleUnstableReducerFactory<A> {
	type Reducer = SampleUnstableReducer<A>;
	fn make(&self) -> Self::Reducer {
		SampleUnstableReducer(streaming_algorithms::SampleUnstable::new(self.0))
	}
}

#[derive(Serialize, Deserialize)]
pub struct SampleUnstableReducer<A>(streaming_algorithms::SampleUnstable<A>);

impl<A> Reducer for SampleUnstableReducer<A> {
	type Item = A;
	type Output = streaming_algorithms::SampleUnstable<A>;

	#[inline(always)]
	fn push(&mut self, item: Self::Item) -> bool {
		self.0.push(item, &mut thread_rng());
		true
	}
	fn ret(self) -> Self::Output {
		self.0
	}
}

#[derive(Serialize, Deserialize)]
pub struct NonzeroReducer<R>(R);

impl<R, B> Reducer for NonzeroReducer<R>
where
	R: Reducer<Output = streaming_algorithms::Zeroable<B>>,
{
	type Item = R::Item;
	type Output = B;

	#[inline(always)]
	fn push(&mut self, item: Self::Item) -> bool {
		self.0.push(item)
	}
	fn ret(self) -> Self::Output {
		self.0.ret().nonzero().unwrap()
	}
}

#[must_use]
pub struct MostFrequent<I> {
	i: I,
	n: usize,
	probability: f64,
	tolerance: f64,
}
impl<I> MostFrequent<I> {
	pub(super) fn new(i: I, n: usize, probability: f64, tolerance: f64) -> Self {
		Self {
			i,
			n,
			probability,
			tolerance,
		}
	}
}

impl<I: DistributedIteratorMulti<Source>, Source>
	DistributedReducer<I, Source, streaming_algorithms::Top<I::Item, usize>> for MostFrequent<I>
where
	I::Item: Clone + Hash + Eq,
{
	type ReduceAFactory = MostFrequentReducerFactory<I::Item>;
	type ReduceA = MostFrequentReducer<I::Item>;
	type ReduceB = NonzeroReducer<
		SumReducer<
			streaming_algorithms::Top<I::Item, usize>,
			streaming_algorithms::Zeroable<streaming_algorithms::Top<I::Item, usize>>,
		>,
	>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			MostFrequentReducerFactory(
				self.n,
				self.probability,
				self.tolerance,
				marker::PhantomData,
			),
			NonzeroReducer(SumReducer(
				streaming_algorithms::Zeroable::Nonzero(streaming_algorithms::Top::new(
					self.n,
					self.probability,
					self.tolerance,
					(),
				)),
				marker::PhantomData,
			)),
		)
	}
}

pub struct MostFrequentReducerFactory<A>(usize, f64, f64, marker::PhantomData<fn(A)>);

impl<A> ReduceFactory for MostFrequentReducerFactory<A>
where
	A: Clone + Hash + Eq,
{
	type Reducer = MostFrequentReducer<A>;
	fn make(&self) -> Self::Reducer {
		MostFrequentReducer(streaming_algorithms::Top::new(self.0, self.1, self.2, ()))
	}
}

#[derive(Serialize, Deserialize)]
#[serde(bound(
	serialize = "A: Hash + Eq + Serialize",
	deserialize = "A: Hash + Eq + Deserialize<'de>"
))]
pub struct MostFrequentReducer<A>(streaming_algorithms::Top<A, usize>);

impl<A> Reducer for MostFrequentReducer<A>
where
	A: Clone + Hash + Eq,
{
	type Item = A;
	type Output = streaming_algorithms::Top<A, usize>;

	#[inline(always)]
	fn push(&mut self, item: Self::Item) -> bool {
		self.0.push(item, &1);
		true
	}
	fn ret(self) -> Self::Output {
		self.0
	}
}

#[must_use]
pub struct MostDistinct<I> {
	i: I,
	n: usize,
	probability: f64,
	tolerance: f64,
	error_rate: f64,
}
impl<I> MostDistinct<I> {
	pub(super) fn new(i: I, n: usize, probability: f64, tolerance: f64, error_rate: f64) -> Self {
		Self {
			i,
			n,
			probability,
			tolerance,
			error_rate,
		}
	}
}

impl<I: DistributedIteratorMulti<Source, Item = (A, B)>, Source, A, B>
	DistributedReducer<
		I,
		Source,
		streaming_algorithms::Top<A, streaming_algorithms::HyperLogLogMagnitude<B>>,
	> for MostDistinct<I>
where
	A: Clone + Hash + Eq,
	B: Hash,
{
	type ReduceAFactory = MostDistinctReducerFactory<A, B>;
	type ReduceA = MostDistinctReducer<A, B>;
	type ReduceB = NonzeroReducer<
		SumReducer<
			streaming_algorithms::Top<A, streaming_algorithms::HyperLogLogMagnitude<B>>,
			streaming_algorithms::Zeroable<
				streaming_algorithms::Top<A, streaming_algorithms::HyperLogLogMagnitude<B>>,
			>,
		>,
	>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			MostDistinctReducerFactory(
				self.n,
				self.probability,
				self.tolerance,
				self.error_rate,
				marker::PhantomData,
			),
			NonzeroReducer(SumReducer(
				streaming_algorithms::Zeroable::Nonzero(streaming_algorithms::Top::new(
					self.n,
					self.probability,
					self.tolerance,
					self.error_rate,
				)),
				marker::PhantomData,
			)),
		)
	}
}

pub struct MostDistinctReducerFactory<A, B>(usize, f64, f64, f64, marker::PhantomData<fn(A, B)>);

impl<A, B> ReduceFactory for MostDistinctReducerFactory<A, B>
where
	A: Clone + Hash + Eq,
	B: Hash,
{
	type Reducer = MostDistinctReducer<A, B>;
	fn make(&self) -> Self::Reducer {
		MostDistinctReducer(streaming_algorithms::Top::new(
			self.0, self.1, self.2, self.3,
		))
	}
}

#[derive(Serialize, Deserialize)]
#[serde(bound(
	serialize = "A: Hash + Eq + Serialize",
	deserialize = "A: Hash + Eq + Deserialize<'de>"
))]
pub struct MostDistinctReducer<A, B: Hash>(
	streaming_algorithms::Top<A, streaming_algorithms::HyperLogLogMagnitude<B>>,
);

impl<A, B> Reducer for MostDistinctReducer<A, B>
where
	A: Clone + Hash + Eq,
	B: Hash,
{
	type Item = (A, B);
	type Output = streaming_algorithms::Top<A, streaming_algorithms::HyperLogLogMagnitude<B>>;

	#[inline(always)]
	fn push(&mut self, item: Self::Item) -> bool {
		self.0.push(item.0, &item.1);
		true
	}
	fn ret(self) -> Self::Output {
		self.0
	}
}
