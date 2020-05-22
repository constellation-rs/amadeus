#![allow(clippy::type_complexity)]

use futures::{ready, Stream};
use pin_project::pin_project;
use rand::thread_rng;
use serde::{Deserialize, Serialize};
use std::{
	hash::Hash, marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedIteratorMulti, DistributedReducer, ReduceFactory, Reducer, ReducerA, ReducerAsync, SumReducer
};
use crate::pool::ProcessSend;

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
	DistributedReducer<I, Source, streaming_algorithms::SampleUnstable<I::Item>> for SampleUnstable<I>
where
	I::Item: ProcessSend,
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
			SampleUnstableReducerFactory(self.samples, PhantomData),
			SumReducer::new(streaming_algorithms::SampleUnstable::new(self.samples)),
		)
	}
}

pub struct SampleUnstableReducerFactory<A>(usize, PhantomData<fn(A)>);

impl<A> ReduceFactory for SampleUnstableReducerFactory<A> {
	type Reducer = SampleUnstableReducer<A>;
	fn make(&self) -> Self::Reducer {
		SampleUnstableReducer(Some(streaming_algorithms::SampleUnstable::new(self.0)))
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct SampleUnstableReducer<A>(Option<streaming_algorithms::SampleUnstable<A>>);

impl<A> Reducer for SampleUnstableReducer<A> {
	type Item = A;
	type Output = streaming_algorithms::SampleUnstable<A>;
	type Async = Self;

	fn into_async(self) -> Self::Async {
		self
	}
}
impl<A> ReducerAsync for SampleUnstableReducer<A> {
	type Item = A;
	type Output = streaming_algorithms::SampleUnstable<A>;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		let self_0 = self_.0.as_mut().unwrap();
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			self_0.push(item, &mut thread_rng());
		}
		Poll::Ready(())
	}
	fn poll_output(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
		Poll::Ready(self.project().0.take().unwrap())
	}
}
impl<A> ReducerA for SampleUnstableReducer<A>
where
	A: ProcessSend,
{
	type Output = streaming_algorithms::SampleUnstable<A>;
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct NonzeroReducer<R>(#[pin] R);

impl<R, B> Reducer for NonzeroReducer<R>
where
	R: Reducer<Output = streaming_algorithms::Zeroable<B>>,
{
	type Item = R::Item;
	type Output = B;
	type Async = NonzeroReducer<R::Async>;

	fn into_async(self) -> Self::Async {
		NonzeroReducer(self.0.into_async())
	}
}
impl<R, B> ReducerAsync for NonzeroReducer<R>
where
	R: ReducerAsync<Output = streaming_algorithms::Zeroable<B>>,
{
	type Item = R::Item;
	type Output = B;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		self.project().0.poll_forward(cx, stream)
	}
	fn poll_output(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		self.project()
			.0
			.poll_output(cx)
			.map(|item| item.nonzero().unwrap())
	}
}
impl<R, B> ReducerA for NonzeroReducer<R>
where
	R: Reducer<Output = streaming_algorithms::Zeroable<B>> + ProcessSend,
	B: ProcessSend,
{
	type Output = B;
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
	I::Item: Clone + Hash + Eq + ProcessSend,
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
			MostFrequentReducerFactory(self.n, self.probability, self.tolerance, PhantomData),
			NonzeroReducer(SumReducer::new(streaming_algorithms::Zeroable::Nonzero(
				streaming_algorithms::Top::new(self.n, self.probability, self.tolerance, ()),
			))),
		)
	}
}

pub struct MostFrequentReducerFactory<A>(usize, f64, f64, PhantomData<fn(A)>);

impl<A> ReduceFactory for MostFrequentReducerFactory<A>
where
	A: Clone + Hash + Eq,
{
	type Reducer = MostFrequentReducer<A>;
	fn make(&self) -> Self::Reducer {
		MostFrequentReducer(Some(streaming_algorithms::Top::new(
			self.0,
			self.1,
			self.2,
			(),
		)))
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
#[serde(bound(
	serialize = "A: Hash + Eq + Serialize",
	deserialize = "A: Hash + Eq + Deserialize<'de>"
))]
pub struct MostFrequentReducer<A>(Option<streaming_algorithms::Top<A, usize>>);

impl<A> Reducer for MostFrequentReducer<A>
where
	A: Clone + Hash + Eq,
{
	type Item = A;
	type Output = streaming_algorithms::Top<A, usize>;
	type Async = Self;

	fn into_async(self) -> Self::Async {
		self
	}
}
impl<A> ReducerAsync for MostFrequentReducer<A>
where
	A: Clone + Hash + Eq,
{
	type Item = A;
	type Output = streaming_algorithms::Top<A, usize>;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		let self_0 = self_.0.as_mut().unwrap();
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			self_0.push(item, &1);
		}
		Poll::Ready(())
	}
	fn poll_output(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
		Poll::Ready(self.project().0.take().unwrap())
	}
}
impl<A> ReducerA for MostFrequentReducer<A>
where
	A: Clone + Hash + Eq + ProcessSend,
{
	type Output = streaming_algorithms::Top<A, usize>;
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
	A: Clone + Hash + Eq + ProcessSend,
	B: Hash + 'static,
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
				PhantomData,
			),
			NonzeroReducer(SumReducer::new(streaming_algorithms::Zeroable::Nonzero(
				streaming_algorithms::Top::new(
					self.n,
					self.probability,
					self.tolerance,
					self.error_rate,
				),
			))),
		)
	}
}

pub struct MostDistinctReducerFactory<A, B>(usize, f64, f64, f64, PhantomData<fn(A, B)>);

impl<A, B> ReduceFactory for MostDistinctReducerFactory<A, B>
where
	A: Clone + Hash + Eq,
	B: Hash,
{
	type Reducer = MostDistinctReducer<A, B>;
	fn make(&self) -> Self::Reducer {
		MostDistinctReducer(Some(streaming_algorithms::Top::new(
			self.0, self.1, self.2, self.3,
		)))
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
#[serde(bound(
	serialize = "A: Hash + Eq + Serialize",
	deserialize = "A: Hash + Eq + Deserialize<'de>"
))]
pub struct MostDistinctReducer<A, B: Hash>(
	Option<streaming_algorithms::Top<A, streaming_algorithms::HyperLogLogMagnitude<B>>>,
);

impl<A, B> Reducer for MostDistinctReducer<A, B>
where
	A: Clone + Hash + Eq,
	B: Hash,
{
	type Item = (A, B);
	type Output = streaming_algorithms::Top<A, streaming_algorithms::HyperLogLogMagnitude<B>>;
	type Async = Self;

	fn into_async(self) -> Self::Async {
		self
	}
}
impl<A, B> ReducerAsync for MostDistinctReducer<A, B>
where
	A: Clone + Hash + Eq,
	B: Hash,
{
	type Item = (A, B);
	type Output = streaming_algorithms::Top<A, streaming_algorithms::HyperLogLogMagnitude<B>>;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		let self_0 = self_.0.as_mut().unwrap();
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			self_0.push(item.0, &item.1);
		}
		Poll::Ready(())
	}
	fn poll_output(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
		Poll::Ready(self.project().0.take().unwrap())
	}
}
impl<A, B> ReducerA for MostDistinctReducer<A, B>
where
	A: Clone + Hash + Eq + ProcessSend,
	B: Hash + 'static,
{
	type Output = streaming_algorithms::Top<A, streaming_algorithms::HyperLogLogMagnitude<B>>;
}
