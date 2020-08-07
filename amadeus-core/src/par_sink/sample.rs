#![allow(clippy::type_complexity)]

use amadeus_streaming::{
	HyperLogLogMagnitude, SampleUnstable as SASampleUnstable, Sort as SASort, Top
};
use derive_new::new;
use rand::thread_rng;
use serde::{Deserialize, Serialize};
use serde_closure::traits;
use std::{cmp::Ordering, hash::Hash};

use super::{
	folder_par_sink, FolderSync, FolderSyncReducer, ParallelPipe, ParallelSink, SumFolder, SumZeroFolder
};

#[derive(new)]
#[must_use]
pub struct SampleUnstable<P> {
	pipe: P,
	samples: usize,
}

impl_par_dist! {
	impl<P: ParallelPipe<Item>, Item> ParallelSink<Item> for SampleUnstable<P>
	where
		P::Output: Send + 'static,
	{
		folder_par_sink!(
			SampleUnstableFolder,
			SumFolder<SASampleUnstable<P::Output>>,
			self,
			SampleUnstableFolder::new(self.samples),
			SumFolder::new()
		);
	}
}

#[derive(Clone, Serialize, Deserialize, new)]
pub struct SampleUnstableFolder {
	samples: usize,
}

impl<Item> FolderSync<Item> for SampleUnstableFolder {
	type State = SASampleUnstable<Item>;
	type Done = Self::State;

	fn zero(&mut self) -> Self::State {
		SASampleUnstable::new(self.samples)
	}
	fn push(&mut self, state: &mut Self::State, item: Item) {
		state.push(item, &mut thread_rng())
	}
	fn done(&mut self, state: Self::State) -> Self::Done {
		state
	}
}

#[derive(new)]
#[must_use]
pub struct Sort<P, F> {
	pipe: P,
	f: F,
	n: usize,
}

impl_par_dist! {
	#[cfg_attr(not(nightly), serde_closure::desugar)]
	impl<P: ParallelPipe<Item>, F, Item> ParallelSink<Item> for Sort<P, F>
	where
		F: traits::Fn(&P::Output, &P::Output) -> Ordering + Clone + Send + 'static,
		P::Output: Clone + Send + 'static,
	{
		folder_par_sink!(
			SortFolder<F>,
			SumZeroFolder<SASort<P::Output, F>>,
			self,
			SortFolder::new(self.f.clone(), self.n),
			SumZeroFolder::new(SASort::new(self.f, self.n))
		);
	}
}

#[derive(Clone, Serialize, Deserialize, new)]
pub struct SortFolder<F> {
	f: F,
	n: usize,
}

#[cfg_attr(not(nightly), serde_closure::desugar)]
impl<Item, F> FolderSync<Item> for SortFolder<F>
where
	F: traits::Fn(&Item, &Item) -> Ordering + Clone,
{
	type State = SASort<Item, F>;
	type Done = Self::State;

	fn zero(&mut self) -> Self::Done {
		SASort::new(self.f.clone(), self.n)
	}
	fn push(&mut self, state: &mut Self::Done, item: Item) {
		state.push(item)
	}
	fn done(&mut self, state: Self::State) -> Self::Done {
		state
	}
}

#[derive(new)]
#[must_use]
pub struct MostFrequent<P> {
	pipe: P,
	n: usize,
	probability: f64,
	tolerance: f64,
}

impl_par_dist! {
	impl<P: ParallelPipe<Item>, Item> ParallelSink<Item> for MostFrequent<P>
	where
		P::Output: Clone + Hash + Eq + Send + 'static,
	{
		folder_par_sink!(
			MostFrequentFolder,
			SumZeroFolder<Top<P::Output, usize>>,
			self,
			MostFrequentFolder::new(self.n, self.probability, self.tolerance),
			SumZeroFolder::new(Top::new(self.n, self.probability, self.tolerance, ()))
		);
	}
}

#[derive(Clone, Serialize, Deserialize, new)]
pub struct MostFrequentFolder {
	n: usize,
	probability: f64,
	tolerance: f64,
}

impl<Item> FolderSync<Item> for MostFrequentFolder
where
	Item: Clone + Hash + Eq + Send + 'static,
{
	type State = Top<Item, usize>;
	type Done = Self::State;

	fn zero(&mut self) -> Self::State {
		Top::new(self.n, self.probability, self.tolerance, ())
	}
	fn push(&mut self, state: &mut Self::State, item: Item) {
		state.push(item, &1)
	}
	fn done(&mut self, state: Self::State) -> Self::Done {
		state
	}
}

#[derive(new)]
#[must_use]
pub struct MostDistinct<P> {
	pipe: P,
	n: usize,
	probability: f64,
	tolerance: f64,
	error_rate: f64,
}

impl_par_dist! {
	impl<P: ParallelPipe<Item, Output = (A, B)>, Item, A, B> ParallelSink<Item> for MostDistinct<P>
	where
		A: Clone + Hash + Eq + Send + 'static,
		B: Hash + 'static,
	{
		folder_par_sink!(
			MostDistinctFolder,
			SumZeroFolder<Top<A, HyperLogLogMagnitude<B>>>,
			self,
			MostDistinctFolder::new(self.n, self.probability, self.tolerance, self.error_rate),
			SumZeroFolder::new(Top::new(
				self.n,
				self.probability,
				self.tolerance,
				self.error_rate
			))
		);
	}
}

#[derive(Clone, Serialize, Deserialize, new)]
pub struct MostDistinctFolder {
	n: usize,
	probability: f64,
	tolerance: f64,
	error_rate: f64,
}

impl<A, B> FolderSync<(A, B)> for MostDistinctFolder
where
	A: Clone + Hash + Eq + Send + 'static,
	B: Hash + 'static,
{
	type State = Top<A, HyperLogLogMagnitude<B>>;
	type Done = Self::State;

	fn zero(&mut self) -> Self::State {
		Top::new(self.n, self.probability, self.tolerance, self.error_rate)
	}
	fn push(&mut self, state: &mut Self::State, item: (A, B)) {
		state.push(item.0, &item.1)
	}
	fn done(&mut self, state: Self::State) -> Self::Done {
		state
	}
}
