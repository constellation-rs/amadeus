#![allow(clippy::type_complexity)]

use derive_new::new;
use rand::thread_rng;
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use streaming_algorithms::{HyperLogLogMagnitude, SampleUnstable as SASampleUnstable, Top};

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
	impl<P: ParallelPipe<Input>, Input> ParallelSink<Input> for SampleUnstable<P>
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
	type Done = SASampleUnstable<Item>;

	fn zero(&mut self) -> Self::Done {
		SASampleUnstable::new(self.samples)
	}
	fn push(&mut self, state: &mut Self::Done, item: Item) {
		state.push(item, &mut thread_rng())
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
	impl<P: ParallelPipe<Input>, Input> ParallelSink<Input> for MostFrequent<P>
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
	type Done = Top<Item, usize>;

	fn zero(&mut self) -> Self::Done {
		Top::new(self.n, self.probability, self.tolerance, ())
	}
	fn push(&mut self, state: &mut Self::Done, item: Item) {
		state.push(item, &1)
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
	impl<P: ParallelPipe<Input, Output = (A, B)>, Input, A, B> ParallelSink<Input> for MostDistinct<P>
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
	type Done = Top<A, HyperLogLogMagnitude<B>>;

	fn zero(&mut self) -> Self::Done {
		Top::new(self.n, self.probability, self.tolerance, self.error_rate)
	}
	fn push(&mut self, state: &mut Self::Done, item: (A, B)) {
		state.push(item.0, &item.1)
	}
}
