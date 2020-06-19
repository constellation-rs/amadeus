#![allow(clippy::type_complexity)]

use derive_new::new;
use rand::thread_rng;
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use streaming_algorithms::{HyperLogLogMagnitude, SampleUnstable as SASampleUnstable, Top};

use super::{
	folder_par_sink, FolderSync, FolderSyncReducer, FolderSyncReducerFactory, ParallelPipe, ParallelSink, SumFolder, SumZeroFolder
};

#[derive(new)]
#[must_use]
pub struct SampleUnstable<I> {
	i: I,
	samples: usize,
}

impl_par_dist! {
	impl<I: ParallelPipe<Source>, Source> ParallelSink<Source>
		for SampleUnstable<I>
	where
		I::Item: Send + 'static,
	{
		folder_par_sink!(SampleUnstableFolder, SumFolder<SASampleUnstable<I::Item>>, self, SampleUnstableFolder::new(self.samples), SumFolder::new());
	}
}

#[derive(Clone, Serialize, Deserialize, new)]
pub struct SampleUnstableFolder {
	samples: usize,
}

impl<A> FolderSync<A> for SampleUnstableFolder {
	type Output = SASampleUnstable<A>;

	fn zero(&mut self) -> Self::Output {
		SASampleUnstable::new(self.samples)
	}
	fn push(&mut self, state: &mut Self::Output, item: A) {
		state.push(item, &mut thread_rng())
	}
}

#[derive(new)]
#[must_use]
pub struct MostFrequent<I> {
	i: I,
	n: usize,
	probability: f64,
	tolerance: f64,
}

impl_par_dist! {
	impl<I: ParallelPipe<Source>, Source> ParallelSink<Source>
		for MostFrequent<I>
	where
		I::Item: Clone + Hash + Eq + Send + 'static,
	{
		folder_par_sink!(MostFrequentFolder, SumZeroFolder<Top<I::Item, usize>>, self, MostFrequentFolder::new(self.n, self.probability, self.tolerance), SumZeroFolder::new(Top::new(self.n, self.probability, self.tolerance, ())));
	}
}

#[derive(Clone, Serialize, Deserialize, new)]
pub struct MostFrequentFolder {
	n: usize,
	probability: f64,
	tolerance: f64,
}

impl<A> FolderSync<A> for MostFrequentFolder
where
	A: Clone + Hash + Eq + Send + 'static,
{
	type Output = Top<A, usize>;

	fn zero(&mut self) -> Self::Output {
		Top::new(self.n, self.probability, self.tolerance, ())
	}
	fn push(&mut self, state: &mut Self::Output, item: A) {
		state.push(item, &1)
	}
}

#[derive(new)]
#[must_use]
pub struct MostDistinct<I> {
	i: I,
	n: usize,
	probability: f64,
	tolerance: f64,
	error_rate: f64,
}

impl_par_dist! {
	impl<I: ParallelPipe<Source, Item = (A, B)>, Source, A, B> ParallelSink<Source> for MostDistinct<I>
	where
		A: Clone + Hash + Eq + Send + 'static,
		B: Hash + 'static,
	{
		folder_par_sink!(MostDistinctFolder, SumZeroFolder<Top<A, HyperLogLogMagnitude<B>>>, self, MostDistinctFolder::new(self.n, self.probability, self.tolerance, self.error_rate), SumZeroFolder::new(Top::new(self.n, self.probability, self.tolerance, self.error_rate)));
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
	type Output = Top<A, HyperLogLogMagnitude<B>>;

	fn zero(&mut self) -> Self::Output {
		Top::new(self.n, self.probability, self.tolerance, self.error_rate)
	}
	fn push(&mut self, state: &mut Self::Output, item: (A, B)) {
		state.push(item.0, &item.1)
	}
}
