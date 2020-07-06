#![allow(clippy::type_complexity)]

use derive_new::new;
use educe::Educe;
use itertools::Itertools;
use replace_with::replace_with_or_default;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, hash::Hash, marker::PhantomData};

use super::{folder_par_sink, FolderSync, FolderSyncReducer, ParallelPipe, ParallelSink};

#[derive(new)]
#[must_use]
pub struct Histogram<I> {
	i: I,
}

impl_par_dist! {
	impl<I: ParallelPipe<Source>, Source> ParallelSink<Source>
		for Histogram<I>
	where
		I::Item: Hash + Ord + Send + 'static,
	{
		folder_par_sink!(HistogramFolder<I::Item, StepA>, HistogramFolder<I::Item, StepB>, self, HistogramFolder::new(), HistogramFolder::new());
	}
}

#[derive(Educe, Serialize, Deserialize, new)]
#[educe(Clone)]
#[serde(bound = "")]
pub struct HistogramFolder<B, Step> {
	marker: PhantomData<fn() -> (B, Step)>,
}

pub struct StepA;
pub struct StepB;

impl<B> FolderSync<B> for HistogramFolder<B, StepA>
where
	B: Hash + Ord,
{
	type Output = HashMap<B, usize>;

	fn zero(&mut self) -> Self::Output {
		HashMap::new()
	}
	fn push(&mut self, state: &mut Self::Output, item: B) {
		*state.entry(item).or_insert(0) += 1;
	}
}
impl<B> FolderSync<HashMap<B, usize>> for HistogramFolder<B, StepB>
where
	B: Hash + Ord,
{
	type Output = Vec<(B, usize)>;

	fn zero(&mut self) -> Self::Output {
		Vec::new()
	}
	fn push(&mut self, state: &mut Self::Output, b: HashMap<B, usize>) {
		let mut b = b.into_iter().collect::<Vec<_>>();
		b.sort_by(|a, b| a.0.cmp(&b.0));
		replace_with_or_default(state, |state| {
			state
				.into_iter()
				.merge(b)
				.coalesce(|a, b| {
					if a.0 == b.0 {
						Ok((a.0, a.1 + b.1))
					} else {
						Err((a, b))
					}
				})
				.collect()
		})
	}
}
impl<B> FolderSync<Vec<(B, usize)>> for HistogramFolder<B, StepB>
where
	B: Hash + Ord,
{
	type Output = Vec<(B, usize)>;

	fn zero(&mut self) -> Self::Output {
		Vec::new()
	}
	fn push(&mut self, state: &mut Self::Output, b: Vec<(B, usize)>) {
		replace_with_or_default(state, |state| {
			state
				.into_iter()
				.merge(b)
				.coalesce(|a, b| {
					if a.0 == b.0 {
						Ok((a.0, a.1 + b.1))
					} else {
						Err((a, b))
					}
				})
				.collect()
		})
	}
}
