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
pub struct Histogram<P> {
	pipe: P,
}

impl_par_dist! {
	impl<P: ParallelPipe<Item>, Item> ParallelSink<Item> for Histogram<P>
	where
		P::Output: Hash + Ord + Send + 'static,
	{
		folder_par_sink!(HistogramFolder<P::Output, StepA>, HistogramFolder<P::Output, StepB>, self, HistogramFolder::new(), HistogramFolder::new());
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

impl<Item> FolderSync<Item> for HistogramFolder<Item, StepA>
where
	Item: Hash + Ord,
{
	type Done = HashMap<Item, usize>;

	fn zero(&mut self) -> Self::Done {
		HashMap::new()
	}
	fn push(&mut self, state: &mut Self::Done, item: Item) {
		*state.entry(item).or_insert(0) += 1;
	}
}
impl<B> FolderSync<HashMap<B, usize>> for HistogramFolder<B, StepB>
where
	B: Hash + Ord,
{
	type Done = Vec<(B, usize)>;

	fn zero(&mut self) -> Self::Done {
		Vec::new()
	}
	fn push(&mut self, state: &mut Self::Done, b: HashMap<B, usize>) {
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
	type Done = Vec<(B, usize)>;

	fn zero(&mut self) -> Self::Done {
		Vec::new()
	}
	fn push(&mut self, state: &mut Self::Done, b: Vec<(B, usize)>) {
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
