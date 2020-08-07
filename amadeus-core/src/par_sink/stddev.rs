use derive_new::new;
use educe::Educe;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use super::{folder_par_sink, FolderSync, FolderSyncReducer, ParallelPipe, ParallelSink};
use crate::util::u64_to_f64;

#[derive(new)]
#[must_use]
pub struct StdDev<P> {
	pipe: P,
}

impl_par_dist! {
	impl<P: ParallelPipe<Item, Output = f64>, Item, > ParallelSink<Item> for StdDev<P> {
		folder_par_sink!(
			SDFolder<StepA>,
			SDFolder<StepB>,
			self,
			SDFolder::new(),
			SDFolder::new()
		);
	}
}

#[derive(Educe, Serialize, Deserialize, new)]
#[educe(Clone)]
#[serde(bound = "")]
pub struct SDFolder<Step> {
	marker: PhantomData<fn() -> Step>,
}

pub struct StepA;
pub struct StepB;

#[derive(Serialize, Deserialize, new)]
pub struct SDState {
	#[new(default)]
	count: u64,
	#[new(default)]
	mean: f64,
	#[new(default)]
	variance: f64,
}

impl FolderSync<f64> for SDFolder<StepA> {
	type State = SDState;
	type Done = f64;

	#[inline(always)]
	fn zero(&mut self) -> Self::State {
		SDState::new()
	}

	#[inline(always)]
	fn push(&mut self, state: &mut Self::State, item: f64) {
		// Taken from https://docs.rs/streaming-stats/0.2.3/src/stats/online.rs.html#64-103
		let q_prev = state.variance * u64_to_f64(state.count);
		let mean_prev = state.mean;
		state.count += 1;
		let count = u64_to_f64(state.count);
		state.mean += (item - state.mean) / count;
		state.variance = (q_prev + (item - mean_prev) * (item - state.mean)) / count;
	}

	#[inline(always)]
	fn done(&mut self, state: Self::State) -> Self::Done {
		state.variance.sqrt()
	}
}

impl FolderSync<SDState> for SDFolder<StepB> {
	type State = SDState;
	type Done = f64;

	#[inline(always)]
	fn zero(&mut self) -> Self::State {
		SDState::new()
	}

	#[inline(always)]
	fn push(&mut self, state: &mut Self::State, item: SDState) {
		let (s1, s2) = (u64_to_f64(state.count), u64_to_f64(item.count));
		let meandiffsq = (state.mean - item.mean) * (state.mean - item.mean);
		let mean = ((s1 * state.mean) + (s2 * item.mean)) / (s1 + s2);
		let var = (((s1 * state.variance) + (s2 * item.variance)) / (s1 + s2))
			+ ((s1 * s2 * meandiffsq) / ((s1 + s2) * (s1 + s2)));
		state.count += item.count;
		state.mean = mean;
		state.variance = var;
	}

	#[inline(always)]
	fn done(&mut self, state: Self::State) -> Self::Done {
		state.variance.sqrt()
	}
}
