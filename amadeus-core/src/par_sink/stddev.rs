use derive_new::new;
use educe::Educe;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use super::{folder_par_sink, FolderSync, FolderSyncReducer, ParallelPipe, ParallelSink};

#[derive(new)]
#[must_use]
pub struct StdDev<P> {
	pipe: P,
}

impl_par_dist! {
	impl<P: ParallelPipe<Item, Output = f64>, Item> ParallelSink<Item> for StdDev<P>
		{
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
	mean: f64,
	#[new(default)]
	correction: f64,
	#[new(default)]
	count: u64,
	#[new(default)]
	value: Vec<f64>,
	#[new(default)]
	sd: f64,
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
		state.count += 1;
		let f = (item - state.mean) / (state.count as f64);
		let y = f - state.correction;
		let t = state.mean + y;
		state.correction = (t - state.mean) - y;
		state.mean = t;

		state.value.push(item);
	}

	#[inline(always)]
	fn done(&mut self, state: Self::State) -> Self::Done {
		let variance_sum = state
			.value
			.iter()
			.map(|x| (x - state.mean).powi(2))
			.collect::<Vec<f64>>();
		let state_variance = variance_sum.iter().sum::<f64>() / state.count as f64;
		let sd = state_variance.sqrt();
		sd
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
		state.correction = ((state.correction * state.count as f64)
			+ (item.correction * item.count as f64))
			/ ((state.count + item.count) as f64);
		state.mean = ((state.mean * state.count as f64) + (item.mean * item.count as f64))
			/ ((state.count + item.count) as f64);

		let state_variance_sum = state
			.value
			.iter()
			.map(|x| (x - state.mean).powi(2))
			.collect::<Vec<f64>>();
		let state_variance = state_variance_sum.iter().sum::<f64>() / state.count as f64;
		let state_sd = state_variance.sqrt();

		let item_variance_sum = item
			.value
			.iter()
			.map(|x| (x - item.mean).powi(2))
			.collect::<Vec<f64>>();
		let item_variance = item_variance_sum.iter().sum::<f64>() / item.count as f64;
		let item_sd = item_variance.sqrt();

		state.sd = ((state.count as f64 - 1.0) * state_sd + (item.count as f64 - 1.0) * item_sd)
			/ ((state.count as f64 + item.count as f64) - 2.0);

		state.count += item.count;
	}

	#[inline(always)]
	fn done(&mut self, state: Self::State) -> Self::Done {
		state.sd
	}
}
