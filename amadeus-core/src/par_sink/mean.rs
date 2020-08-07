use derive_new::new;
use educe::Educe;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use super::{folder_par_sink, FolderSync, FolderSyncReducer, ParallelPipe, ParallelSink};
use crate::util::u64_to_f64;

#[derive(new)]
#[must_use]
pub struct Mean<P> {
	pipe: P,
}

impl_par_dist! {
	impl<P: ParallelPipe<Item, Output = f64>, Item> ParallelSink<Item> for Mean<P> {
		folder_par_sink!(
			MeanFolder<StepA>,
			MeanFolder<StepB>,
			self,
			MeanFolder::new(),
			MeanFolder::new()
		);
	}
}

#[derive(Educe, Serialize, Deserialize, new)]
#[educe(Clone)]
#[serde(bound = "")]

pub struct MeanFolder<Step> {
	marker: PhantomData<fn() -> Step>,
}

pub struct StepA;
pub struct StepB;

#[derive(Serialize, Deserialize, new)]
pub struct State {
	#[new(default)]
	mean: f64,
	#[new(default)]
	correction: f64,
	#[new(default)]
	count: u64,
}

impl FolderSync<f64> for MeanFolder<StepA> {
	type State = State;
	type Done = f64;

	#[inline(always)]
	fn zero(&mut self) -> Self::State {
		State::new()
	}

	#[inline(always)]
	fn push(&mut self, state: &mut Self::State, item: f64) {
		state.count += 1;
		let f = (item - state.mean) / (u64_to_f64(state.count));
		let y = f - state.correction;
		let t = state.mean + y;
		state.correction = (t - state.mean) - y;
		state.mean = t;
	}

	#[inline(always)]
	fn done(&mut self, state: Self::State) -> Self::Done {
		state.mean
	}
}

impl FolderSync<State> for MeanFolder<StepB> {
	type State = State;
	type Done = f64;

	#[inline(always)]
	fn zero(&mut self) -> Self::State {
		State::new()
	}

	#[inline(always)]
	fn push(&mut self, state: &mut Self::State, item: State) {
		state.correction = (state.correction * u64_to_f64(state.count))
			+ (item.correction * u64_to_f64(item.count)) / u64_to_f64(state.count + item.count);
		state.mean = ((state.mean * u64_to_f64(state.count))
			+ (item.mean * u64_to_f64(item.count)))
			/ u64_to_f64(state.count + item.count);
		state.count += item.count;
	}

	#[inline(always)]
	fn done(&mut self, state: Self::State) -> Self::Done {
		state.mean
	}
}
