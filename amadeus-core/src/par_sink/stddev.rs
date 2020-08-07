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
	impl<P: ParallelPipe<Item, Output = f64>, Item, > ParallelSink<Item> for StdDev<P>	{
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
	sum: f64,
	#[new(default)]
	pre_variance: f64,
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
		state.count += 1;
		state.sum += item;
		let diff = u64_to_f64(state.count) * item - state.sum;
		state.pre_variance +=
			diff * diff / (u64_to_f64(state.count) * (u64_to_f64(state.count) - 1.0));
		if state.count > 1 {
			state.variance = state.pre_variance / u64_to_f64(state.count) - 1.0;
		} else {
			state.variance = f64::NAN;
		}
	}

	#[inline(always)]
	fn done(&mut self, state: Self::State) -> Self::Done {
		if state.count > 1 {
			state.variance.sqrt()
		} else {
			f64::NAN
		}
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
		state.variance = ((u64_to_f64(state.count) - 1.0) * state.variance
			+ (u64_to_f64(item.count) - 1.0) * item.variance)
			/ ((u64_to_f64(state.count) + u64_to_f64(item.count)) - 2.0);

		state.count += item.count;
	}

	#[inline(always)]
	fn done(&mut self, state: Self::State) -> Self::Done {
		state.variance.sqrt()
	}
}
