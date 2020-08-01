extern crate num;

use derive_new::new;
use educe::Educe;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use super::{
	folder_par_sink, FolderSync, FolderSyncReducer, ParallelPipe, ParallelSink
};

#[derive(new)]
#[must_use]
pub struct Mean<P> {
    pipe: P,
}

impl_par_dist! {
    impl<P: ParallelPipe<f64>, f64> ParallelSink<f64> for Mean<P> {
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

impl FolderSync<f64> for MeanFolder<StepA> {
    type State = (f64, usize);
    type Done = f64;

    #[inline(always)]
    fn zero(&mut self) -> Self::State {
        (0.0,0)
    }

    #[inline(always)]
    fn push(&mut self, state: &mut Self::State, item: f64) {
        state.0 += item;
        state.1 += 1;
    }

    #[inline(always)]
    fn done(&mut self, state: Self::State) -> Self::Done {
        let sum = state.0;
        let count = state.1 as f64;
        return sum / count
    }
}




impl FolderSync<(f64, usize)> for MeanFolder<StepB> {
    type State = (f64, usize);
    type Done = f64;

    #[inline(always)]
    fn zero(&mut self) -> Self::State {
        (0.0,0)
    }

    #[inline(always)]
	fn push(&mut self, state: &mut Self::State, item: (f64, usize)) {
        state.0 += item.0;
        state.1 += 1;
    }

    #[inline(always)]
    fn done(&mut self, state: Self::State) -> Self::Done { 
        let sum = state.0;
        let count = state.1 as f64;
        return sum / count;

    }

}
