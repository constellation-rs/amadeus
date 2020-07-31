use derive_new::new;
use educe::Educe;
use serde::{Deserialize, Serialize};
use std::{iter, marker::PhantomData, mem};

use super::{
	folder_par_sink, FolderSync, FolderSyncReducer, ParallelPipe, ParallelSink
};

#[derive(new)]
#[must_use]
pub struct Mean<P, B> {
    pipe: P,
    marker: PhantomData<fn() -> B>,
}

impl_par_dist! {
    impl<P: ParallelPipe<Item>, Item, B> ParallelSink<Item> for Mean<P, B> 
    where
        B: iter::Sum<P::Output> + iter::Sum<B> + Send + 'static,
    {
		folder_par_sink!(
			MeanFolder<B, StepA>,
			MeanFolder<B, StepB>,
			self,
			MeanFolder::new(),
			MeanFolder::new()
		);
	}
}

#[derive(Educe, Serialize, Deserialize, new)]
#[educe(Clone)]
#[serde(bound = "")]

pub struct MeanFolder<B, Step> {
    marker: PhantomData<fn() -> (B, Step)>,
}

pub struct StepA;
pub struct StepB;

impl<B, Item> FolderSync<Item> for MeanFolder<B, StepA>
where
    B: iter::Sum<Item> + iter::Sum<B>,
{
    type Done = (B, usize);

    #[inline(always)]
    fn zero(&mut self) -> Self::Done {
        (iter::empty::<B>().sum(),0)
    }

    #[inline(always)]
	fn push(&mut self, state: &mut Self::Done, item: Item) {
        let zero = iter::empty::<B>().sum();
        let left = mem::replace(&mut state.0, zero);
        let right = iter::once(item).sum::<B>();
        
        state.0 = B::sum(iter::once(left).chain(iter::once(right)));
        state.1 += 1;
    }
}



impl<B> FolderSync<(B, usize)> for MeanFolder<B, StepB>
where
    B: iter::Sum<B> 
{
    type Done = (B, usize);

    #[inline(always)]
    fn zero(&mut self) -> Self::Done {
        (iter::empty().sum(),0)
    }

    #[inline(always)]
	fn push(&mut self, state: &mut Self::Done, item: (B, usize)) {
        let zero = iter::empty().sum();
        let left = mem::replace(&mut state.0, zero);
        let right = iter::once(item.0).sum();
        
        state.0 = B::sum(iter::once(left).chain(iter::once(right)));
        state.1 += 1;
    }
}
