#![allow(clippy::type_complexity)]

use derive_new::new;
use educe::Educe;
use either::Either;
use replace_with::replace_with_or_abort;
use serde::{Deserialize, Serialize};
use serde_closure::traits::FnMut;
use std::marker::PhantomData;

use super::{folder_par_sink, FolderSync, FolderSyncReducer, ParallelPipe, ParallelSink};

#[derive(new)]
#[must_use]
pub struct Fold<P, ID, F, B> {
	pipe: P,
	identity: ID,
	op: F,
	marker: PhantomData<fn() -> B>,
}

impl_par_dist! {
	impl<P: ParallelPipe<Item>, Item, ID, F, B> ParallelSink<Item> for Fold<P, ID, F, B>
	where
		ID: FnMut<(), Output = B> + Clone + Send + 'static,
		F: FnMut<(B, Either<P::Output, B>), Output = B> + Clone + Send + 'static,
		B: Send + 'static,
	{
		folder_par_sink!(FoldFolder<P::Output, ID, F, B, StepA>, FoldFolder<P::Output, ID, F, B, StepB>, self, FoldFolder::new(self.identity.clone(), self.op.clone()), FoldFolder::new(self.identity, self.op));
	}
}

#[derive(Educe, Serialize, Deserialize, new)]
#[educe(Clone(bound = "ID: Clone, F: Clone"))]
#[serde(
	bound(serialize = "ID: Serialize, F: Serialize"),
	bound(deserialize = "ID: Deserialize<'de>, F: Deserialize<'de>")
)]
pub struct FoldFolder<Item, ID, F, B, Step> {
	identity: ID,
	op: F,
	marker: PhantomData<fn() -> (Item, B, Step)>,
}

pub struct StepA;
pub struct StepB;

impl<Item, ID, F, B> FolderSync<Item> for FoldFolder<Item, ID, F, B, StepA>
where
	ID: FnMut<(), Output = B>,
	F: FnMut<(B, Either<Item, B>), Output = B>,
{
	type State = B;
	type Done = Self::State;

	fn zero(&mut self) -> Self::State {
		self.identity.call_mut(())
	}
	fn push(&mut self, state: &mut Self::State, item: Item) {
		replace_with_or_abort(state, |state| self.op.call_mut((state, Either::Left(item))))
	}
    fn done(&mut self, state: Self::State) -> Self::Done { state }

}
impl<A, ID, F, Item> FolderSync<Item> for FoldFolder<A, ID, F, Item, StepB>
where
	ID: FnMut<(), Output = Item>,
	F: FnMut<(Item, Either<A, Item>), Output = Item>,
{
	type State = Item;
	type Done = Self::State;

	fn zero(&mut self) -> Self::State {
		self.identity.call_mut(())
	}
	fn push(&mut self, state: &mut Self::State, item: Item) {
		replace_with_or_abort(state, |state| {
			self.op.call_mut((state, Either::Right(item)))
		})
	}
	#[inline(always)]
    fn done(&mut self, state: Self::State) -> Self::Done { state }

}
