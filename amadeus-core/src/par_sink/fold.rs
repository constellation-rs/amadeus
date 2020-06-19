#![allow(clippy::type_complexity)]

use derive_new::new;
use educe::Educe;
use either::Either;
use replace_with::replace_with_or_abort;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use super::{
	folder_par_sink, FolderSync, FolderSyncReducer, FolderSyncReducerFactory, ParallelPipe, ParallelSink
};

#[derive(new)]
#[must_use]
pub struct Fold<I, ID, F, B> {
	i: I,
	identity: ID,
	op: F,
	marker: PhantomData<fn() -> B>,
}

impl_par_dist! {
	impl<I: ParallelPipe<Source>, Source, ID, F, B> ParallelSink<Source>
		for Fold<I, ID, F, B>
	where
		ID: FnMut() -> B + Clone + Send + 'static,
		F: FnMut(B, Either<I::Item, B>) -> B + Clone + Send + 'static,
		B: Send + 'static,
	{
		folder_par_sink!(FoldFolder<I::Item, ID, F, B, StepA>, FoldFolder<I::Item, ID, F, B, StepB>, self, FoldFolder::new(self.identity.clone(), self.op.clone()), FoldFolder::new(self.identity, self.op));
	}
}

#[derive(Educe, Serialize, Deserialize, new)]
#[educe(Clone(bound = "ID: Clone, F: Clone"))]
#[serde(
	bound(serialize = "ID: Serialize, F: Serialize"),
	bound(deserialize = "ID: Deserialize<'de>, F: Deserialize<'de>")
)]
pub struct FoldFolder<A, ID, F, B, Step> {
	identity: ID,
	op: F,
	marker: PhantomData<fn() -> (A, B, Step)>,
}

pub struct StepA;
pub struct StepB;

impl<A, ID, F, B> FolderSync<A> for FoldFolder<A, ID, F, B, StepA>
where
	ID: FnMut() -> B,
	F: FnMut(B, Either<A, B>) -> B,
{
	type Output = B;

	fn zero(&mut self) -> Self::Output {
		(self.identity)()
	}
	fn push(&mut self, state: &mut Self::Output, item: A) {
		replace_with_or_abort(state, |state| (self.op)(state, Either::Left(item)))
	}
}
impl<A, ID, F, B> FolderSync<B> for FoldFolder<A, ID, F, B, StepB>
where
	ID: FnMut() -> B,
	F: FnMut(B, Either<A, B>) -> B,
{
	type Output = B;

	fn zero(&mut self) -> Self::Output {
		(self.identity)()
	}
	fn push(&mut self, state: &mut Self::Output, item: B) {
		replace_with_or_abort(state, |state| (self.op)(state, Either::Right(item)))
	}
}
