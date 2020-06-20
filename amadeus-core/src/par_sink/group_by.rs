#![allow(clippy::type_complexity)]

use derive_new::new;
use educe::Educe;
use either::Either;
use replace_with::{replace_with_or_abort, replace_with_or_default};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, hash::Hash, marker::PhantomData, mem};

use super::{
	folder_par_sink, FolderSync, FolderSyncReducer, FolderSyncReducerFactory, ParallelPipe, ParallelSink
};

#[derive(new)]
#[must_use]
pub struct GroupBy<I, ID, F, B> {
	i: I,
	identity: ID,
	op: F,
	marker: PhantomData<fn() -> B>,
}

impl_par_dist! {
	impl<I: ParallelPipe<Source, Item = (A, B)>, Source, A, B, ID, F, C> ParallelSink<Source>
		for GroupBy<I, ID, F, C>
	where
		A: Eq + Hash + Send + 'static,
		ID: FnMut() -> C + Clone + Send + 'static,
		F: FnMut(C, Either<B, C>) -> C + Clone + Send + 'static,
		C: Send + 'static,
	{
		folder_par_sink!(GroupByFolder<A, B, ID, F, C, StepA>, GroupByFolder<A, B, ID, F, C, StepB>, self, GroupByFolder::new(self.identity.clone(), self.op.clone()), GroupByFolder::new(self.identity, self.op));
	}
}

#[derive(Educe, Serialize, Deserialize, new)]
#[educe(Clone(bound = "ID: Clone, F: Clone"))]
#[serde(
	bound(serialize = "ID: Serialize, F: Serialize"),
	bound(deserialize = "ID: Deserialize<'de>, F: Deserialize<'de>")
)]
pub struct GroupByFolder<A, B, ID, F, C, Step> {
	identity: ID,
	op: F,
	marker: PhantomData<fn() -> (A, B, C, Step)>,
}

pub struct StepA;
pub struct StepB;

impl<A, B, ID, F, C> FolderSync<(A, B)> for GroupByFolder<A, B, ID, F, C, StepA>
where
	A: Eq + Hash,
	ID: FnMut() -> C,
	F: FnMut(C, Either<B, C>) -> C,
{
	type Output = HashMap<A, C>;

	fn zero(&mut self) -> Self::Output {
		HashMap::new()
	}
	fn push(&mut self, state: &mut Self::Output, (key, value): (A, B)) {
		let state = state.entry(key).or_insert_with(&mut self.identity);
		replace_with_or_abort(state, |state| (self.op)(state, Either::Left(value)))
	}
}
impl<A, B, ID, F, C> FolderSync<HashMap<A, C>> for GroupByFolder<A, B, ID, F, C, StepB>
where
	A: Eq + Hash,
	ID: FnMut() -> C,
	F: FnMut(C, Either<B, C>) -> C,
{
	type Output = HashMap<A, C>;

	fn zero(&mut self) -> Self::Output {
		HashMap::new()
	}
	fn push(&mut self, state: &mut Self::Output, mut b: HashMap<A, C>) {
		replace_with_or_default(state, |mut a| {
			if a.len() < b.len() {
				mem::swap(&mut a, &mut b);
			}
			for (key, value) in b {
				let state = a.entry(key).or_insert_with(&mut self.identity);
				replace_with_or_abort(state, |state| (self.op)(state, Either::Right(value)))
			}
			a
		})
	}
}
