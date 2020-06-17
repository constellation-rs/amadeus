use derive_new::new;
use educe::Educe;
use replace_with::replace_with_or_abort;
use serde::{Deserialize, Serialize};
use std::{iter, marker::PhantomData, mem};

use super::{
	folder_par_sink, FolderSync, FolderSyncReducer, FolderSyncReducerFactory, ParallelPipe, ParallelSink
};

#[derive(new)]
#[must_use]
pub struct Sum<I, B> {
	i: I,
	marker: PhantomData<fn() -> B>,
}

impl_par_dist! {
	impl<I: ParallelPipe<Source>, Source, B> ParallelSink<Source>
		for Sum<I, B>
	where
		B: iter::Sum<I::Item> + iter::Sum<B> + Send+'static,
		I::Item: 'static,
	{
		folder_par_sink!(SumFolder<B>, SumFolder<B>, self, SumFolder::new(), SumFolder::new());
	}
}

#[derive(Educe, Serialize, Deserialize, new)]
#[educe(Clone)]
#[serde(bound = "")]
pub struct SumFolder<B> {
	marker: PhantomData<fn() -> B>,
}

impl<A, B> FolderSync<A> for SumFolder<B>
where
	B: iter::Sum<A> + iter::Sum<B>,
{
	type Output = B;

	fn zero(&mut self) -> Self::Output {
		iter::empty::<B>().sum()
	}
	fn push(&mut self, state: &mut Self::Output, item: A) {
		*state = iter::once(mem::replace(state, iter::empty::<B>().sum()))
			.chain(iter::once(iter::once(item).sum::<B>()))
			.sum();
	}
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SumZeroFolder<B> {
	zero: Option<B>,
}
impl<B> SumZeroFolder<B> {
	pub(crate) fn new(zero: B) -> Self {
		Self { zero: Some(zero) }
	}
}

impl<B> FolderSync<B> for SumZeroFolder<B>
where
	Option<B>: iter::Sum<B>,
{
	type Output = B;

	fn zero(&mut self) -> Self::Output {
		self.zero.take().unwrap()
	}
	fn push(&mut self, state: &mut Self::Output, item: B) {
		replace_with_or_abort(state, |state| {
			iter::once(state)
				.chain(iter::once(iter::once(item).sum::<Option<B>>().unwrap()))
				.sum::<Option<B>>()
				.unwrap()
		})
	}
}
