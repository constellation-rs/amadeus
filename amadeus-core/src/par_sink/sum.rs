use derive_new::new;
use educe::Educe;
use replace_with::replace_with_or_abort;
use serde::{Deserialize, Serialize};
use std::{iter, marker::PhantomData, mem};

use super::{folder_par_sink, FolderSync, FolderSyncReducer, ParallelPipe, ParallelSink};

#[derive(new)]
#[must_use]
pub struct Sum<P, B> {
	pipe: P,
	marker: PhantomData<fn() -> B>,
}

impl_par_dist! {
	impl<P: ParallelPipe<Input>, Input, B> ParallelSink<Input> for Sum<P, B>
	where
		B: iter::Sum<P::Output> + iter::Sum<B> + Send + 'static,
	{
		folder_par_sink!(
			SumFolder<B>,
			SumFolder<B>,
			self,
			SumFolder::new(),
			SumFolder::new()
		);
	}
}

#[derive(Educe, Serialize, Deserialize, new)]
#[educe(Clone)]
#[serde(bound = "")]
pub struct SumFolder<B> {
	marker: PhantomData<fn() -> B>,
}

impl<Item, B> FolderSync<Item> for SumFolder<B>
where
	B: iter::Sum<Item> + iter::Sum<B>,
{
	type Done = B;

	fn zero(&mut self) -> Self::Done {
		iter::empty::<B>().sum()
	}
	fn push(&mut self, state: &mut Self::Done, item: Item) {
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

impl<Item> FolderSync<Item> for SumZeroFolder<Item>
where
	Option<Item>: iter::Sum<Item>,
{
	type Done = Item;

	fn zero(&mut self) -> Self::Done {
		self.zero.take().unwrap()
	}
	fn push(&mut self, state: &mut Self::Done, item: Item) {
		replace_with_or_abort(state, |state| {
			iter::once(state)
				.chain(iter::once(iter::once(item).sum::<Option<Item>>().unwrap()))
				.sum::<Option<Item>>()
				.unwrap()
		})
	}
}
