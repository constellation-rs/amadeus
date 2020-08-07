use derive_new::new;
use educe::Educe;
use replace_with::{replace_with, replace_with_or_abort};
use serde::{Deserialize, Serialize};
use std::{iter, marker::PhantomData};

use super::{folder_par_sink, FolderSync, FolderSyncReducer, ParallelPipe, ParallelSink};

#[derive(new)]
#[must_use]
pub struct Sum<P, B> {
	pipe: P,
	marker: PhantomData<fn() -> B>,
}

impl_par_dist! {
	impl<P: ParallelPipe<Item>, Item, B> ParallelSink<Item> for Sum<P, B>
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
	type State = B;
	type Done = Self::State;

	#[inline(always)]
	fn zero(&mut self) -> Self::Done {
		B::sum(iter::empty::<B>())
	}
	#[inline(always)]
	fn push(&mut self, state: &mut Self::Done, item: Item) {
		let default = || B::sum(iter::empty::<B>());
		replace_with(state, default, |left| {
			let right = iter::once(item).sum::<B>();
			B::sum(iter::once(left).chain(iter::once(right)))
		})
	}
	#[inline(always)]
	fn done(&mut self, state: Self::State) -> Self::Done {
		state
	}
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SumZeroFolder<B> {
	zero: Option<B>,
}
impl<B> SumZeroFolder<B> {
	#[inline(always)]
	pub(crate) fn new(zero: B) -> Self {
		Self { zero: Some(zero) }
	}
}

impl<Item> FolderSync<Item> for SumZeroFolder<Item>
where
	Option<Item>: iter::Sum<Item>,
{
	type State = Item;
	type Done = Self::State;

	#[inline(always)]
	fn zero(&mut self) -> Self::Done {
		self.zero.take().unwrap()
	}
	#[inline(always)]
	fn push(&mut self, state: &mut Self::Done, item: Item) {
		replace_with_or_abort(state, |left| {
			let right = iter::once(item).sum::<Option<Item>>().unwrap();
			<Option<Item> as iter::Sum<Item>>::sum(iter::once(left).chain(iter::once(right)))
				.unwrap()
		})
	}

	#[inline(always)]
	fn done(&mut self, state: Self::State) -> Self::Done {
		state
	}
}
