#![allow(clippy::option_if_let_else)]

use derive_new::new;
use educe::Educe;
use serde::{Deserialize, Serialize};
use serde_closure::traits::FnMut;
use std::marker::PhantomData;

use super::{combiner_par_sink, FolderSync, FolderSyncReducer, ParallelPipe, ParallelSink};

#[derive(Educe, Serialize, Deserialize, new)]
#[educe(Clone(bound = "F: Clone"))]
#[serde(
	bound(serialize = "F: Serialize"),
	bound(deserialize = "F: Deserialize<'de>")
)]
pub struct ReduceFn<F, A>(F, PhantomData<fn() -> A>);
impl<F, A, Item> FolderSync<Item> for ReduceFn<F, A>
where
	F: FnMut<(A, A), Output = A>,
	Item: Into<Option<A>>,
{
	type State = Option<A>;
	type Done = Self::State;

	fn zero(&mut self) -> Self::State {
		None
	}
	fn push(&mut self, state: &mut Self::State, item: Item) {
		if let Some(item) = item.into() {
			*state = Some(if let Some(state) = state.take() {
				self.0.call_mut((state, item))
			} else {
				item
			});
		}
	}
    fn done(&mut self, state: Self::State) -> Self::Done { state }

}

#[derive(new)]
#[must_use]
pub struct Combine<P, F> {
	pipe: P,
	f: F,
}

impl_par_dist! {
	impl<P: ParallelPipe<Item>, Item, F> ParallelSink<Item> for Combine<P, F>
	where
		F: FnMut<(P::Output, P::Output), Output = P::Output> + Clone + Send + 'static,
		P::Output: Send + 'static,
	{
		combiner_par_sink!(ReduceFn<F, P::Output>, self, ReduceFn::new(self.f));
	}
}
