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
impl<F, A, T> FolderSync<T> for ReduceFn<F, A>
where
	F: FnMut<(A, A), Output = A>,
	T: Into<Option<A>>,
{
	type Output = Option<A>;

	fn zero(&mut self) -> Self::Output {
		None
	}
	fn push(&mut self, state: &mut Self::Output, item: T) {
		if let Some(item) = item.into() {
			*state = Some(if let Some(state) = state.take() {
				self.0.call_mut((state, item))
			} else {
				item
			});
		}
	}
}

#[derive(new)]
#[must_use]
pub struct Combine<I, F> {
	i: I,
	f: F,
}

impl_par_dist! {
	impl<I: ParallelPipe<Source>, Source, F> ParallelSink<Source>
		for Combine<I, F>
	where
		F: FnMut<(I::Item, I::Item,), Output = I::Item> + Clone + Send + 'static,
		I::Item: Send + 'static,
	{
		combiner_par_sink!(ReduceFn<F, I::Item>, self, ReduceFn::new(self.f));
	}
}
