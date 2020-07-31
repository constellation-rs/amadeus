#![allow(unused_imports, clippy::single_component_path_imports, clippy::option_if_let_else)]

use super::FolderSync;

mod macros {
	#[macro_export]
	macro_rules! combiner_par_sink {
		($combiner:ty, $self:ident, $init:expr) => {
			type Done = <Self::ReduceC as $crate::par_sink::Reducer<<Self::ReduceA as $crate::par_sink::Reducer<P::Output>>::Done>>::Done;
			type Pipe = P;
			type ReduceA = FolderSyncReducer<P::Output, $combiner>;
			type ReduceC = FolderSyncReducer<<Self::ReduceA as $crate::par_sink::Reducer<P::Output>>::Done, $combiner>;

			fn reducers($self) -> (P, Self::ReduceA, Self::ReduceC) {
				let init = $init;
				(
					$self.pipe,
					FolderSyncReducer::new(init.clone()),
					FolderSyncReducer::new(init),
				)
			}
		};
	}
	#[macro_export]
	macro_rules! combiner_dist_sink {
		($combiner:ty, $self:ident, $init:expr) => {
			type Done = <Self::ReduceC as $crate::par_sink::Reducer<<Self::ReduceB as $crate::par_sink::Reducer<<Self::ReduceA as $crate::par_sink::Reducer<P::Output>>::Done>>::Done>>::Done;
			type Pipe = P;
			type ReduceA = FolderSyncReducer<P::Output, $combiner>;
			type ReduceB = FolderSyncReducer<<Self::ReduceA as $crate::par_sink::Reducer<P::Output>>::Done, $combiner>;
			type ReduceC = FolderSyncReducer<<Self::ReduceB as $crate::par_sink::Reducer<<Self::ReduceA as $crate::par_sink::Reducer<P::Output>>::Done>>::Done, $combiner>;

			fn reducers($self) -> (P, Self::ReduceA, Self::ReduceB, Self::ReduceC) {
				let init = $init;
				(
					$self.pipe,
					FolderSyncReducer::new(init.clone()),
					FolderSyncReducer::new(init.clone()),
					FolderSyncReducer::new(init),
				)
			}
		};
	}
	pub(crate) use combiner_dist_sink;
	pub(crate) use combiner_par_sink;
}

pub(crate) use macros::{combiner_dist_sink, combiner_par_sink};

pub trait CombinerSync {
	type Done;

	fn combine(&mut self, a: Self::Done, b: Self::Done) -> Self::Done;
}
impl<C, Item, B> FolderSync<Item> for C
where
	C: CombinerSync<Done = B>,
	Item: Into<Option<B>>,
{
	type State = Option<B>;
	type Done = Self::State;

	fn zero(&mut self) -> Self::State {
		None
	}
	fn push(&mut self, state: &mut Self::State, item: Item) {
		if let Some(item) = item.into() {
			*state = Some(if let Some(state) = state.take() {
				self.combine(state, item)
			} else {
				item
			});
		}
	}
    fn done(&mut self, state: Self::State) -> Self::Done { state }

}
