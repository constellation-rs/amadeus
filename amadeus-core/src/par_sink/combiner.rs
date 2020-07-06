#![allow(unused_imports,clippy::single_component_path_imports)]

use super::FolderSync;

mod macros {
	#[macro_export]
	macro_rules! combiner_par_sink {
		($combiner:ty, $self:ident, $init:expr) => {
			type Output = <Self::ReduceC as $crate::par_sink::Reducer<<Self::ReduceA as $crate::par_sink::Reducer<I::Item>>::Output>>::Output;
			type Pipe = I;
			type ReduceA = FolderSyncReducer<I::Item, $combiner>;
			type ReduceC = FolderSyncReducer<<Self::ReduceA as $crate::par_sink::Reducer<I::Item>>::Output, $combiner>;

			fn reducers($self) -> (I, Self::ReduceA, Self::ReduceC) {
				let init = $init;
				(
					$self.i,
					FolderSyncReducer::new(init.clone()),
					FolderSyncReducer::new(init),
				)
			}
		};
	}
	#[macro_export]
	macro_rules! combiner_dist_sink {
		($combiner:ty, $self:ident, $init:expr) => {
			type Output = <Self::ReduceC as $crate::par_sink::Reducer<<Self::ReduceB as $crate::par_sink::Reducer<<Self::ReduceA as $crate::par_sink::Reducer<I::Item>>::Output>>::Output>>::Output;
			type Pipe = I;
			type ReduceA = FolderSyncReducer<I::Item, $combiner>;
			type ReduceB = FolderSyncReducer<<Self::ReduceA as $crate::par_sink::Reducer<I::Item>>::Output, $combiner>;
			type ReduceC = FolderSyncReducer<<Self::ReduceB as $crate::par_sink::Reducer<<Self::ReduceA as $crate::par_sink::Reducer<I::Item>>::Output>>::Output, $combiner>;

			fn reducers($self) -> (I, Self::ReduceA, Self::ReduceB, Self::ReduceC) {
				let init = $init;
				(
					$self.i,
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
	type Output;

	fn combine(&mut self, a: Self::Output, b: Self::Output) -> Self::Output;
}
impl<C, A, B> FolderSync<A> for C
where
	C: CombinerSync<Output = B>,
	A: Into<Option<B>>,
{
	type Output = Option<B>;

	fn zero(&mut self) -> Self::Output {
		None
	}
	fn push(&mut self, state: &mut Self::Output, item: A) {
		if let Some(item) = item.into() {
			*state = Some(if let Some(state) = state.take() {
				self.combine(state, item)
			} else {
				item
			});
		}
	}
}
