use derive_new::new;
use serde::{Deserialize, Serialize};

use super::{
	folder_par_sink, FolderSync, FolderSyncReducer, ParallelPipe, ParallelSink, SumFolder
};

#[derive(new)]
#[must_use]
pub struct Count<P> {
	pipe: P,
}

impl_par_dist! {
	impl<P: ParallelPipe<Item>, Item> ParallelSink<Item> for Count<P> {
		folder_par_sink!(
			CountFolder,
			SumFolder<usize>,
			self,
			CountFolder::new(),
			SumFolder::new()
		);
	}
}

#[derive(Clone, Serialize, Deserialize, new)]
pub struct CountFolder;

impl<Item> FolderSync<Item> for CountFolder {
	type Done = usize;

	#[inline(always)]
	fn zero(&mut self) -> Self::Done {
		0
	}
	#[inline(always)]
	fn push(&mut self, state: &mut Self::Done, _item: Item) {
		*state += 1;
	}
}
