use derive_new::new;
use serde::{Deserialize, Serialize};

use super::{
	folder_par_sink, FolderSync, FolderSyncReducer, FolderSyncReducerFactory, ParallelPipe, ParallelSink, SumFolder
};

#[derive(new)]
#[must_use]
pub struct Count<I> {
	i: I,
}

impl_par_dist! {
	impl<I: ParallelPipe<Source>, Source> ParallelSink<Source>
		for Count<I>
	where
		I::Item: 'static,
	{
		folder_par_sink!(CountFolder, SumFolder<usize>, self, CountFolder::new(), SumFolder::new());
	}
}

#[derive(Clone, Serialize, Deserialize, new)]
pub struct CountFolder;

impl<A> FolderSync<A> for CountFolder {
	type Output = usize;

	fn zero(&mut self) -> Self::Output {
		0
	}
	fn push(&mut self, state: &mut Self::Output, _item: A) {
		*state += 1;
	}
}
