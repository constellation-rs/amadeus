#![allow(unused_imports,clippy::single_component_path_imports)]

use derive_new::new;
use educe::Educe;
use futures::{ready, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	future::Future, marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{Reducer, ReducerProcessSend, ReducerSend};
use crate::{pipe::Sink, pool::ProcessSend};

mod macros {
	#[macro_export]
	macro_rules! folder_par_sink {
		($folder_a:ty, $folder_b:ty, $self:ident, $init_a:expr, $init_b:expr) => {
			type Done = <Self::ReduceC as $crate::par_sink::Reducer<<Self::ReduceA as $crate::par_sink::Reducer<P::Output>>::Done>>::Done;
			type Pipe = P;
			type ReduceA = FolderSyncReducer<P::Output, $folder_a, crate::par_sink::Inter>;
			type ReduceC = FolderSyncReducer<<Self::ReduceA as $crate::par_sink::Reducer<P::Output>>::Done, $folder_b, crate::par_sink::Final>;

			fn reducers($self) -> (P, Self::ReduceA, Self::ReduceC) {
				let init_a = $init_a;
				let init_b = $init_b;
				(
					$self.pipe,
					FolderSyncReducer::new(init_a),
					FolderSyncReducer::new(init_b),
				)
			}
		};
	}
	#[macro_export]
	macro_rules! folder_dist_sink {
		($folder_a:ty, $folder_b:ty, $self:ident, $init_a:expr, $init_b:expr) => {
			type Done = <Self::ReduceC as $crate::par_sink::Reducer<<Self::ReduceB as $crate::par_sink::Reducer<<Self::ReduceA as $crate::par_sink::Reducer<P::Output>>::Done>>::Done>>::Done;
			type Pipe = P;
			type ReduceA = FolderSyncReducer<P::Output, $folder_a, crate::par_sink::Inter>;
			type ReduceB = FolderSyncReducer<<Self::ReduceA as $crate::par_sink::Reducer<P::Output>>::Done, $folder_b, crate::par_sink::Inter>;
			type ReduceC = FolderSyncReducer<<Self::ReduceB as $crate::par_sink::Reducer<<Self::ReduceA as $crate::par_sink::Reducer<P::Output>>::Done>>::Done, $folder_b, crate::par_sink::Final>;

			fn reducers($self) -> (P, Self::ReduceA, Self::ReduceB, Self::ReduceC) {
				let init_a = $init_a;
				let init_b = $init_b;
				(
					$self.pipe,
					FolderSyncReducer::new(init_a),
					FolderSyncReducer::new(init_b.clone()),
					FolderSyncReducer::new(init_b),
				)
			}
		};
	}
	pub(crate) use folder_dist_sink;
	pub(crate) use folder_par_sink;
}

pub(crate) use macros::{folder_dist_sink, folder_par_sink};

pub trait FolderSync<Item> {
	type State;
	type Done;

	fn zero(&mut self) -> Self::State;
	fn push(&mut self, state: &mut Self::State, item: Item);
	fn done(&mut self, state: Self::State) -> Self::Done;
}

pub struct Inter;
pub struct Final;

#[derive(Educe, Serialize, Deserialize, new)]
#[educe(Clone(bound = "F: Clone"))]
#[serde(
	bound(serialize = "F: Serialize"),
	bound(deserialize = "F: Deserialize<'de>")
)]
pub struct FolderSyncReducer<Item, F, Final> {
	folder: F,
	marker: PhantomData<fn() -> (Item, Final)>,
}

impl<Item, F> Reducer<Item> for FolderSyncReducer<Item, F, Inter>
where
	F: FolderSync<Item>,
{
	type Done = F::State;
	type Async = FolderSyncReducerAsync<Item, F, F::State, Inter>;

	fn into_async(mut self) -> Self::Async {
		FolderSyncReducerAsync {
			state: Some(self.folder.zero()),
			folder: self.folder,
			marker: PhantomData,
		}
	}
}
impl<Item, F> ReducerProcessSend<Item> for FolderSyncReducer<Item, F, Inter>
where
	F: FolderSync<Item>,
	F::State: ProcessSend,
{
	type Done = F::State;
}
impl<Item, F> ReducerSend<Item> for FolderSyncReducer<Item, F, Inter>
where
	F: FolderSync<Item>,
	F::State: Send,
{
	type Done = F::State;
}

impl<Item, F> Reducer<Item> for FolderSyncReducer<Item, F, Final>
where
	F: FolderSync<Item>,
{
	type Done = F::Done;
	type Async = FolderSyncReducerAsync<Item, F, F::State, Final>;

	fn into_async(mut self) -> Self::Async {
		FolderSyncReducerAsync {
			state: Some(self.folder.zero()),
			folder: self.folder,
			marker: PhantomData,
		}
	}
}
impl<Item, F> ReducerProcessSend<Item> for FolderSyncReducer<Item, F, Final>
where
	F: FolderSync<Item>,
	F::Done: ProcessSend,
{
	type Done = F::Done;
}
impl<Item, F> ReducerSend<Item> for FolderSyncReducer<Item, F, Final>
where
	F: FolderSync<Item>,
	F::Done: Send,
{
	type Done = F::Done;
}

#[pin_project]
pub struct FolderSyncReducerAsync<Item, F, S, Final> {
	state: Option<S>,
	folder: F,
	marker: PhantomData<fn() -> (Item, Final)>,
}
impl<Item, F> Sink<Item> for FolderSyncReducerAsync<Item, F, F::State, Inter>
where
	F: FolderSync<Item>,
{
	type Done = F::State;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<Self::Done> {
		let self_ = self.project();
		let folder = self_.folder;
		let state = self_.state.as_mut().unwrap();
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			folder.push(state, item);
		}
		Poll::Ready(self_.state.take().unwrap())
	}
}
impl<Item, F> Sink<Item> for FolderSyncReducerAsync<Item, F, F::State, Final>
where
	F: FolderSync<Item>,
{
	type Done = F::Done;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<Self::Done> {
		let self_ = self.project();
		let folder = self_.folder;
		let state = self_.state.as_mut().unwrap();
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			folder.push(state, item);
		}
		Poll::Ready(folder.done(self_.state.take().unwrap()))
	}
}
