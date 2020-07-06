#![allow(unused_imports,clippy::single_component_path_imports)]

use derive_new::new;
use educe::Educe;
use futures::{ready, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	future::Future, marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{Reducer, ReducerAsync, ReducerProcessSend, ReducerSend};
use crate::{pipe::Sink, pool::ProcessSend};

mod macros {
	#[macro_export]
	macro_rules! folder_par_sink {
		($folder_a:ty, $folder_b:ty, $self:ident, $init_a:expr, $init_b:expr) => {
			type Output = <Self::ReduceC as $crate::par_sink::Reducer<<Self::ReduceA as $crate::par_sink::Reducer<I::Item>>::Output>>::Output;
			type Pipe = I;
			type ReduceA = FolderSyncReducer<I::Item, $folder_a>;
			type ReduceC = FolderSyncReducer<<Self::ReduceA as $crate::par_sink::Reducer<I::Item>>::Output, $folder_b>;

			fn reducers($self) -> (I, Self::ReduceA, Self::ReduceC) {
				let init_a = $init_a;
				let init_b = $init_b;
				(
					$self.i,
					FolderSyncReducer::new(init_a),
					FolderSyncReducer::new(init_b),
				)
			}
		};
	}
	#[macro_export]
	macro_rules! folder_dist_sink {
		($folder_a:ty, $folder_b:ty, $self:ident, $init_a:expr, $init_b:expr) => {
			type Output = <Self::ReduceC as $crate::par_sink::Reducer<<Self::ReduceB as $crate::par_sink::Reducer<<Self::ReduceA as $crate::par_sink::Reducer<I::Item>>::Output>>::Output>>::Output;
			type Pipe = I;
			type ReduceA = FolderSyncReducer<I::Item, $folder_a>;
			type ReduceB = FolderSyncReducer<<Self::ReduceA as $crate::par_sink::Reducer<I::Item>>::Output, $folder_b>;
			type ReduceC = FolderSyncReducer<<Self::ReduceB as $crate::par_sink::Reducer<<Self::ReduceA as $crate::par_sink::Reducer<I::Item>>::Output>>::Output, $folder_b>;

			fn reducers($self) -> (I, Self::ReduceA, Self::ReduceB, Self::ReduceC) {
				let init_a = $init_a;
				let init_b = $init_b;
				(
					$self.i,
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

pub trait FolderSync<A> {
	type Output;

	fn zero(&mut self) -> Self::Output;
	fn push(&mut self, state: &mut Self::Output, item: A);
}

#[derive(Educe, Serialize, Deserialize, new)]
#[educe(Clone(bound = "C: Clone"))]
#[serde(
	bound(serialize = "C: Serialize"),
	bound(deserialize = "C: Deserialize<'de>")
)]
pub struct FolderSyncReducer<A, C> {
	folder: C,
	marker: PhantomData<fn() -> A>,
}

impl<A, C> Reducer<A> for FolderSyncReducer<A, C>
where
	C: FolderSync<A>,
{
	type Output = C::Output;
	type Async = FolderSyncReducerAsync<A, C, C::Output>;

	fn into_async(mut self) -> Self::Async {
		FolderSyncReducerAsync {
			state: Some(self.folder.zero()),
			folder: self.folder,
			marker: PhantomData,
		}
	}
}
impl<A, C> ReducerProcessSend<A> for FolderSyncReducer<A, C>
where
	C: FolderSync<A>,
	C::Output: ProcessSend + 'static,
{
	type Output = C::Output;
}
impl<A, C> ReducerSend<A> for FolderSyncReducer<A, C>
where
	C: FolderSync<A>,
	C::Output: Send + 'static,
{
	type Output = C::Output;
}

#[pin_project]
pub struct FolderSyncReducerAsync<A, C, D> {
	state: Option<D>,
	folder: C,
	marker: PhantomData<fn() -> A>,
}
impl<A, C> Sink<A> for FolderSyncReducerAsync<A, C, C::Output>
where
	C: FolderSync<A>,
{
	#[inline(always)]
	fn poll_pipe(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = A>>,
	) -> Poll<()> {
		let self_ = self.project();
		let folder = self_.folder;
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			folder.push(self_.state.as_mut().unwrap(), item);
		}
		Poll::Ready(())
	}
}
impl<A, C> ReducerAsync<A> for FolderSyncReducerAsync<A, C, C::Output>
where
	C: FolderSync<A>,
{
	type Output = C::Output;

	fn output<'a>(self: Pin<&'a mut Self>) -> Pin<Box<dyn Future<Output = Self::Output> + 'a>> {
		Box::pin(async move { self.project().state.take().unwrap() })
	}
}
