use derive_new::new;
use futures::Stream;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use serde_closure::traits::FnMut;
use std::{
	pin::Pin, task::{Context, Poll}
};

use super::{ParallelPipe, ParallelStream, PipeTask, StreamTask};
use crate::pipe::Pipe;

#[derive(new)]
#[must_use]
pub struct Inspect<I, F> {
	i: I,
	f: F,
}

impl_par_dist! {
	impl<I: ParallelStream, F> ParallelStream for Inspect<I, F>
	where
		F: for<'a> FnMut<(&'a I::Item,), Output = ()> + Clone + Send + 'static,
	{
		type Item = I::Item;
		type Task = InspectTask<I::Task, F>;

		fn size_hint(&self) -> (usize, Option<usize>) {
			self.i.size_hint()
		}
		fn next_task(&mut self) -> Option<Self::Task> {
			self.i.next_task().map(|task| {
				let f = self.f.clone();
				InspectTask { task, f }
			})
		}
	}

	impl<I: ParallelPipe<Source>, F, Source> ParallelPipe<Source> for Inspect<I, F>
	where
		F: for<'a> FnMut<(&'a I::Item,), Output = ()> + Clone + Send + 'static,
	{
		type Item = I::Item;
		type Task = InspectTask<I::Task, F>;

		fn task(&self) -> Self::Task {
			let task = self.i.task();
			let f = self.f.clone();
			InspectTask { task, f }
		}
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct InspectTask<T, F> {
	#[pin]
	task: T,
	f: F,
}

impl<C: StreamTask, F> StreamTask for InspectTask<C, F>
where
	F: for<'a> FnMut<(&'a C::Item,), Output = ()> + Clone,
{
	type Item = C::Item;
	type Async = InspectTask<C::Async, F>;
	fn into_async(self) -> Self::Async {
		InspectTask {
			task: self.task.into_async(),
			f: self.f,
		}
	}
}
impl<C: PipeTask<Source>, F, Source> PipeTask<Source> for InspectTask<C, F>
where
	F: for<'a> FnMut<(&'a C::Item,), Output = ()> + Clone,
{
	type Item = C::Item;
	type Async = InspectTask<C::Async, F>;
	fn into_async(self) -> Self::Async {
		InspectTask {
			task: self.task.into_async(),
			f: self.f,
		}
	}
}

impl<C: Stream, F> Stream for InspectTask<C, F>
where
	F: for<'a> FnMut<(&'a C::Item,), Output = ()> + Clone,
{
	type Item = C::Item;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		task.poll_next(cx).map(|item| {
			item.map(|item| {
				f.call_mut((&item,));
				item
			})
		})
	}
}

impl<C: Pipe<Source>, F, Source> Pipe<Source> for InspectTask<C, F>
where
	F: for<'a> FnMut<(&'a C::Item,), Output = ()> + Clone,
{
	type Item = C::Item;

	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
	) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		task.poll_next(cx, stream).map(|item| {
			item.map(|item| {
				f.call_mut((&item,));
				item
			})
		})
	}
}
