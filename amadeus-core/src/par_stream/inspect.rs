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

#[pin_project]
#[derive(new)]
#[must_use]
pub struct Inspect<P, F> {
	#[pin]
	pipe: P,
	f: F,
}

impl_par_dist! {
	impl<P: ParallelStream, F> ParallelStream for Inspect<P, F>
	where
		F: for<'a> FnMut<(&'a P::Item,), Output = ()> + Clone + Send,
	{
		type Item = P::Item;
		type Task = InspectTask<P::Task, F>;

		fn size_hint(&self) -> (usize, Option<usize>) {
			self.pipe.size_hint()
		}
		fn next_task(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Task>> {
			let self_ = self.project();
			let f = self_.f;
			self_.pipe.next_task(cx).map(|task| {
				task.map(|task| {
					let f = f.clone();
					InspectTask { task, f }
				})
			})
		}
	}

	impl<P: ParallelPipe<Input>, F, Input> ParallelPipe<Input> for Inspect<P, F>
	where
		F: for<'a> FnMut<(&'a P::Output,), Output = ()> + Clone + Send,
	{
		type Output = P::Output;
		type Task = InspectTask<P::Task, F>;

		fn task(&self) -> Self::Task {
			let task = self.pipe.task();
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
impl<C: PipeTask<Input>, F, Input> PipeTask<Input> for InspectTask<C, F>
where
	F: for<'a> FnMut<(&'a C::Output,), Output = ()> + Clone,
{
	type Output = C::Output;
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

impl<C: Pipe<Input>, F, Input> Pipe<Input> for InspectTask<C, F>
where
	F: for<'a> FnMut<(&'a C::Output,), Output = ()> + Clone,
{
	type Output = C::Output;

	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Input>>,
	) -> Poll<Option<Self::Output>> {
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
