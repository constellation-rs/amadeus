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
pub struct Update<I, F> {
	i: I,
	f: F,
}

impl_par_dist! {
	impl<I: ParallelStream, F> ParallelStream for Update<I, F>
	where
		F: for<'a>FnMut<(&'a mut I::Item,), Output = ()> + Clone + Send + 'static,
	{
		type Item = I::Item;
		type Task = UpdateTask<I::Task, F>;

		fn size_hint(&self) -> (usize, Option<usize>) {
			self.i.size_hint()
		}
		fn next_task(&mut self) -> Option<Self::Task> {
			self.i.next_task().map(|task| {
				let f = self.f.clone();
				UpdateTask { task, f }
			})
		}
	}

	impl<I: ParallelPipe<Source>, F, Source> ParallelPipe<Source> for Update<I, F>
	where
		F: for<'a>FnMut<(&'a mut I::Item,), Output = ()> + Clone + Send + 'static,
	{
		type Item = I::Item;
		type Task = UpdateTask<I::Task, F>;

		fn task(&self) -> Self::Task {
			let task = self.i.task();
			let f = self.f.clone();
			UpdateTask { task, f }
		}
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct UpdateTask<T, F> {
	#[pin]
	task: T,
	f: F,
}

impl<C: StreamTask, F> StreamTask for UpdateTask<C, F>
where
	F: for<'a> FnMut<(&'a mut C::Item,), Output = ()> + Clone,
{
	type Item = C::Item;
	type Async = UpdateTask<C::Async, F>;
	fn into_async(self) -> Self::Async {
		UpdateTask {
			task: self.task.into_async(),
			f: self.f,
		}
	}
}
impl<C: PipeTask<Source>, F, Source> PipeTask<Source> for UpdateTask<C, F>
where
	F: for<'a> FnMut<(&'a mut <C as PipeTask<Source>>::Item,), Output = ()> + Clone,
{
	type Item = C::Item;
	type Async = UpdateTask<C::Async, F>;
	fn into_async(self) -> Self::Async {
		UpdateTask {
			task: self.task.into_async(),
			f: self.f,
		}
	}
}

impl<C: Stream, F> Stream for UpdateTask<C, F>
where
	F: for<'a> FnMut<(&'a mut C::Item,), Output = ()> + Clone,
{
	type Item = C::Item;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		task.poll_next(cx).map(|item| {
			item.map(|mut item| {
				f.call_mut((&mut item,));
				item
			})
		})
	}
}

impl<C: Pipe<Source>, F, Source> Pipe<Source> for UpdateTask<C, F>
where
	F: for<'a> FnMut<(&'a mut C::Item,), Output = ()> + Clone,
{
	type Item = C::Item;

	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Source>>,
	) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		let (task, f) = (self_.task, &mut self_.f);
		task.poll_next(cx, stream).map(|item| {
			item.map(|mut item| {
				f.call_mut((&mut item,));
				item
			})
		})
	}
}
