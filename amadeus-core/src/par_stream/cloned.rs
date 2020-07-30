use derive_new::new;
use futures::Stream;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{ParallelPipe, PipeTask};
use crate::pipe::Pipe;

#[derive(new)]
#[must_use]
pub struct Cloned<P, T, Input> {
	pipe: P,
	marker: PhantomData<fn() -> (Input, T)>,
}

impl_par_dist! {
	impl<'a, P, Input, T: 'a> ParallelPipe<&'a Input> for Cloned<P, T, Input>
	where
		P: ParallelPipe<&'a Input, Output = &'a T>,
		T: Clone,
	{
		type Output = T;
		type Task = ClonedTask<P::Task>;

		fn task(&self) -> Self::Task {
			let task = self.pipe.task();
			ClonedTask { task }
		}
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct ClonedTask<T> {
	#[pin]
	task: T,
}
impl<'a, C, Input, T: 'a> PipeTask<&'a Input> for ClonedTask<C>
where
	C: PipeTask<&'a Input, Output = &'a T>,
	T: Clone,
{
	type Output = T;
	type Async = ClonedTask<C::Async>;

	fn into_async(self) -> Self::Async {
		ClonedTask {
			task: self.task.into_async(),
		}
	}
}

impl<'a, C, Input: 'a, T: 'a> Pipe<&'a Input> for ClonedTask<C>
where
	C: Pipe<&'a Input, Output = &'a T>,
	T: Clone,
{
	type Output = T;

	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = &'a Input>>,
	) -> Poll<Option<Self::Output>> {
		self.project()
			.task
			.poll_next(cx, stream)
			.map(Option::<&_>::cloned)
	}
}
