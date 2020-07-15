use derive_new::new;
use futures::{ready, Stream};
use pin_project::pin_project;
use serde_closure::traits::FnMut;
use std::{
	marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::Pipe;

#[pin_project]
#[derive(new)]
pub struct Map<C, F, R> {
	#[pin]
	task: C,
	f: F,
	marker: PhantomData<fn() -> R>,
}

impl<C: Stream, F, R> Stream for Map<C, F, R>
where
	F: FnMut<(C::Item,), Output = R>,
{
	type Item = R;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		let (mut task, f) = (self_.task, &mut self_.f);
		Poll::Ready(ready!(task.as_mut().poll_next(cx)).map(|t| f.call_mut((t,))))
	}
}

impl<C: Pipe<Source>, F, R, Source> Pipe<Source> for Map<C, F, R>
where
	F: FnMut<(C::Item,), Output = R>,
{
	type Item = R;

	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Source>>,
	) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		let (mut task, f) = (self_.task, &mut self_.f);
		Poll::Ready(ready!(task.as_mut().poll_next(cx, stream.as_mut())).map(|t| f.call_mut((t,))))
	}
}
