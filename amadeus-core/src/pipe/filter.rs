use derive_new::new;
use futures::{ready, Stream};
use pin_project::pin_project;
use std::{
	pin::Pin, task::{Context, Poll}
};

use super::Pipe;

#[pin_project]
#[derive(new)]
pub struct Filter<C, F> {
	#[pin]
	task: C,
	f: F,
}

impl<C: Stream, F> Stream for Filter<C, F>
where
	F: FnMut(&C::Item) -> bool,
{
	type Item = C::Item;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		let (mut task, f) = (self_.task, &mut self_.f);
		Poll::Ready(loop {
			match ready!(task.as_mut().poll_next(cx)) {
				Some(t) if f(&t) => break Some(t),
				Some(_) => (),
				None => break None,
			}
		})
	}
}

impl<C: Pipe<Source>, F, Source> Pipe<Source> for Filter<C, F>
where
	F: FnMut(&C::Item) -> bool,
{
	type Item = C::Item;

	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Source>>,
	) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		let (mut task, f) = (self_.task, &mut self_.f);
		Poll::Ready(loop {
			match ready!(task.as_mut().poll_next(cx, stream.as_mut())) {
				Some(t) if f(&t) => break Some(t),
				Some(_) => (),
				None => break None,
			}
		})
	}
}
