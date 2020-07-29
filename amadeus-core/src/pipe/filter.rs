use derive_new::new;
use futures::{ready, Stream};
use pin_project::pin_project;
use serde_closure::traits::FnMut;
use std::{
	pin::Pin, task::{Context, Poll}
};

use super::Pipe;

#[pin_project]
#[derive(new)]
pub struct Filter<P, F> {
	#[pin]
	pipe: P,
	f: F,
}

impl<P: Stream, F> Stream for Filter<P, F>
where
	F: for<'a> FnMut<(&'a P::Item,), Output = bool>,
{
	type Item = P::Item;

	#[inline]
	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		let (mut pipe, f) = (self_.pipe, &mut self_.f);
		Poll::Ready(loop {
			match ready!(pipe.as_mut().poll_next(cx)) {
				Some(t) if f.call_mut((&t,)) => break Some(t),
				Some(_) => (),
				None => break None,
			}
		})
	}
}

impl<P: Pipe<Input>, F, Input> Pipe<Input> for Filter<P, F>
where
	F: for<'a> FnMut<(&'a P::Output,), Output = bool>,
{
	type Output = P::Output;

	#[inline]
	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Input>>,
	) -> Poll<Option<Self::Output>> {
		let mut self_ = self.project();
		let (mut pipe, f) = (self_.pipe, &mut self_.f);
		Poll::Ready(loop {
			match ready!(pipe.as_mut().poll_next(cx, stream.as_mut())) {
				Some(t) if f.call_mut((&t,)) => break Some(t),
				Some(_) => (),
				None => break None,
			}
		})
	}
}
