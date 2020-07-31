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
pub struct FilterMapSync<P, F> {
	#[pin]
	pipe: P,
	f: F,
}

impl<P: Stream, F, U> Stream for FilterMapSync<P, F>
where
	F: FnMut<(P::Item,), Output = Option<U>>,
{
	type Item = U;

	#[inline]
	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		let (mut pipe, f) = (self_.pipe, &mut self_.f);
		Poll::Ready(loop {
			match ready!(pipe.as_mut().poll_next(cx)) {
				Some(t) => {
					if let Some(t) = f.call_mut((t,)) {
						break Some(t);
					}
				}
				None => break None,
			}
		})
	}
}

impl<P: Pipe<Input>, F, U, Input> Pipe<Input> for FilterMapSync<P, F>
where
	F: FnMut<(P::Output,), Output = Option<U>>,
{
	type Output = U;

	#[inline]
	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Input>>,
	) -> Poll<Option<Self::Output>> {
		let mut self_ = self.project();
		let (mut pipe, f) = (self_.pipe, &mut self_.f);
		Poll::Ready(loop {
			match ready!(pipe.as_mut().poll_next(cx, stream.as_mut())) {
				Some(t) => {
					if let Some(t) = f.call_mut((t,)) {
						break Some(t);
					}
				}
				None => break None,
			}
		})
	}
}
