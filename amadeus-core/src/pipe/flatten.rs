use derive_new::new;
use futures::{ready, Stream};
use pin_project::pin_project;
use std::{
	pin::Pin, task::{Context, Poll}
};

use super::Pipe;

#[pin_project]
#[derive(new)]
pub struct Flatten<P, U> {
	#[pin]
	pipe: P,
	#[pin]
	#[new(default)]
	next: Option<U>,
}

impl<P: Stream> Stream for Flatten<P, P::Item>
where
	P::Item: Stream,
{
	type Item = <P::Item as Stream>::Item;

	#[inline]
	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		Poll::Ready(loop {
			if let Some(s) = self_.next.as_mut().as_pin_mut() {
				if let Some(item) = ready!(s.poll_next(cx)) {
					break Some(item);
				} else {
					self_.next.set(None);
				}
			} else if let Some(s) = ready!(self_.pipe.as_mut().poll_next(cx)) {
				self_.next.set(Some(s));
			} else {
				break None;
			}
		})
	}
}

impl<P: Pipe<Input>, Input> Pipe<Input> for Flatten<P, P::Output>
where
	P::Output: Stream,
{
	type Output = <P::Output as Stream>::Item;

	#[inline]
	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Input>>,
	) -> Poll<Option<Self::Output>> {
		let mut self_ = self.project();
		Poll::Ready(loop {
			if let Some(s) = self_.next.as_mut().as_pin_mut() {
				if let Some(item) = ready!(s.poll_next(cx)) {
					break Some(item);
				} else {
					self_.next.set(None);
				}
			} else if let Some(s) = ready!(self_.pipe.as_mut().poll_next(cx, stream.as_mut())) {
				self_.next.set(Some(s));
			} else {
				break None;
			}
		})
	}
}
