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
pub struct FlatMapSync<P, F, R> {
	#[pin]
	pipe: P,
	f: F,
	#[new(default)]
	next: Option<R>,
}

impl<P: Stream, F, R> Stream for FlatMapSync<P, F, R>
where
	F: FnMut<(P::Item,), Output = R>,
	R: Iterator,
{
	type Item = R::Item;

	#[inline]
	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		Poll::Ready(loop {
			if let Some(s) = self_.next.as_mut() {
				if let Some(item) = s.next() {
					break Some(item);
				} else {
					*self_.next = None;
				}
			} else if let Some(s) = ready!(self_.pipe.as_mut().poll_next(cx)) {
				*self_.next = Some(self_.f.call_mut((s,)));
			} else {
				break None;
			}
		})
	}
}

impl<P: Pipe<Input>, F, R, Input> Pipe<Input> for FlatMapSync<P, F, R>
where
	F: FnMut<(P::Output,), Output = R>,
	R: Iterator,
{
	type Output = R::Item;

	#[inline]
	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Input>>,
	) -> Poll<Option<Self::Output>> {
		let mut self_ = self.project();
		Poll::Ready(loop {
			if let Some(s) = self_.next.as_mut() {
				if let Some(item) = s.next() {
					break Some(item);
				} else {
					*self_.next = None;
				}
			} else if let Some(s) = ready!(self_.pipe.as_mut().poll_next(cx, stream.as_mut())) {
				*self_.next = Some(self_.f.call_mut((s,)));
			} else {
				break None;
			}
		})
	}
}
