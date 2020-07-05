use derive_new::new;
use futures::{ready, Stream};
use pin_project::pin_project;
use std::{
	pin::Pin, task::{Context, Poll}
};

use super::Pipe;

#[pin_project]
#[derive(new)]
pub struct FlatMap<C, F, R> {
	#[pin]
	pipe: C,
	f: F,
	#[pin]
	#[new(default)]
	next: Option<R>,
}

impl<C: Stream, F, R> Stream for FlatMap<C, F, R>
where
	F: FnMut(C::Item) -> R,
	R: Stream,
{
	type Item = R::Item;

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
				self_.next.set(Some((self_.f)(s)));
			} else {
				break None;
			}
		})
	}
}

impl<C: Pipe<Source>, F, R, Source> Pipe<Source> for FlatMap<C, F, R>
where
	F: FnMut(C::Item) -> R,
	R: Stream,
{
	type Item = R::Item;

	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Source>>,
	) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		Poll::Ready(loop {
			if let Some(s) = self_.next.as_mut().as_pin_mut() {
				if let Some(item) = ready!(s.poll_next(cx)) {
					break Some(item);
				} else {
					self_.next.set(None);
				}
			} else if let Some(s) = ready!(self_.pipe.as_mut().poll_next(cx, stream.as_mut())) {
				self_.next.set(Some((self_.f)(s)));
			} else {
				break None;
			}
		})
	}
}
