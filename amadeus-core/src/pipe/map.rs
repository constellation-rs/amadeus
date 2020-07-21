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
pub struct Map<P, F> {
	#[pin]
	pipe: P,
	f: F,
}

impl<P: Stream, F> Stream for Map<P, F>
where
	F: FnMut<(P::Item,)>,
{
	type Item = F::Output;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		let (mut pipe, f) = (self_.pipe, &mut self_.f);
		Poll::Ready(ready!(pipe.as_mut().poll_next(cx)).map(|t| f.call_mut((t,))))
	}
}

impl<P: Pipe<Input>, F, Input> Pipe<Input> for Map<P, F>
where
	F: FnMut<(P::Output,)>,
{
	type Output = F::Output;

	fn poll_next(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Input>>,
	) -> Poll<Option<Self::Output>> {
		let mut self_ = self.project();
		let (mut pipe, f) = (self_.pipe, &mut self_.f);
		Poll::Ready(ready!(pipe.as_mut().poll_next(cx, stream.as_mut())).map(|t| f.call_mut((t,))))
	}
}
