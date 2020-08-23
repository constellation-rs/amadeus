#![allow(unsafe_code)]

use derive_new::new;
use futures::{ready, Stream};
use pin_project::{pin_project, pinned_drop};
use std::{
	future::Future, iter::FromIterator, ops, pin::Pin, sync::Arc, task::{Context, Poll}
};

pub trait FuturePinnedAsyncDrop: Future + PinnedAsyncDrop {}
impl<T: ?Sized> FuturePinnedAsyncDrop for T where T: Future + PinnedAsyncDrop {}

pub type BoxFuturePinnedAsyncDrop<'a, T> =
	Pin<Box<dyn FuturePinnedAsyncDrop<Output = T> + 'a + Send>>;

pub trait AsyncDrop {
	fn poll_drop_ready(&mut self, cx: &mut Context<'_>) -> Poll<()> {
		let _ = cx;
		Poll::Ready(())
	}
}

pub trait PinnedAsyncDrop {
	fn poll_drop_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
		let _ = cx;
		Poll::Ready(())
	}
}
impl<T: ?Sized + PinnedAsyncDrop + Unpin> PinnedAsyncDrop for &mut T {
	fn poll_drop_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
		T::poll_drop_ready(Pin::new(&mut **self), cx)
	}
}
impl<P> PinnedAsyncDrop for Pin<P>
where
	P: Unpin + ops::DerefMut,
	P::Target: PinnedAsyncDrop,
{
	fn poll_drop_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
		Pin::get_mut(self).as_mut().poll_drop_ready(cx)
	}
}

impl<T: ?Sized> PinnedAsyncDrop for Box<T>
where
	T: PinnedAsyncDrop + Unpin,
{
	fn poll_drop_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
		T::poll_drop_ready(Pin::new(&mut *self), cx)
	}
}
impl<T: ?Sized> AsyncDrop for Box<T>
where
	T: PinnedAsyncDrop + Unpin,
{
	fn poll_drop_ready(&mut self, cx: &mut Context<'_>) -> Poll<()> {
		unsafe { Pin::new_unchecked(self) }.poll_drop_ready(cx)
	}
}

impl<T: ?Sized> PinnedAsyncDrop for Arc<T>
where
	T: PinnedAsyncDrop + Unpin,
{
	fn poll_drop_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
		todo!()
		// lock weak_count (this also needs to block upgrading?)
		// let res = if Self::strong_count(self) == 1 {
		// 	Pin::new(Self::get_mut_unchecked(self)).poll_drop_ready(cx)
		// } else {
		// 	Poll::Ready(())
		// };
		// unlock weak_count
		// res
	}
}

#[pin_project(PinnedDrop)]
#[derive(new)]
pub struct KeepFuture<F>(
	Option<Pin<Box<F>>>,
	*const futures::stream::FuturesUnordered<DropFuture<Pin<Box<F>>>>,
);
impl<F> KeepFuture<F> {
	fn keep(self: Pin<&mut Self>) {
		let self_ = self.project();
		let f = self_.0.take().unwrap();
		unsafe { &**self_.1 }.push(DropFuture(f));
	}
}
impl<F> Future for KeepFuture<F>
where
	F: Future,
{
	type Output = F::Output;

	fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		self.project().0.as_mut().unwrap().as_mut().poll(cx)
	}
}
#[pinned_drop]
impl<F> PinnedDrop for KeepFuture<F> {
	fn drop(self: Pin<&mut Self>) {
		self.keep()
	}
}

#[pin_project]
#[derive(new)]
pub struct DropFuture<F>(#[pin] F);
impl<F> Future for DropFuture<F>
where
	F: PinnedAsyncDrop,
{
	type Output = ();

	fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		self.project().0.poll_drop_ready(cx)
	}
}

#[pin_project]
pub struct FuturesUnordered<F> {
	#[pin]
	inner: futures::stream::FuturesUnordered<KeepFuture<F>>,
	#[pin]
	dropping: futures::stream::FuturesUnordered<DropFuture<Pin<Box<F>>>>,
	emptied: bool,
}
impl<F> FuturesUnordered<F> {
	pub fn new() -> Self {
		Self {
			inner: futures::stream::FuturesUnordered::new(),
			dropping: futures::stream::FuturesUnordered::new(),
			emptied: false,
		}
	}
	pub fn push(&self, future: F) {
		self.inner.push(KeepFuture(
			Some(Box::pin(future)),
			&self.dropping as *const _,
		))
	}
}
impl<F> Default for FuturesUnordered<F> {
	fn default() -> Self {
		Self::new()
	}
}
impl<F> FromIterator<F> for FuturesUnordered<F> {
	fn from_iter<I>(iter: I) -> Self
	where
		I: IntoIterator<Item = F>,
	{
		let acc = FuturesUnordered::new();
		iter.into_iter().fold(acc, |acc, item| {
			acc.push(item);
			acc
		})
	}
}
impl<F> Stream for FuturesUnordered<F>
where
	F: Future + PinnedAsyncDrop,
{
	type Item = F::Output;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let mut self_ = self.project();
		while let Poll::Ready(Some(())) = self_.dropping.as_mut().poll_next(cx) {}
		self_.inner.poll_next(cx)
	}
	fn size_hint(&self) -> (usize, Option<usize>) {
		(self.inner.len(), None)
	}
}

impl<F> PinnedAsyncDrop for FuturesUnordered<F>
where
	F: Future + PinnedAsyncDrop,
{
	fn poll_drop_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
		let mut self_ = self.project();
		if !*self_.emptied {
			self_.inner.iter_pin_mut().for_each(KeepFuture::keep);
			*self_.emptied = true;
		}
		while let Some(()) = ready!(self_.dropping.as_mut().poll_next(cx)) {}
		Poll::Ready(())
	}
}
impl<F> AsyncDrop for FuturesUnordered<F>
where
	F: Future + PinnedAsyncDrop,
{
	fn poll_drop_ready(&mut self, cx: &mut Context<'_>) -> Poll<()> {
		unsafe { Pin::new_unchecked(self) }.poll_drop_ready(cx)
	}
}
