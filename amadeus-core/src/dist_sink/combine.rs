use futures::{ready, Stream};
use pin_project::pin_project;
use replace_with::replace_with_or_abort;
use serde::{Deserialize, Serialize};
use std::{
	marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedPipe, DistributedSink, Factory, Reducer, ReducerAsync, ReducerProcessSend, ReducerSend
};
use crate::pool::ProcessSend;

#[must_use]
pub struct Combine<I, F> {
	i: I,
	f: F,
}
impl<I, F> Combine<I, F> {
	pub(crate) fn new(i: I, f: F) -> Self {
		Self { i, f }
	}
}

impl<I: DistributedPipe<Source>, Source, F> DistributedSink<I, Source, Option<I::Item>>
	for Combine<I, F>
where
	F: FnMut(I::Item, I::Item) -> I::Item + Clone + ProcessSend,
	I::Item: ProcessSend,
{
	type ReduceAFactory = CombineReduceFactory<I::Item, I::Item, CombineFn<F>>;
	type ReduceBFactory = CombineReduceFactory<Option<I::Item>, I::Item, CombineFn<F>>;
	type ReduceA = CombineReducer<I::Item, I::Item, CombineFn<F>>;
	type ReduceB = CombineReducer<Option<I::Item>, I::Item, CombineFn<F>>;
	type ReduceC = CombineReducer<Option<I::Item>, I::Item, CombineFn<F>>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceBFactory, Self::ReduceC) {
		(
			self.i,
			CombineReduceFactory(CombineFn(self.f.clone()), PhantomData),
			CombineReduceFactory(CombineFn(self.f.clone()), PhantomData),
			CombineReducer(None, CombineFn(self.f), PhantomData),
		)
	}
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct CombineFn<F>(F);
impl<F, A> Combiner<A> for CombineFn<F>
where
	F: FnMut(A, A) -> A,
{
	fn combine(&mut self, a: A, b: A) -> A {
		self.0(a, b)
	}
}

pub trait Combiner<A> {
	fn combine(&mut self, a: A, b: A) -> A;
}

#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "F: Serialize"),
	bound(deserialize = "F: Deserialize<'de>")
)]
pub struct CombineReduceFactory<A, B, F>(pub(crate) F, pub(crate) PhantomData<fn(A, B)>);
impl<A, B, F> Factory for CombineReduceFactory<A, B, F>
where
	Option<B>: From<A>,
	F: Combiner<B> + Clone,
{
	type Item = CombineReducer<A, B, F>;
	fn make(&self) -> Self::Item {
		CombineReducer(None, self.0.clone(), PhantomData)
	}
}
impl<A, B, F> Clone for CombineReduceFactory<A, B, F>
where
	F: Clone,
{
	fn clone(&self) -> Self {
		Self(self.0.clone(), PhantomData)
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "B: Serialize, F: Serialize"),
	bound(deserialize = "B: Deserialize<'de>, F: Deserialize<'de>")
)]
pub struct CombineReducer<A, B, F>(
	pub(crate) Option<B>,
	pub(crate) F,
	pub(crate) PhantomData<fn(A)>,
);

impl<A, B, F> Reducer for CombineReducer<A, B, F>
where
	Option<B>: From<A>,
	F: Combiner<B>,
{
	type Item = A;
	type Output = Option<B>;
	type Async = Self;

	fn into_async(self) -> Self::Async {
		self
	}
}
impl<A, B, F> ReducerAsync for CombineReducer<A, B, F>
where
	Option<B>: From<A>,
	F: Combiner<B>,
{
	type Item = A;
	type Output = Option<B>;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		let self_1 = self_.1;
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			let item: Option<B> = item.into();
			if let Some(item) = item {
				replace_with_or_abort(self_.0, |self_0| {
					Some(if let Some(cur) = self_0 {
						self_1.combine(cur, item)
					} else {
						item
					})
				});
			}
		}
		Poll::Ready(())
	}
	fn poll_output(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
		Poll::Ready(self.project().0.take())
	}
}
impl<A, B, F> ReducerProcessSend for CombineReducer<A, B, F>
where
	A: 'static,
	Option<B>: From<A>,
	F: Combiner<B> + ProcessSend,
	B: ProcessSend,
{
	type Output = Option<B>;
}
impl<A, B, F> ReducerSend for CombineReducer<A, B, F>
where
	A: 'static,
	Option<B>: From<A>,
	F: Combiner<B> + Send + 'static,
	B: Send + 'static,
{
	type Output = Option<B>;
}
