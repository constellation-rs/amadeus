use either::Either;
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
pub struct Fold<I, ID, F, B> {
	i: I,
	identity: ID,
	op: F,
	marker: PhantomData<fn() -> B>,
}
impl<I, ID, F, B> Fold<I, ID, F, B> {
	pub(crate) fn new(i: I, identity: ID, op: F) -> Self {
		Self {
			i,
			identity,
			op,
			marker: PhantomData,
		}
	}
}

impl<I: DistributedPipe<Source>, Source, ID, F, B> DistributedSink<I, Source, B>
	for Fold<I, ID, F, B>
where
	ID: FnMut() -> B + Clone + ProcessSend,
	F: FnMut(B, Either<I::Item, B>) -> B + Clone + ProcessSend,
	B: ProcessSend,
	I::Item: 'static,
{
	type ReduceAFactory = FoldReducerAFactory<I::Item, ID, F, B>;
	type ReduceBFactory = FoldReducerBFactory<I::Item, ID, F, B>;
	type ReduceA = FoldReducerA<I::Item, ID, F, B>;
	type ReduceB = FoldReducerB<I::Item, ID, F, B>;
	type ReduceC = FoldReducerB<I::Item, ID, F, B>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceBFactory, Self::ReduceC) {
		(
			self.i,
			FoldReducerAFactory(self.identity.clone(), self.op.clone(), PhantomData),
			FoldReducerBFactory(self.identity.clone(), self.op.clone(), PhantomData),
			FoldReducerB(Some(Either::Left(self.identity)), self.op, PhantomData),
		)
	}
}

#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "ID: Serialize, F: Serialize"),
	bound(deserialize = "ID: Deserialize<'de>, F: Deserialize<'de>")
)]
pub struct FoldReducerAFactory<A, ID, F, B>(ID, F, PhantomData<fn(A, B)>);
impl<A, ID, F, B> Factory for FoldReducerAFactory<A, ID, F, B>
where
	ID: FnMut() -> B + Clone,
	F: FnMut(B, Either<A, B>) -> B + Clone,
{
	type Item = FoldReducerA<A, ID, F, B>;
	fn make(&self) -> Self::Item {
		FoldReducerA(
			Some(Either::Left(self.0.clone())),
			self.1.clone(),
			PhantomData,
		)
	}
}
impl<A, ID, F, B> Clone for FoldReducerAFactory<A, ID, F, B>
where
	ID: Clone,
	F: Clone,
{
	fn clone(&self) -> Self {
		Self(self.0.clone(), self.1.clone(), PhantomData)
	}
}

#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "ID: Serialize, F: Serialize"),
	bound(deserialize = "ID: Deserialize<'de>, F: Deserialize<'de>")
)]
pub struct FoldReducerBFactory<A, ID, F, B>(ID, F, PhantomData<fn(A, B)>);
impl<A, ID, F, B> Factory for FoldReducerBFactory<A, ID, F, B>
where
	ID: FnMut() -> B + Clone,
	F: FnMut(B, Either<A, B>) -> B + Clone,
{
	type Item = FoldReducerB<A, ID, F, B>;
	fn make(&self) -> Self::Item {
		FoldReducerB(
			Some(Either::Left(self.0.clone())),
			self.1.clone(),
			PhantomData,
		)
	}
}
impl<A, ID, F, B> Clone for FoldReducerBFactory<A, ID, F, B>
where
	ID: Clone,
	F: Clone,
{
	fn clone(&self) -> Self {
		Self(self.0.clone(), self.1.clone(), PhantomData)
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "ID: Serialize, B: Serialize, F: Serialize"),
	bound(deserialize = "ID: Deserialize<'de>, B: Deserialize<'de>, F: Deserialize<'de>")
)]
pub struct FoldReducerA<A, ID, F, B>(Option<Either<ID, B>>, F, PhantomData<fn(A)>);

impl<A, ID, F, B> Reducer for FoldReducerA<A, ID, F, B>
where
	ID: FnMut() -> B + Clone,
	F: FnMut(B, Either<A, B>) -> B + Clone,
{
	type Item = A;
	type Output = B;
	type Async = Self;

	fn into_async(self) -> Self::Async {
		self
	}
}
impl<A, ID, F, B> ReducerAsync for FoldReducerA<A, ID, F, B>
where
	ID: FnMut() -> B + Clone,
	F: FnMut(B, Either<A, B>) -> B + Clone,
{
	type Item = A;
	type Output = B;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		let self_0 = self_.0.as_mut().unwrap();
		let self_1 = self_.1;
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			replace_with_or_abort(self_0, |self_0| {
				Either::Right(self_0.map_left(|mut identity| identity()).into_inner())
			});
			replace_with_or_abort(self_0, |self_0| {
				Either::Right((self_1)(self_0.right().unwrap(), Either::Left(item)))
			});
		}
		Poll::Ready(())
	}
	fn poll_output(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
		Poll::Ready(
			self.project()
				.0
				.take()
				.unwrap()
				.map_left(|mut identity| identity())
				.into_inner(),
		)
	}
}
impl<A, ID, F, B> ReducerProcessSend for FoldReducerA<A, ID, F, B>
where
	A: 'static,
	ID: FnMut() -> B + Clone + ProcessSend,
	F: FnMut(B, Either<A, B>) -> B + Clone + ProcessSend,
	B: ProcessSend,
{
	type Output = B;
}
impl<A, ID, F, B> ReducerSend for FoldReducerA<A, ID, F, B>
where
	A: 'static,
	ID: FnMut() -> B + Clone + Send + 'static,
	F: FnMut(B, Either<A, B>) -> B + Clone + Send + 'static,
	B: Send + 'static,
{
	type Output = B;
}

#[pin_project]
#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "ID: Serialize, B: Serialize, F: Serialize"),
	bound(deserialize = "ID: Deserialize<'de>, B: Deserialize<'de>, F: Deserialize<'de>")
)]
pub struct FoldReducerB<A, ID, F, B>(Option<Either<ID, B>>, F, PhantomData<fn(A)>);

impl<A, ID, F, B> Clone for FoldReducerB<A, ID, F, B>
where
	ID: Clone,
	F: Clone,
{
	fn clone(&self) -> Self {
		Self(
			Some(Either::Left(
				self.0.as_ref().unwrap().as_ref().left().unwrap().clone(),
			)),
			self.1.clone(),
			PhantomData,
		)
	}
}

impl<A, ID, F, B> Reducer for FoldReducerB<A, ID, F, B>
where
	ID: FnMut() -> B + Clone,
	F: FnMut(B, Either<A, B>) -> B + Clone,
{
	type Item = B;
	type Output = B;
	type Async = Self;

	fn into_async(self) -> Self::Async {
		self
	}
}
impl<A, ID, F, B> ReducerAsync for FoldReducerB<A, ID, F, B>
where
	ID: FnMut() -> B + Clone,
	F: FnMut(B, Either<A, B>) -> B + Clone,
{
	type Item = B;
	type Output = B;

	#[inline(always)]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		let self_1 = self_.1;
		let self_0 = self_.0.as_mut().unwrap();
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			replace_with_or_abort(self_0, |self_0| {
				Either::Right(self_0.map_left(|mut identity| identity()).into_inner())
			});
			replace_with_or_abort(self_0, |self_0| {
				Either::Right((self_1)(self_0.right().unwrap(), Either::Right(item)))
			});
		}
		Poll::Ready(())
	}
	fn poll_output(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
		Poll::Ready(
			self.project()
				.0
				.take()
				.unwrap()
				.map_left(|mut identity| identity())
				.into_inner(),
		)
	}
}
impl<A, ID, F, B> ReducerProcessSend for FoldReducerB<A, ID, F, B>
where
	A: 'static,
	ID: FnMut() -> B + Clone + ProcessSend,
	F: FnMut(B, Either<A, B>) -> B + Clone + ProcessSend,
	B: ProcessSend,
{
	type Output = B;
}
impl<A, ID, F, B> ReducerSend for FoldReducerB<A, ID, F, B>
where
	A: 'static,
	ID: FnMut() -> B + Clone + Send + 'static,
	F: FnMut(B, Either<A, B>) -> B + Clone + Send + 'static,
	B: Send + 'static,
{
	type Output = B;
}
